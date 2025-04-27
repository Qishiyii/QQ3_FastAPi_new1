import os
import json
import time
import threading
import asyncio
from collections import deque, defaultdict
from kafka import KafkaProducer, KafkaConsumer
import websockets
from web3 import Web3
from web3.providers.websocket import WebsocketProvider
from web3.exceptions import BlockNotFound
from fastapi import FastAPI
from dotenv import load_dotenv
from contextlib import asynccontextmanager

# Load environment variables
load_dotenv()

# --- FastAPI App with lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(eth_listener())
    asyncio.create_task(kafka_listener())
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health_check():
    return {"status": "ok"}

# --- Configuration ---
ETH_WS = os.getenv("ETH_WS")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS").split(",")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
TOPIC_BLOCK = os.getenv("TOPIC_BLOCK", "raw-blocks")
TOPIC_PENDING = os.getenv("TOPIC_PENDING", "pending-txs")
TOPIC_ALERT = os.getenv("TOPIC_ALERT", "eth_alerts")

# --- Web3 & Kafka ---
w3 = Web3(WebsocketProvider(ETH_WS))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

# --- Risk Windows ---
TX_COUNT_WINDOW = 60
TX_COUNT_THRESHOLD = 50
SMALL_TX_WINDOW = 300
SMALL_VALUE_LIMIT = Web3.to_wei(0.1, "ether")
SMALL_TX_THRESHOLD = 200
SUM_VALUE_THRESHOLD = Web3.to_wei(500, "ether")
GAS_WINDOW = 300
GAS_ANOMALY_FACTOR = 1.5

tx_times = defaultdict(lambda: deque())
small_tx_times = defaultdict(lambda: deque())
gas_window = deque()

# --- Risk Check Functions ---
def cleanup_deque(dq: deque, window: int):
    cutoff = time.time() - window
    while dq and dq[0] < cutoff:
        dq.popleft()

def check_metrics():
    now = time.time()
    # 高频交易检测
    for addr, dq in list(tx_times.items()):
        cleanup_deque(dq, TX_COUNT_WINDOW)
        if len(dq) >= TX_COUNT_THRESHOLD:
            alert = {"type": "high_freq", "address": addr, "count_1m": len(dq), "timestamp": now}
            print("🚨 高频交易报警:", alert)
            producer.send(TOPIC_ALERT, alert)
    # 小额交易淋洗检测
    for addr, dq in list(small_tx_times.items()):
        cleanup_deque(dq, SMALL_TX_WINDOW)
        if len(dq) >= SMALL_TX_THRESHOLD:
            alert = {"type": "wash_small", "address": addr, "count_5m": len(dq), "timestamp": now}
            print("🚨 小额刷交易报警:", alert)
            producer.send(TOPIC_ALERT, alert)
    # Gas价格异常检测
    cleanup_deque(gas_window, GAS_WINDOW)
    if gas_window:
        avg_gas = sum(g for (_, g) in gas_window) / len(gas_window)
        thresh = avg_gas * GAS_ANOMALY_FACTOR
        for ts, gp in gas_window:
            if gp > thresh:
                alert = {"type": "gas_anomaly", "gasPrice": gp, "avg_gasPrice": avg_gas, "timestamp": ts}
                print("🚨 GasPrice异常报警:", alert)
                producer.send(TOPIC_ALERT, alert)
                break
    producer.flush()

def scheduler():
    while True:
        time.sleep(10)
        check_metrics()

threading.Thread(target=scheduler, daemon=True).start()

# --- Helper Functions ---
def fetch_block_with_retry(number, retries=3, delay=1):
    for i in range(retries):
        try:
            return w3.eth.get_block(number, full_transactions=True)
        except BlockNotFound:
            if i < retries - 1:
                time.sleep(delay)
    return None

# --- Ethereum Listener ---
async def eth_listener():
    async with websockets.connect(ETH_WS, ping_interval=20, ping_timeout=10) as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe", "params": ["newPendingTransactions"]}))

        sub_block_id = sub_pending_id = None
        for _ in range(2):
            resp = json.loads(await ws.recv())
            if resp.get("id") == 1: sub_block_id = resp.get("result")
            if resp.get("id") == 2: sub_pending_id = resp.get("result")

        print(f"✅ Subscribed: block={sub_block_id}, pending={sub_pending_id}")

        async for msg in ws:
            data = json.loads(msg)
            if data.get("method") != "eth_subscription":
                continue

            sub_id = data["params"]["subscription"]
            result = data["params"]["result"]

            if sub_id == sub_block_id and isinstance(result, dict):
                num = int(result["number"], 16)
                blk = fetch_block_with_retry(num)
                if blk:
                    payload = {
                        "number": blk.number,
                        "hash": blk.hash.hex(),
                        "timestamp": blk.timestamp,
                        "miner": blk.miner,
                        "size": blk.size,
                        "gasUsed": blk.gasUsed,
                        "gasLimit": blk.gasLimit,
                        "transactionCount": len(blk.transactions),
                        "transactions": [
                            {"hash": tx.hash.hex(), "from": tx["from"], "to": tx.to, "value": tx.value, "gas": tx.gas, "gasPrice": tx.gasPrice}
                            for tx in blk.transactions
                        ]
                    }
                    producer.send(TOPIC_BLOCK, payload)
                    print(f"📦 Block {payload['number']} pushed to Kafka")
                else:
                    producer.send(TOPIC_BLOCK, {"error": f"Block {num} fetch failed"})

            elif sub_id == sub_pending_id and isinstance(result, str):
                txh = result
                try:
                    tx = w3.eth.get_transaction(txh)
                    payload = {
                        "hash": tx.hash.hex(),
                        "from": tx["from"],
                        "to": tx.to,
                        "value": tx.value,
                        "gas": tx.gas,
                        "gasPrice": tx.gasPrice,
                        "nonce": tx.nonce
                    }
                except Exception as e:
                    payload = {"hash": txh, "error": str(e)}

                producer.send(TOPIC_PENDING, payload)
                print(f"🚀 PendingTx {txh}")

                # 更新风险检测窗口
                ts = time.time()
                addr = payload.get("from", "").lower()
                val  = int(payload.get("value", 0))
                gp   = int(payload.get("gasPrice", 0))

                tx_times[addr].append(ts)
                if val < SMALL_VALUE_LIMIT:
                    small_tx_times[addr].append(ts)
                gas_window.append((ts, gp))

                # 检查大额交易
                if val >= SUM_VALUE_THRESHOLD:
                    alert = {"type": "large_value", "address": addr, "value": val, "timestamp": ts}
                    print("🚨 大额交易报警:", alert)
                    producer.send(TOPIC_ALERT, alert)

        producer.flush()

latest_message = {}

# --- 修改 Kafka Consumer Listener ---
async def kafka_listener():
    loop = asyncio.get_running_loop()

    def consume():
        consumer = KafkaConsumer(
            TOPIC_BLOCK, TOPIC_PENDING, TOPIC_ALERT,
            bootstrap_servers=KAFKA_BROKERS,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset='latest',
            group_id='monitor-consumer-group',
            enable_auto_commit=True
        )
        print("🚀 Kafka Consumer Started...")
        for msg in consumer:
            parsed_message = {
                "topic": msg.topic,
                "data": msg.value,
                "timestamp": time.time()
            }
            print(f"🔔 [Kafka Message] Topic: {msg.topic}, Data: {msg.value}")

            # --- 新增：保存最新一条消息 ---
            global latest_message
            latest_message = parsed_message

    await loop.run_in_executor(None, consume)

# --- 新增 HTTP接口：外部可以访问拿到最新Kafka消费的数据 ---
@app.get("/latest_message")
async def get_latest_message():
    return latest_message