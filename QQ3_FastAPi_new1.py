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
import requests
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

# --- Web3 & Contract ---
w3 = Web3(WebsocketProvider(ETH_WS))

# --- Kafka Producer with SASL ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

# --- Block Fetch ---
def fetch_block_with_retry(number, retries=3, delay=1):
    for i in range(retries):
        try:
            return w3.eth.get_block(number, full_transactions=True)
        except BlockNotFound:
            if i < retries - 1:
                time.sleep(delay)
    return None

# --- Main WebSocket Listener ---
async def eth_listener():
    async with websockets.connect(ETH_WS, ping_interval=20, ping_timeout=10) as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe", "params": ["newPendingTransactions"]}))

        sub_block_id = sub_pending_id = None
        for _ in range(2):
            resp = json.loads(await ws.recv())
            if resp.get("id") == 1: sub_block_id = resp.get("result")
            if resp.get("id") == 2: sub_pending_id = resp.get("result")
        print(f"✅ Subscribed block={sub_block_id}, pending={sub_pending_id}")

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
                    print(f"📦 New Block Sent to Kafka: {payload['number']}")
                else:
                    producer.send(TOPIC_BLOCK, {"error": f"Block {num} fetch failed"})

            elif sub_id == sub_pending_id and isinstance(result, str):
                txh = result
                try:
                    tx = w3.eth.get_transaction(txh)
                    payload = {"hash": tx.hash.hex(), "from": tx["from"], "to": tx.to, "value": tx.value, "gas": tx.gas, "gasPrice": tx.gasPrice, "nonce": tx.nonce}
                except Exception as e:
                    payload = {"hash": txh, "error": str(e)}

                producer.send(TOPIC_PENDING, payload)
                print(f"📦 New Pending Tx Sent to Kafka: {txh}")

        producer.flush()

# --- Kafka Consumer Listener ---
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
            print(f"🔔 [Kafka Message] Topic: {msg.topic}, Data: {msg.value}")

    await loop.run_in_executor(None, consume)
