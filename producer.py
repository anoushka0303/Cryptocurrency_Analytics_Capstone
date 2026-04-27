import os
import json
import time
import logging
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EVENT_HUB_CONNECTION_STRING = os.environ["EVENT_HUB_CONNECTION_STRING"]
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "crypto-stream")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))
REPLAY_LOOP = os.environ.get("REPLAY_LOOP", "true").lower() == "true"
LOOP_DELAY_SECS = int(os.environ.get("LOOP_DELAY_SECS", "300"))
JSON_FILE_PATH = os.environ.get("JSON_FILE_PATH", "/app/data/top_20_crypto.json")


def read_json(file_path: str) -> list[dict]:
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        rows = data
    else:
        rows = [data]

    logger.info(f"Loaded {len(rows)} records from {file_path}")
    return rows


def send_batch(producer, rows):
    by_coin = {}

    for row in rows:
        coin_id = row.get("id", "unknown")
        by_coin.setdefault(coin_id, []).append(row)

    for coin_id, coin_rows in by_coin.items():
        batch = producer.create_batch(partition_key=coin_id)

        for row in coin_rows:
            batch.add(EventData(json.dumps(row)))

        producer.send_batch(batch)

    logger.info(f"Sent {len(rows)} events across {len(by_coin)} partitions")


def main():
    logger.info("═══ Crypto Event Producer starting ═══")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME,
    )

    rows = read_json(JSON_FILE_PATH)

    try:
        loop = 0
        while True:
            loop += 1
            logger.info(f"── Loop #{loop} ──")

            for i in range(0, len(rows), BATCH_SIZE):
                chunk = rows[i:i + BATCH_SIZE]
                send_batch(producer, chunk)
                time.sleep(0.5)

            logger.info(f"Loop #{loop} done — {len(rows)} events sent")

            if not REPLAY_LOOP:
                break

            logger.info(f"Sleeping {LOOP_DELAY_SECS}s...")
            time.sleep(LOOP_DELAY_SECS)

    except KeyboardInterrupt:
        logger.info("Stopped by user")

    finally:
        producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    main()