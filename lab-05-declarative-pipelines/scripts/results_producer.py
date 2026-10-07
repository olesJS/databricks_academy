"""
F1 Results Event Hub Stream Producer

Simulates streaming ingestion by reading F1 race results from CSV,
cleans Kaggle null values ('\\N'), and asynchronously publishes batches
to Azure Event Hubs.

Requires 'eventhub_config.yml' in the project root or environment variables.
"""

import csv
import json
import yaml
import asyncio
import time
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

with open('eventhub_config.yml', 'r') as f:
    config = yaml.safe_load(f)

EVENT_HUB_CONNECTION_STR = config.get('EVENTHUB_CONN_STR')
EVENT_HUB_NAME = config.get('EVENTHUB_NAME')

CSV_SOURCE_PATH = 'data/results.csv'

BATCH_SIZE = 100
SLEEP_TIME = 1.0

async def run():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    async with producer:
        with open(CSV_SOURCE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            batch = await producer.create_batch()
            batch_count = 0
            total_sent = 0

            for row in reader:
                row = {k: (None if v in (r"\N", "") else v) for k, v in row.items()}
                row['event_timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

                payload = json.dumps(row)
                event_data = EventData(payload)

                try:
                    batch.add(event_data)
                    batch_count += 1
                except ValueError:
                    await producer.send_batch(batch)
                    total_sent += batch_count

                    batch = await producer.create_batch()
                    batch.add(event_data)
                    batch_count = 1

                if batch_count >= BATCH_SIZE:
                    await producer.send_batch(batch)
                    total_sent += batch_count

                    batch = await producer.create_batch()
                    batch_count = 0
                    await asyncio.sleep(SLEEP_TIME)

            if len(batch) > 0:
                await producer.send_batch(batch)
                total_sent += len(batch)


asyncio.run(run())