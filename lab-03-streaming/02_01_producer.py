import asyncio
import csv
import json
from datetime import datetime, timezone

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData


SCOPE = dbutils.widgets.get("kv_scope")
KEY_EVH_NAME = dbutils.widgets.get("kv_evh_name")
KEY_CONN_STR = dbutils.widgets.get("kv_evh_connection_str")

EVENT_HUB_NAME = dbutils.secrets.get(scope=SCOPE, key=KEY_EVH_NAME)
CONNECTION_STRING = dbutils.secrets.get(scope=SCOPE, key=KEY_CONN_STR)

SOURCE_FILE_PATH = dbutils.widgets.get("source_file_path")

BATCH_SIZE = 50
DELAY_SEC = 1


async def run():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )

    async with producer: 
        with open(SOURCE_FILE_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            event_data_batch = await producer.create_batch()
            batch_count = 0
            total_sent = 0

            for row in reader:
                row["produced_at"] = datetime.now(timezone.utc).isoformat()
            
                payload = json.dumps(row)
                event = EventData(payload)

                try:
                    event_data_batch.add(event)
                    batch_count += 1
                except ValueError:
                    await producer.send_batch(event_data_batch)
                    total_sent += batch_count
                    
                    event_data_batch = await producer.create_batch()
                    event_data_batch.add(event)
                    batch_count = 1
                    await asyncio.sleep(DELAY_SEC)
                
                if batch_count >= BATCH_SIZE:
                    await producer.send_batch(event_data_batch)
                    total_sent += batch_count
                    
                    event_data_batch = await producer.create_batch()
                    batch_count = 0
                    await asyncio.sleep(DELAY_SEC) 
            
            if batch_count > 0:
                await producer.send_batch(event_data_batch)
                total_sent += batch_count

if __name__ == "__main__":
    asyncio.run(run())