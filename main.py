import os
import logging
from uuid import uuid4
from datetime import datetime
from xlib.utils import get_exchange_data, get_utc_from_unix_time


def extract():
    url = "https://api.coincap.io/v2/exchanges"
    return get_exchange_data(url)


def transform_enrich(data):
    global batchId, batchDatetime
    batchId = str(uuid4())
    batchDatetime = datetime.now()
    # Add batch_id & current date to data before inserting
    for d in data:
        d["batchId"] = batchId
        d["batchDatetime"] = batchDatetime
        d["updatedUTC"] = get_utc_from_unix_time(d.get("updated"))
    return data


def load(exhange_data: list):
    import pandas as pd
    from minio import Minio
    from io import BytesIO

    MINIO_BUCKET_NAME = "coincap-exchanges"  # os.getenv("MINIO_BUCKET_NAME")
    MINIO_ROOT_USER = "minioadmin"  # os.getenv("MINIO_ROOT_USER")
    MINIO_ROOT_PASSWORD = "minioadmin"  # os.getenv("MINIO_ROOT_PASSWORD")
    MINIO_HOST = "localhost:9000"  # os.getenv("MINIO_ROOT_PASSWORD")

    df = pd.DataFrame(exhange_data)
    csv = df.to_parquet(index=False)  # .encode("utf-8")

    client = Minio(MINIO_HOST, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

    # Make MINIO_BUCKET_NAME if not exist.
    found = client.bucket_exists(MINIO_BUCKET_NAME)
    if not found:
        client.make_bucket(MINIO_BUCKET_NAME)
    else:
        logging.info(f"Bucket '{MINIO_BUCKET_NAME}' already exists!")

    # Put parquet data in the bucket
    filename = f"{batchDatetime.strftime('%Y/%m/%d')}/coincap_excahnges_{batchDatetime.strftime('%H%M%S')}_{batchId.replace('-','')}.parquet"
    print(filename)
    client.put_object(MINIO_BUCKET_NAME, filename, data=BytesIO(csv), length=len(csv))
    logging.info(f"Added '{filename}' to bucket!")


if __name__ == "__main__":
    data = extract()
    data_enriched = transform_enrich(data)
    load(data_enriched)
