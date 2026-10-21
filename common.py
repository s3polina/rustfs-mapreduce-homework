import csv
import io
import re

import boto3
from botocore.config import Config

from config import *

_VARIANT_RE = re.compile(VARIANT_REGEX)

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

def ensure_bucket():
    s3 = s3_client()
    names = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if BUCKET not in names:
        s3.create_bucket(Bucket=BUCKET)

def object_exists(key):
    try:
        s3_client().head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False

def put_text(key, text):
    s3_client().put_object(Bucket=BUCKET, Key=key, Body=text.encode("utf-8"))

def get_text(key):
    obj = s3_client().get_object(Bucket=BUCKET, Key=key)
    return obj["Body"].read().decode("utf-8")

class S3ChunkStream(io.RawIOBase):
    def __init__(self, chunk_size=S3_CHUNK_BYTES):
        self.body = s3_client().get_object(
            Bucket=BUCKET,
            Key=INPUT_KEY,
        )["Body"]

        self.iterator = self.body.iter_chunks(chunk_size=chunk_size)
        self.buffer = bytearray()
        self.closed_stream = False

    def readable(self):
        return True

    def readinto(self, b):
        while len(self.buffer) < len(b) and not self.closed_stream:
            try:
                chunk = next(self.iterator)
                if chunk:
                    self.buffer.extend(chunk)
            except StopIteration:
                self.closed_stream = True
                break

        if not self.buffer:
            return 0

        n = min(len(b), len(self.buffer))
        b[:n] = self.buffer[:n]
        del self.buffer[:n]

        return n


def iter_csv_rows_from_s3(chunk_bytes=S3_CHUNK_BYTES):
    raw = S3ChunkStream(chunk_bytes)

    buffered = io.BufferedReader(
        raw,
        buffer_size=chunk_bytes,
    )

    text = io.TextIOWrapper(
        buffered,
        encoding="utf-8-sig",
        newline="",
        errors="replace",
    )

    reader = csv.DictReader(text)

    for row in reader:
        yield row

def normalize_skills(value):
    if value is None:
        return []

    value = str(value).strip()

    if not value:
        return []

    cleaned = (
        value
        .replace("[", "")
        .replace("]", "")
        .replace('"', "")
        .replace("'", "")
    )

    parts = re.split(r"\s*[;,|]\s*", cleaned)

    result = []
    seen = set()

    for item in parts:
        item = re.sub(r"\s+", " ", item.strip())

        if not item:
            continue

        # Удаляем только точные дубликаты.
        # Регистр сохраняем так же, как DuckDB.
        if item not in seen:
            seen.add(item)
            result.append(item)

    return result

def variant_match(row):
    text = " ".join(
        str(row.get(col, "") or "")
        for col in ("name", "role", "key_skills", "description")
    )
    return bool(_VARIANT_RE.search(text))
