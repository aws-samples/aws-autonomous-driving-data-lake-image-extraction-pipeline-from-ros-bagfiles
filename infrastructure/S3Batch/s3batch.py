import boto3
import json
import time
from datetime import datetime
import logging
import os


# This is for testing the pipeline - it allows us to inject messages at various point using S3 batch

# Send .bag files to the bag file ingest queue
def proc_bag(event):
    bag_queue = os.environ["bag_queue_url"]
    sqs = boto3.client("sqs")

    print(event)
    m = {}
    m["Id"] = f"{time.process_time()}".replace(".", "-")
    entries = []
    for e in event["tasks"]:
        m_body = {}
        m_body["s3BucketArn"] = e["s3BucketArn"]
        m_body["s3Key"] = e["s3Key"]
        m["MessageBody"] = json.dumps(m_body)
        entries.append(m)

    response = sqs.send_message_batch(QueueUrl=bag_queue, Entries=entries)
    return response


# send mp4 files to the rejkogition processing queue
def proc_mp4(event):
    job_queue = os.environ["job_queue_url"]
    sqs = boto3.client("sqs")

    print(event)
    m = {}
    m["Id"] = f"{time.process_time()}".replace(".", "-")
    entries = []
    for e in event["tasks"]:
        m_body = {}
        bucket = e["s3BucketArn"].replace("arn:aws:s3:::", "")
        m_body["s3"] = {"bucket": {"name": bucket}, "object": {"key": e["s3Key"]}}
        m["MessageBody"] = json.dumps({"Records": [m_body]})
        entries.append(m)

    print(entries)
    response = sqs.send_message_batch(QueueUrl=job_queue, Entries=entries)
    print(response)
    return response


def lambda_handler(event, context):

    print(event)

    if "mp4" in event["tasks"][0]["s3Key"]:

        response = proc_mp4(event)
    else:
        response = proc_bag(event)

    return {"status": 200}
