import boto3
import json
import time
import re
import tarfile
import os
import logging
from botocore.exceptions import ClientError
import shutil


def trigger_bag_processing(bucket, dest_bucket, prefix):
    state_machine_arn = os.environ["state_machine_arn"]
    s3_object = dict(
        [("bucket", bucket), ("key", prefix), ("dest_bucket", dest_bucket)]
    )
    now = str(int(time.time()))
    name = prefix + "-sf-" + now
    name = re.sub("\W+", "", name)
    print(s3_object)
    client = boto3.client("stepfunctions")
    try:
        response = client.start_execution(
            stateMachineArn=state_machine_arn, name=name, input=json.dumps(s3_object)
        )
    except client.exceptions.InvalidName as e:
        logging.warning(e)


def lambda_handler(event, context):

    sqs = boto3.client("sqs")
    queue = os.environ["bag_queue_url"]
    dest_bucket = os.environ["dest_bucket"]

    print(event)
    if "Records" in event:
        messages = event["Records"]
        for m in messages:
            print(m)
            body = json.loads(m["body"])
            if "Records" in body:
                for r in body["Records"]:
                    bucket = r["s3"]["bucket"]["name"]
                    prefix = r["s3"]["object"]["key"]
                    print(prefix)
                    if prefix.endswith(".bag"):
                        trigger_bag_processing(bucket, dest_bucket, prefix)
            else:
                bucket = body["s3BucketArn"].replace("arn:aws:s3:::", "")
                prefix = body["s3Key"]
                print(prefix)
                if prefix.endswith(".bag"):
                    trigger_bag_processing(bucket, dest_bucket, prefix)

    return {"status": 200}
