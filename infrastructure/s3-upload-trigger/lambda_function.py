import boto3
import json
import time
import re
import tarfile
import os
import logging
from botocore.exceptions import ClientError
import shutil


state_machine_arn = os.environ["state_machine_arn"]


def local_bags(dir):
    all_files = absolute_file_paths(dir)
    return [f for f in all_files if f.endswith(".bag")]


def absolute_file_paths(directory):
    for dir_path, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dir_path, f))


def get_object(bucket, object_path, local_dir):
    local_path = os.path.join(local_dir, object_path.split("/")[-1])
    s3 = boto3.client("s3")
    print("Downloading")
    s3.download_file(bucket, object_path, local_path)
    print("Download complete")
    return local_path


def upload_file(file_name, bucket, object_name):
    # Upload the file
    s3_client = boto3.client("s3")
    print(f"Uploading {object_name}")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        print(f"Uploaded {object_name} to {bucket}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def trigger_bag_processing(bucket, prefix):
    s3_object = dict([("bucket", bucket), ("key", prefix)])
    now = str(int(time.time()))
    name = prefix + "-sf-" + now
    name = re.sub("\W+", "", name)
    print(s3_object)
    client = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=state_machine_arn, name=name, input=json.dumps(s3_object)
    )


def lambda_handler(event, context):
    print(event)
    for e in event["tasks"]:
        bucket = e["s3BucketArn"].replace("arn:aws:s3:::", "")
        prefix = e["s3Key"]
        print(prefix)
        if prefix.endswith(".bag") or prefix.endswith(".tar.gz"):
            trigger_bag_processing(bucket, prefix)
        else:
            raise Exception()
