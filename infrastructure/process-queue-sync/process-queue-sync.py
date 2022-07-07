import os
import boto3
import json
import logging
from botocore.exceptions import ClientError
import shutil
import io
import time
from datetime import datetime, timedelta
import re

s3 = boto3.client(
    "s3",
)
dynamo = boto3.client("dynamodb")


def export_json_file(file_labels, bucket, key):

    frame_duration = int(os.environ["frame_duration"])

    # This function assumes that the mp4 we processed contains one or more frames, the time of the start
    # of the mp4 can be calculated from the filename index * frame duration
    # Assumption: the S3 prefixes are of the form:
    # <name>-

    path_elems = key.split("/")
    file_elems = path_elems[-1].split(".")

    file = f"{file_elems[0]}.json"
    f = io.BytesIO(json.dumps(file_labels).encode())
    upload_path = "/".join(path_elems[0:-1])
    upload_key = f"{upload_path}/{file}"
    logging.info(f"Uploading {bucket}/{upload_key}")
    s3.upload_fileobj(f, bucket, upload_key)


    # get the timestamp from the filename which will of the format 
    # image_raw-2020-12-16T23_32_19.969307-0002.png
    year, month, day_time = file_elems[0].split('-')[1:4]
    # iso 
    day_time = day_time.replace('_',':')
    iso = f'{year}-{month}-{day_time}'
    frame_time = datetime.fromisoformat(iso)

    camera = re.match("[A-Za-z_]*", path_elems[-2]).group(0)
    s3_key = "/".join([bucket, key]).replace("mp4", "png")
    table = os.environ["results_table"]

    db_key = {"timestamp": {"S": frame_time.isoformat()}, "camera": {"S": camera}}

    item = {
        "timestamp": {"S": frame_time.isoformat()},
        "camera": {"S": camera},
        "s3_loc": {"S": s3_key},
        #        "json" : {"S" : json.dumps(file_labels)} # handy for development but not necessary as the json is also in S3
    }

    # Put the entry into the table
    dynamo.put_item(TableName=table, Item=item)

    # Upated the table with each detection
    ped_cnt = 0
    bike_cnt = 0
    motorbike_cnt = 0
    for l in file_labels:

        # Keep a count of the number of people/bikes/motorbikes in the image
        name = l["Name"].replace(" ", "_")
        # Ignore items where there's no bounding box
        if "Instances" in l:
            count = len(l["Instances"])
            if count == 0:
                continue
            if name == "Person":
                print(name)
                ped_cnt = ped_cnt + count
            elif name == "Bicycle":
                print(name)
                bike_cnt = bike_cnt + count
            elif name == "Motorcycle":
                print(name)
                motorbike_cnt = motorbike_cnt + count

        update_expression = f"SET {name} = :conf"
        condition_expression = f"attribute_not_exists({name}) OR {name} < :conf"

        #
        try:

            dynamo.update_item(
                TableName=table,
                Key=db_key,
                UpdateExpression=update_expression,
                ConditionExpression=condition_expression,
                ExpressionAttributeValues={":conf": {"N": f'{l["Confidence"]}'}},
            )
        except ClientError as e:
            logging.warning(e)

    try:

        dynamo.update_item(
            TableName=table,
            Key=db_key,
            UpdateExpression=f"SET Ped_Count = :peds, Bike_Count  = :bikes, Motorbike_Count = :motorbikes ",
            ExpressionAttributeValues={
                ":peds": {"N": f"{ped_cnt}"},
                ":bikes": {"N": f"{bike_cnt}"},
                ":motorbikes": {"N": f"{motorbike_cnt}"},
            },
        )
    except ClientError as e:
        logging.warning(e)
    file_labels = []


def process_labels(bucket, key, labels):

    export_json_file(labels, bucket, key)


def lambda_handler(event, context):

    print(json.dumps(event))

    sqs = boto3.client("sqs")
    rek = boto3.client("rekognition")
    ddb = boto3.client("dynamodb")
    s3 = boto3.client("s3")

    messages = event["Records"]

    label_count = 0
    sleep_time = 0
    monitor_table = os.environ["monitor_table"]

    for m in messages:

        body = json.loads(m["body"])
        print(f"Body: {body}")
        if "Records" not in body:
            continue
        bucket = body["Records"][0]["s3"]["bucket"]["name"]
        key = body["Records"][0]["s3"]["object"]["key"]
        s3_loc = "/".join([bucket, key])
        receipt_handle = m["receiptHandle"]

        # only process raw images
        if key.contains('image_raw'):
            
            json_key = key.replace("png", "json")
            print(f'key: {key}')
            response = rek.detect_labels(
                Image={"S3Object": {"Bucket": bucket, "Name": key}}
            )
            print(response)
    
            process_labels(bucket, key, response["Labels"])
            ddb.update_item(
                TableName=monitor_table,
                Key={"img_file": {"S": s3_loc}},
                UpdateExpression="SET #s = :sts, #e = :now",
                ExpressionAttributeNames={"#s": "Status", "#e": "End"},
                ExpressionAttributeValues={
                    ":sts": {"S": "Complete"},
                    ":now": {"S": datetime.now().isoformat()},
                },
            )

        sqs.delete_message(
            QueueUrl=os.environ["job_queue_url"], ReceiptHandle=receipt_handle
        )

    return {"status": 200}
