import boto3
import json
import os
import logging
import time
from botocore.exceptions import ClientError
import shutil
import io
import sys
from PIL import Image, ImageDraw, ExifTags, ImageColor, ImageFilter

s3 = boto3.client("s3")


def iterate_bucket_items(bucket, prefix):
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                yield item


def detect_scene(bucket, key, region):
    client = boto3.client("rekognition", region)
    response = client.detect_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}},
        MaxLabels=10,
        MinConfidence=90,
    )
    return response["Labels"]


def filter_vru(annotations):

    contains_ped = False
    contains_wheeler = False
    if "Timestamp" in annotations[0]:
        # json is from StartLabelDetection i.e. video analysis with timestamps
        for a in annotations:
            if a["Label"]["Name"] in ["Person"]:
                contains_ped = True
            if a["Label"]["Name"] in ["Bicycle", "Motorcycle", "Motorbike", "Bike"]:
                contains_wheeler = True
    else:
        # json is from GetLabels i.e. image analysis and has no timestamps
        for a in annotations:
            if a["Name"] in ["Person"]:
                contains_ped = True
            if a["Name"] in ["Bicycle", "Motorcycle", "Motorbike", "Bike"]:
                contains_wheeler = True

    return contains_ped, contains_wheeler


def anonymize_PII(photo, bucket, key, region, blurriness=15):
    client = boto3.client("rekognition", region)
    # Call DetectText
    while True:
        try:
            response_text = client.detect_text(
                Image={"S3Object": {"Bucket": bucket, "Name": key}}
            )
            break
        except ClientError as e:
            print(e)
            time.sleep(5)

    image = Image.open(photo)
    imgWidth, imgHeight = image.size
    draw = ImageDraw.Draw(image)

    # Calculate and display bounding boxes for each detected text
    for text in response_text["TextDetections"]:
        box = text["Geometry"]["BoundingBox"]
        left = imgWidth * box["Left"]
        top = imgHeight * box["Top"]
        width = imgWidth * box["Width"]
        height = imgHeight * box["Height"]

        # blur text inside the bounding boxes
        x1 = left - 10
        y1 = top - 10
        x2 = left + width + 10
        y2 = top + height + 10

        mask = Image.new("L", image.size, 0)
        draw = ImageDraw.Draw(mask)
        draw.rectangle([(x1, y1), (x2, y2)], fill=255)
        blurred = image.filter(ImageFilter.GaussianBlur(blurriness))
        image.paste(blurred, mask=mask)

    while True:
        try:
            response_face = client.detect_faces(
                Image={"S3Object": {"Bucket": bucket, "Name": key}}
            )
            break
        except ClientError as e:
            print(e)
            time.sleep(5)

    print(response_face)
    # Calculate and display bounding boxes for each detected face
    for faceDetail in response_face["FaceDetails"]:
        box = faceDetail["BoundingBox"]
        left = imgWidth * box["Left"]
        top = imgHeight * box["Top"]
        width = imgWidth * box["Width"]
        height = imgHeight * box["Height"]

        # blur faces inside the bounding boxes
        x1 = left - 5
        y1 = top - 5
        x2 = left + width + 5
        y2 = top + height + 5

        mask = Image.new("L", image.size, 0)
        draw = ImageDraw.Draw(mask)
        draw.rectangle([(x1, y1), (x2, y2)], fill=255)
        blurred = image.filter(ImageFilter.GaussianBlur(blurriness))
        image.paste(blurred, mask=mask)

    image.save(photo)
    return image


def lambda_handler(event, context):

    print(event)
    for r in event["Records"]:

        bucket = r["s3"]["bucket"]["name"]
        key = r["s3"]["object"]["key"]
        image_bucket = os.environ["image_bucket"]
        region = r["awsRegion"]

        json_obj = boto3.resource("s3").Object(bucket, key)
        json_obj.wait_until_exists()

        png_key = key.replace(".json", ".png")

        local_file = png_key.split("/")[-1]

        local_file = f"/tmp/{local_file}"
        print(local_file)
        json_obj = s3.get_object(Bucket=bucket, Key=key)
        annotations = json.loads(json_obj["Body"].read())
        # print(annotations)
        ped, wheeler = filter_vru(annotations)
        if not ped and not wheeler:
            continue
        print(f"downloading {bucket}/{png_key}")
        s3.download_file(bucket, png_key, local_file)
        anonymize_PII(local_file, bucket, png_key, region)
        s3.upload_file(local_file, image_bucket, png_key)
        os.remove(local_file)


if __name__ == "__main__":

    event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "eu-west-1",
                "eventTime": "2020-10-26T14:53:21.544Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "AWS:AROA5CGYXMF26EFIRBV7L:my-rosbag-stack-RekResultsProcessor85514127-IFMMFKTRENS4"
                },
                "requestParameters": {"sourceIPAddress": "63.35.200.35"},
                "responseElements": {
                    "x-amz-request-id": "655E2C74743F26FE",
                    "x-amz-id-2": "z67goe00YvfStPVCgkg5d2WaTYzCKE1cvJkjjNsv5hy1uc7KxvmD059qhb2MWm3lLrmi1g1h53EQXco53iRTSukvD1cjPz4F",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "ZWE1NTFmN2MtZDViYy00N2NlLTk2NWUtNjc0YjBlMjU3N2U5",
                    "bucket": {
                        "name": "my-rosbag-stack-destbucket3708473c-hevpuy70d45q",
                        "ownerIdentity": {"principalId": "A1OHAXFWJQKA9R"},
                        "arn": "arn:aws:s3:::my-rosbag-stack-destbucket3708473c-hevpuy70d45q",
                    },
                    "object": {
                        "key": "20201005/20201005_112005/2020-10-05-11-21-54_17/rear0042.json",
                        "size": 2696,
                        "eTag": "0140dfe00749b329440413763b5ba5c5",
                        "sequencer": "005F96E2E45144BA89",
                    },
                },
            }
        ]
    }

    if len(sys.argv) == 2:
        event["Records"][0]["s3"]["object"]["key"] = sys.argv[1]

    os.environ[
        "image_bucket"
    ] = "my-rosbag-stack-anonlabellingimgsbb222971-qlwt6ify0kg4"
    lambda_handler(event, "")
    exit(0)

    response = s3.list_objects_v2(
        Bucket="my-rosbag-stack-destbucket3708473c-hevpuy70d45q"
    )
    count = 0

    while True:
        print(f"count:{response['KeyCount']}")
        for obj in response["Contents"]:
            if ".json" in obj["Key"]:
                event["Records"][0]["s3"]["object"]["key"] = obj["Key"]
                lambda_handler(event, "")
                count = count + 1
                print(count)
        if response["IsTruncated"] == True:
            response = s3.list_objects_v2(
                Bucket="my-rosbag-stack-destbucket3708473c-hevpuy70d45q",
                StartAfter=obj["Key"],
            )
        else:
            break
