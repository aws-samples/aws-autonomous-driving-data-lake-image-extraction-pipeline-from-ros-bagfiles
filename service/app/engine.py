import boto3
import os
import subprocess
import shutil
import yaml
import logging
from botocore.exceptions import ClientError
import pandas as pd
import time
import psutil
import tarfile
import sys
import time


working_dir=os.environ['ROS_HOME']

def print_files_in_path(d):
    logging.warning(d)
    fs = absolute_file_paths(d)
    for f in fs:
        logging.warning(f)


def download_bag_file(config):

    csv_dir = os.path.join(working_dir, "csvs")
    clean_directory(working_dir)
    clean_directory(csv_dir)

    if config["s3_bag_file_prefix"].endswith(".tar.gz"):
        local_tar_file = get_object(
            config["s3_bag_file_bucket"], config["s3_bag_file_prefix"], working_dir
        )
        logging.warning("Untarring")
        tar = tarfile.open(local_tar_file, "r:gz")
        tar.extractall(working_dir)
        print_files_in_path(working_dir)
        tar.close()
        logging.warning(f"Deleting {local_tar_file}")
        os.remove(local_tar_file)
        local_bag_files = [
            x for x in absolute_file_paths(working_dir) if x.endswith(".bag")
        ]
        assert (
            len(local_bag_files) == 1
        ), f"More than 1 bag file found {local_bag_files}"
        local_bag_file = local_bag_files[0]

        # logging.warning("testing efs - copy 1")
        # shutil.copyfile(local_bag_file, local_bag_file + "_2")
        # logging.warning("testing efs - copy 2")
        # shutil.copyfile(local_bag_file, local_bag_file + "_3")
        # logging.warning("testing efs - copy 3")
        # shutil.copyfile(local_bag_file, local_bag_file + "_4")
        # logging.warning(psutil.disk_usage('/'))
        # logging.warning(psutil.disk_usage(working_dir))
        # os.remove(local_bag_file + "_4")
        # os.remove(local_bag_file + "_3")
        # os.remove(local_bag_file + "_2")

    else:
        # Download Bag File from S3
        local_bag_file = get_object(
            config["s3_bag_file_bucket"], config["s3_bag_file_prefix"], working_dir
        )

    return local_bag_file, working_dir


def parse_bag(config):

    logging.warning(psutil.disk_usage("/"))
    logging.warning(config)

    local_bag_file, working_dir = download_bag_file(config)

    try:
        # get the bagfile info and  upload to S3 for debug/future reference
        info_filename = (
            config["s3_bag_file_prefix"].split("/")[-1].replace("bag", "info")
        )
        info_filename = f"{working_dir}/{info_filename}"
        info = open(info_filename, "w")
        subprocess.run(
            f"source /opt/ros/melodic/setup.bash; rosbag info {local_bag_file}",
            shell=True,
            stdout=info,
        )
        info.close()

        # Play back at 1/10th speed to avoid overruns
        retcode = subprocess.call(
            f"source /opt/ros/melodic/setup.bash; rosbag play -r 0.1 {local_bag_file}",
            shell=True,
        )
        if retcode < 0:
            print(f"Child was terminated by signal {-retcode} {sys.stderr}")
        else:
            print(f"Child returned  {retcode} {sys.stderr}")

        files = os.listdir(working_dir)
        print(f"list:{files}")
        png_files = [x for x in files if ".png" in x]
        channels = {x[:-8] for x in png_files}

        print(f"PNG files:{png_files}")

        # generate mp4 files
        [
            subprocess.call(
                f'ffmpeg -framerate {config["framerate"]} -i {working_dir}/{x}%04d.png -c:v libx264 -crf 20 -pix_fmt yuv420p {working_dir}/{x}.mp4',
                shell=True,
            )
            for x in channels
        ]

    except OSError as e:
        print("Execution failed:", e, file=sys.stderr)

    logging.info(local_bag_file)

    #    s3_output_prefix = '/'.join(config['s3_bag_file_prefix'].split('/')[-2:-1])
    #    print(s3_output_prefix)

    #    s3_output_prefix = os.path.join(s3_output_prefix, local_bag_file.replace('.bag', ''))

    s3_output_prefix = config["s3_bag_file_prefix"].replace(".bag", "")
    print(f"s3-prefix {s3_output_prefix}")

    now = working_dir.split("/")
    now = now[len(now) - 1]

    s3_sync_results(
        config["s3_output_bucket"],
        s3_output_prefix,
        working_dir,
    )

    logging.warning(psutil.disk_usage("/"))
    shutil.rmtree(working_dir, ignore_errors=True)
    logging.warning(psutil.disk_usage("/"))

    return "success"


def convert_csv_to_parquet(csv_dir, parquet_dir):
    csv_files = [c for c in absolute_file_paths(csv_dir) if c.endswith(".csv")]
    logging.warning(csv_files)
    for c in csv_files:
        logging.warning(f"csv file: {c}")
        now = int(time.time())
        csv_prefix = c.split(csv_dir)[-1]
        parquet_path = os.path.join(
            parquet_dir, csv_prefix.replace(".csv", f".parquet")
        )
        parquet_partition_dir = "/".join(parquet_path.split("/")[0:-1])
        if not os.path.exists(parquet_partition_dir):
            os.makedirs(parquet_partition_dir)
        pd.read_csv(c).to_parquet(parquet_path, compression="snappy")



def clean_directory(dir):
    try:
        shutil.rmtree(dir)
    except Exception as e:
        logging.warning(f'Failed to remove {dir}: {e}')
    try:
        os.makedirs(dir)
    except Exception as e:
        logging.warning(e)

def make_config_yaml(topics_to_extract, local_dir):

    topics_to_extract = topics_to_extract.split(",")

    local_file = os.path.join(local_dir, "config.yaml")

    acceptable_topics = [
        "/gps",
        "/gps_time",
        "/imu",
        "/pose_ground_truth",
        "/pose_localized",
        "/pose_raw",
        "/tf",
        "/velocity_raw",
    ]

    topics_to_extract = list(set([str(x) for x in topics_to_extract]))
    for t in topics_to_extract:
        assert t in acceptable_topics, t + " not in topic whitelist: {ts}".format(
            ts=acceptable_topics
        )

    dict_file = {
        "topicsToBeAdded": topics_to_extract,
        "topicsToBeExcluded": [
            t for t in acceptable_topics if t not in topics_to_extract
        ],
    }

    with open(local_file, "w") as file:
        documents = yaml.dump(dict_file, file)

    logging.warning(documents)

    return local_file


def get_object(bucket, object_path, local_dir):
    local_path = os.path.join(local_dir, object_path.split("/")[-1])
    s3 = boto3.client("s3")
    logging.warning(
        "Getting s3://{bucket}/{prefix}".format(bucket=bucket, prefix=object_path)
    )
    s3.download_file(bucket, object_path, local_path)
    return local_path


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        if object_name.startswith("/root/"):
            object_name = object_name.replace("/root/", "", 1)
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def local_bags(dir):
    all_files = absolute_file_paths(dir)
    return [f for f in all_files if f.endswith(".bag")]


def absolute_file_paths(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def s3_sync_results(bucket, prefix, local_dir):

    logging.warning(f"Sending data from {local_dir} to {bucket}/{prefix}")
    files = absolute_file_paths(local_dir)
    count = 0
    for local_path in files:
        logging.warning(f"file found: {local_path}")
        if (
            local_path.endswith(".parquet")
            or local_path.endswith(".png")
            or local_path.endswith(".mp4")
            or local_path.endswith(".info")
            or local_path.endswith(".log")
        ):

            f = local_path.split('/')[-1]
            s3_path = os.path.join(prefix, f)
            logging.warning("Uploading " + local_path + " to " + s3_path)
            success = upload_file(local_path, bucket, object_name=s3_path)
            if not success:
                raise ClientError("Failed to upload to s3")
            count = count + 1
    print(f"Uploaded {count} files to S3")
