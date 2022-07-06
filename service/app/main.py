from bagstream import bagFileStream
import os
import boto3
import logging
from multiprocessing import Process, Queue
import subprocess
import uuid


class Uploader(Process):
    """
    Uploader creates a separate process to upload file to S3 as they are generated. It creates a Queue for passing the
    names of files to be uploaded to the run() method, which gets spawned when start() is called
    """
    def __init__(self, s3_dest_bucket, framerate):
        """ Constructor take the destination S3 bucket name as an argument"""
        self.s3_dest_bucket = s3_dest_bucket
        self.framerate = framerate
        self.q = Queue()
        self.working_dir = f"/root/efs/{uuid.uuid1().hex}/"
        self.image_dirs = set()
        super().__init__()

    def run(self):
        """ run method pulls filenames from the queue and uploads them to S3. If it pulls 'Finished' from the queue
        it will generate mp4s for any channels with png images and upload those to S3 before terminating"""

        s3 = boto3.client("s3")
        while True:
            file = self.q.get()
            if file == 'Finished':
                self.generate_mp4s(s3)
                return
            logging.info(f"uploading {file} to bucket {self.s3_dest_bucket}")
            try:
                s3_prefix = file.replace(self.working_dir, "")
                s3.upload_file(file, self.s3_dest_bucket, s3_prefix)
                image_dir = "/".join(file.split("/")[:-1])
                if file.endswith(".png"):
                    print(f"adding {image_dir}")
                    self.image_dirs.add(image_dir)
                    print(f"{self.image_dirs}")

            except Exception as e:
                logging.warning(e)

    def generate_mp4s(self, s3):
        """
        Goes through the list of directories containing png files and generates an mp4 for each of them. These are
        uploaded to S3 along side the images.
        """

        # generate mp4 files
        print(f"processing {self.image_dirs}")

        for x in self.image_dirs:
            try:
                subprocess.call(
                    f"ffmpeg -framerate {self.framerate} -i {x}/image_raw-%04d.png -c:v libx264 -crf 20 -pix_fmt yuv420p {x}.mp4",
                    shell=True,
                )
                s3_prefix = x.replace(self.working_dir, "")
                s3.upload_file(f"{x}.mp4", self.s3_dest_bucket, f"{s3_prefix}.mp4")

            except Exception as e:
                logging.warning(e)

    def upload_callback(self,file):
        """ Call back function to pass to the bagFileStream object. Just queues the file for upload"""
        logging.info(f"queuing {file} for upload to bucket {s3_src_bucket}")
        self.q.put(file)


if __name__ == "__main__":

    s3_src_bucket = os.environ["s3_source"]
    s3_src_key = os.environ["s3_source_prefix"]
    s3_dest_bucket = os.environ["s3_destination"]
    if "framerate" in os.environ:
        framerate = os.environ["framerate"]
    else:
        framerate = 20

    upload = Uploader(s3_dest_bucket, framerate)
    upload.start()

    key_root = s3_src_key.split(".")[:-1]
    file_root = "/".join(key_root[0].split("/")[0:-1]).replace(".", "")
    # get the name of the input file without the .bag extension
    datafolder = os.path.join("/".join(key_root), file_root)

    s3 = boto3.client("s3")
    input_stream = s3.get_object(Bucket=s3_src_bucket, Key=s3_src_key)["Body"]

    bagfile = bagFileStream(
        input_stream, upload.upload_callback, output_prefix=upload.working_dir + datafolder
    )
    bagfile.upload_csvs()

    upload.upload_callback('Finished')
    upload.join()
    upload.close()

    # Clean up
    subprocess.call(
        f'rm -rf {upload.working_dir}',
        shell=True,
    )
