from bagstream import bagFileStream
import os
import boto3
import logging
from multiprocessing import Process, Queue
import subprocess
import uuid



class Uploader(Process):
    def __init__(self, s3_bucket):
        self.s3_bucket = s3_src_bucket
        self.q = Queue()
        self._image_dirs = []
        self.working_dir = f'/root/efs/{uuid.uuid1().hex}/'
        super().__init__()


    def run(self):

        s3 = boto3.client('s3')
        while True:
            file = self.q.get()
            logging.info(f'uploading {file} to bucket {self.s3_bucket}')
            try:
                s3_prefix = file.replace(self.working_dir,'')
                s3.upload_file(file, self.s3_bucket, s3_prefix)
                image_dir = '/'.join(file.split('/')[:-1])
                if file.endswith('.png'):
                    if image_dir not in self._image_dirs:
                        self._image_dirs.append(image_dir)
                
            except Exception as e:
                logging.warning(e)


    def get_queue(self):
        return self.q
        
    
    def terminate(self):
        """
        replace the terminate method to generate mp4 videos for each of the 
        cameras and upload them to s3 before actually terminating
        """
        print("terminating!")
        if "framerate" in os.environ:
            framerate = os.environ["framerate"]
        else:
            framerate = 20


        try:
            # generate mp4 files
            print (f'processing {self._image_dirs}')
            [
                subprocess.call(
                    f'ffmpeg -framerate {framerate} -i {self.working_dir}{x}%04d.png -c:v libx264 -crf 20 -pix_fmt yuv420p {self.working_dir}{x}.mp4',
                    shell=True,
                )
                for x in self._image_dirs
            ]
            # upload the mp4s we just generated
            for x in self._image_dirs:
                s3.upload_file(f'{x}.mp4', self.s3_bucket, f'{x.mp4}')
                
        except Exception as e:
            logging.warn(e)
            
        return super().terminate()



def upload_callback(file):
    logging.info(f'queuing {file} for upload to bucket {s3_src_bucket}')
    queue.put(file)


if __name__ == '__main__':    
    
    s3 = boto3.client('s3')
    s3_src_bucket = os.environ["s3_source"]
    s3_src_key = os.environ["s3_source_prefix"]
    s3_dest_bucket = os.environ["s3_destination"]
    
    upload = Uploader(s3_dest_bucket)
    upload.start()
    queue = upload.get_queue()

    key_root = s3_src_key.split('.')[:-1]
    file_root = '/'.join(key_root[0].split('/')[0:-1]).replace('.', '')
    # get the name of teh input file without the .bag extension
    datafolder = os.path.join('/'.join(key_root), file_root)


    input_stream = s3.get_object(Bucket=s3_src_bucket, Key=s3_src_key)['Body']

    bagfile = bagFileStream(input_stream,upload_callback, output_prefix=upload.working_dir+datafolder)
    bagfile.upload_csvs()


    while not queue.empty():
        pass
    upload.terminate()
    #
    #subprocess.call(
    #    f'rm -rf {upload.working_dir}',
    #    shell=True,
    #)

    

