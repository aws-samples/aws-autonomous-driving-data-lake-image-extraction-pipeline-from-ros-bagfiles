# Copyright (c) Amazon Web Services
# Author : Clive Davies
# Initial Date: June 22 2021
# About: Class to extract the entire contents af a ROS bag file from an S3
#        streaming body and upload the contents to S3. Inspiration and some
#        code taken from the bagpy module by Rahul Bhandani
#        https://github.com/jmscslgroup/bagpy
# License: MIT License

#   Permission is hereby granted, free of charge, to any person obtaining
#   a copy of this software and associated documentation files
#   (the "Software"), to deal in the Software without restriction, including
#   without limitation the rights to use, copy, modify, merge, publish,
#   distribute, sublicense, and/or sell copies of the Software, and to
#   permit persons to whom the Software is furnished to do so, subject
#   to the following conditions:

#   The above copyright notice and this permission notice shall be
#   included in all copies or substantial portions of the Software.

#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
#   ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
#   TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
#   PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
#   SHALL THE AUTHORS, COPYRIGHT HOLDERS OR ARIZONA BOARD OF REGENTS
#   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
#   AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#   OR OTHER DEALINGS IN THE SOFTWARE.


import sys
import logging
from io import BytesIO
import bz2
import csv
import os
from PIL import Image
from rosbag import bag
from bagpy.bagreader import slotvalues
from datetime import datetime, timedelta


class ConnectionInfo (bag._ConnectionInfo):
    def __init__(self, conn):
        self.id = int.from_bytes(conn['conn'], byteorder='little')
        self.topic = conn['topic']
        self.datatype = conn ['type']
        self.md5sum = conn ['md5sum']
        self.msg_def = conn['message_definition']
        self.header = conn

logging.basicConfig(level=logging.INFO)

class bagFileStream:
    """
    Extracts data from a ROS bag file using streaming access only.
    
    This is really for data lake formation where we want to just extract 
    everything from the bag file.
    
    """

    def __init__(self, input_stream, upload_callback, output_prefix=''):

        self.filepos = 0

        self.bagfile = input_stream
        self.upload_callback = upload_callback
        self.bag_header = {}
        self.connections = {}
        self.output_prefix = output_prefix

        v_string = self.read_string( b'\n', self.bagfile)
        if '2.0' not in v_string:
            logging.info(f'Version {v_string} not supported. Only V2.0 is currently supported')
            exit()


        while True:
            self.bagfile, record_header = self.read_record_header(self.bagfile)
            logging.info(record_header)
            if record_header is None:
                return

            if self.process_record[record_header['op']]:
                self.bagfile = self.process_record[record_header['op']](self, record_header, self.bagfile)
            else:
                logging.warning(f'No handler for op code {record_header["op"]}')

    def read_string(self, terminator, bagfile):
        str_bytes=[]
        while True:
            b = bagfile.read(1)
            if b == terminator:
                break
            str_bytes.append(b)
        self.filepos += len(b)
        return b''.join(str_bytes).decode('ISO-8859-1')



    def read_record_header(self, bagfile,fields=None):
        if not fields:
            fields={}
        hdr_len = int.from_bytes(bagfile.read(4), byteorder='little')
        if hdr_len == 0:
            #EOF
            return None, None
        logging.info(f'header length: {hdr_len}')
        fields['hdr_len']=hdr_len
        if hdr_len == 0 :
            logging.warning('zero length header')

        while hdr_len >  0:
            field_len = int.from_bytes(bagfile.read(4), byteorder='little')
            self.filepos += 4
            field_name = self.read_string( b'=', bagfile)
            if field_name == 'op':
                fields[field_name] = int.from_bytes(bagfile.read(1), byteorder='little')
                logging.info(f'header op: {fields[field_name]}')
                self.filepos += 1
            else:
                fields[field_name] = bagfile.read(field_len - len(field_name) - 1)
                self.filepos += field_len - len(field_name) - 1

            hdr_len = hdr_len - field_len - 4

        fields['data_len']= int.from_bytes(bagfile.read(4), byteorder='little')

        return bagfile, fields

    def read_connection_header(self, bagfile,fields=None):
        if not fields:
            fields={}
        hdr_len = fields['data_len']
        logging.info(f'conn header length: {hdr_len}')
        fields['hdr_len']=hdr_len

        if hdr_len == 0 :
            logging.warning('zero length header')

        while hdr_len >  0:
            field_len = int.from_bytes(bagfile.read(4), byteorder='little')
            self.filepos += 4
            field_name = self.read_string( b'=', bagfile)
            if field_name == 'op':
                fields[field_name] = int.from_bytes(bagfile.read(1), byteorder='little')
                self.filepos += 1
                logging.info(f'conn header op: {fields[field_name]}')
            else:
                fields[field_name] = bagfile.read(field_len - len(field_name) - 1).decode('ISO-8859-1')
                self.filepos += field_len - len(field_name) - 1

            hdr_len = hdr_len - field_len - 4

        fields['data_len']= int.from_bytes(bagfile.read(4), byteorder='little')

        return bagfile, fields


    def process_bag_header(self, record, bagfile):
        self.bagfile.read(record['data_len'])
        self.bag_header = record
        return self.bagfile


    def process_connection(self, record, bagfile):
        con_hdr = BytesIO(bagfile.read(record['data_len']))
        self.read_connection_header(con_hdr, fields=record)
        csvfile=os.path.join(self.output_prefix, f"{record['topic']}.csv".replace('/','',1))
        dir = os.path.dirname(csvfile)
        if not os.path.exists(dir):
            os.makedirs(dir)
        record['csv_filename'] = csvfile
        csvf = open(csvfile, 'w', newline='')
        record['csv_file'] = csvf
        record['csv_writer']= csv.writer(csvf, delimiter=',')
        self.connections[record['conn']] = record
        record['frame_count'] = 0
        record['csv_header_written']= False
        return bagfile

    def process_chunk(self, record, bagfile):
        if record['compression'] == 'bz2':
            logging.warning('Compressed chunks not tested')
            data = bz2.decompress(BytesIO(self.bagfile.read(record['data_len'])))
        else:
            data = self.bagfile.read(record['data_len'])


        bytes_to_process = int.from_bytes(record['size'], byteorder='little')

        chunk_io = BytesIO(data)
        while bytes_to_process > 0:
            chunk_io, record_header = self.read_record_header(chunk_io)
            if record_header is None:
                break
            logging.info (record_header)
            if len(record_header)==0:
                exit()

            if self.process_record[record_header['op']]:
               chunk_io  = self.process_record[record_header['op']](self, record_header, chunk_io)
            else:
                logging.warning(f'No handler for op code {record_header["op"]}')
            bytes_to_process = bytes_to_process - record_header['hdr_len'] - record_header['data_len'] - 8
            logging.info(f'bytes to process: {bytes_to_process}')

        return bagfile

    def ros_time_to_iso(self, timestamp):
        time = datetime.fromtimestamp(0) + \
            timedelta(seconds=timestamp & 0xffffffff, microseconds=(timestamp >> 32) // 1000)
        # replace colons with underscores for s3 compatibility    
        return time.isoformat().replace(':', '_')


    def process_message(self, record_header, bagfile):
        data = bagfile.read(record_header['data_len'])
        conn = self.connections[record_header['conn']]
        record_header['time'] = int.from_bytes(record_header['time'], byteorder='little')
        record_header['isotime'] = self.ros_time_to_iso(record_header['time'])

        msg_type = bag._get_message_type(ConnectionInfo(conn ))
        msg = msg_type()
        msg.deserialize(data)

        msg_type = conn['type']
        if 'std_msgs' in msg_type:
            msg_type = 'std_msg'

        if msg_type in self.process_message_map.keys():
            self.process_message_map[msg_type](self, conn, data, record_header, msg)
        else:
            self.process_topic(conn, data, record_header, msg)
            logging.warning(f"unknown message type: {conn['type']}")
        return bagfile

    def process_unknown(self, record, bagfile):
        bagfile.read(record['data_len'])
        logging.warning(f"Unknown header op={record['op']}")
        return bagfile


    def process_image_data(self, conn, data, record_header, msg):


        img_encodings = {'rgb8': 'RGB', 'rgba8': 'RGBA', 'mono8': 'L', '8UC3' : 'RGB'}

        img_root = os.path.join(self.output_prefix, conn["topic"].replace('/','',1))
        img_file = os.path.join(f'{img_root}-{record_header["isotime"]}-{conn["frame_count"]:04d}.png')
        conn['frame_count'] = conn['frame_count'] + 1

        img = Image.frombytes(img_encodings[msg.encoding], (msg.width, msg.height), msg.data)

        if msg.encoding == '8UC3':
            b, g, r = img.split()
            img = Image.merge("RGB",(r,g,b))

        dir = os.path.dirname(img_file)
        if not os.path.exists(dir):
            os.makedirs(dir)
        img.save(img_file)

        self.upload_callback(img_file)

        new_row = [record_header['time'], record_header['isotime'], img_file]
        conn['csv_writer'].writerow(new_row)

    def process_laser_data(self, conn, data, record_header, msg):
        new_row = [record_header['time'],
                   record_header['isotime'],
                   msg.header.seq,
                   msg.header.frame_id,
                   msg.angle_min,
                   msg.angle_max,
                   msg.angle_increment,
                   msg.time_increment,
                   msg.scan_time,
                   msg.range_min,
                   msg.range_max]
        conn['csv_writer'].writerow(new_row)

    def process_std_data(self, conn, data, record_header, msg):
        new_row = [record_header['time'],
                   record_header['isotime'],
                   msg.data]
        conn['csv_writer'].writerow(new_row)

    def process_odometry_data(self, conn, data, record_header, msg):
        new_row = [record_header['time'],
                   record_header['isotime'],
                   msg.header.seq,
                   msg.header.frame_id,
                   msg.child_frame_id,
                   msg.pose.pose.position.x,
                   msg.pose.pose.position.y,
                   msg.pose.pose.position.z,
                   msg.pose.pose.orientation.x,
                   msg.pose.pose.orientation.y,
                   msg.pose.pose.orientation.z,
                   msg.pose.pose.orientation.w,
                   msg.twist.twist.linear.x,
                   msg.twist.twist.linear.y,
                   msg.twist.twist.linear.z]
        conn['csv_writer'].writerow(new_row)

    def process_wrench_data(self, conn, data, record_header, msg):

        new_row = [record_header['time'],
                   record_header['isotime'],
                   msg.force.x,
                   msg.force.y,
                   msg.force.z,
                   msg.torque.x,
                   msg.torque.y,
                   msg.torque.z]

        conn['csv_writer'].writerow(new_row)

    def process_topic(self,  conn, data, record_header, msg):

        if not conn['csv_header_written']:
            # set column names from the slots
            cols = ["Time", "ISOTime"]
            slots = msg.__slots__
            for s in slots:
                v, s = slotvalues(msg, s)
                if isinstance(v, tuple):
                    snew_array = []
                    p = list(range(0, len(v)))
                    snew_array = [s + "_" + str(pelem) for pelem in p]
                    s = snew_array

                if isinstance(s, list):
                    for i, s1 in enumerate(s):
                        cols.append(s1)
                else:
                    cols.append(s)
            conn['csv_writer'].writerow(cols)
            conn['csv_header_written'] = True

        slots = msg.__slots__
        vals = []
        vals.append(record_header['time'])
        vals.append(record_header['isotime'])
        for s in slots:
            v, s = slotvalues(msg, s)
            if isinstance(v, tuple):
                snew_array = []
                p = list(range(0, len(v)))
                snew_array = [s + "_" + str(pelem) for pelem in p]
                s = snew_array

            if isinstance(s, list):
                for i, s1 in enumerate(s):
                    vals.append(v[i])
            else:
                vals.append(v)

        conn['csv_writer'].writerow(vals)
    # Table of processing function indexed by record type 0-7
    process_record=[process_unknown,
                    process_unknown,
                    process_message,
                    process_bag_header,
                    process_unknown,
                    process_chunk,
                    process_unknown,
                    process_connection]

    def upload_csvs(self):
        for conn_key in self.connections.keys():
            conn = self.connections[conn_key]
            conn['csv_file'].close()
            self.upload_callback(conn['csv_filename'])

    process_message_map={'sensor_msgs/Image' : process_image_data,
                     'sensor_msgs/LaserScan' : process_laser_data,
                     "std_msgs" : process_std_data,
                     "nav_msgs/Odometry" : process_odometry_data,
                     "geometry_msgs/Wrench" : process_wrench_data
                     }





# See PyCharm help at https://www.jetbrains.com/help/pycharm/
