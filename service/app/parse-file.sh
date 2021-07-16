#!/bin/bash

bag=$1
config=$2

cd /app/catkin_ws
source devel/setup.bash
echo $ROS_PACKAGE_PATH
python2 /app/catkin_ws/src/AVData/ford_demo/scripts/bag_to_csv.py $bag $config
cd /app