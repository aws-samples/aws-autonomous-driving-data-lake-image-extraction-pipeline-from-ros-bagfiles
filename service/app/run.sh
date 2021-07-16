#!/bin/bash
/ros_entrypoint.sh
source /opt/ros/melodic/setup.bash
export ROS_HOME=/root/efs/$s3_source_prefix
echo $ROS_PACKAGE_PATH
export PYTHONPATH=$PYTHONPATH:$ROS_PACKAGE_PATH
env
python3 main.py