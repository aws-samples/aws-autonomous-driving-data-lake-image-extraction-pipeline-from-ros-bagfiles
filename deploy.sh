#!/bin/bash


cmd=$1
build=$2


export aws_account_id=$(aws sts get-caller-identity --query Account --output text)

REPO_NAME=vsi-rosbag-image-repository # Should match the ecr repository name given in config.json
IMAGE_NAME=my-vsi-ros-image          # Should match the image name given in config.json
REGION=eu-west-1

python3 -m venv .env
source .env/bin/activate
pip install -r requirements.txt | grep -v 'already satisfied'
cdk bootstrap aws://$aws_account_id/$REGION

if [ $build = true ] ;
then
    export repo_url=$aws_account_id.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME
    docker build ./service -t $IMAGE_NAME:latest
    last_image_id=$(docker images | awk '{print $3}' | awk 'NR==2')
    docker tag $last_image_id $repo_url
    #This command requires AWSCLI V2
    aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $repo_url
    docker push $repo_url:latest
else
  echo Skipping build
fi

cdk $cmd --region $REGION

