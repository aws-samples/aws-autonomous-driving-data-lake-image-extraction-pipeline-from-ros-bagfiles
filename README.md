
# ROS bag image extraction pipeline and Model Training
This solution describes a workflow that processes ROS bag files on Amazon S3, extracts the PNG files from a video stream using AWS Fargate on Amazon Elastic Container Services. The solution builds a DynamoDB table containing all detection results from Amazon Rekognition, which can be queried to find images of interest such as images containing cars. Afterwards, we want to label these images and fine-tune a Object Detection Model to detect cars on the road. For simplicity reasons, we have provided an example SageMaker Ground Truth Manifest File from a Bounding Boxes Labeling Job. In order to train the Object Detection Model we will convert the SageMaker Ground Truth Manifest file into the RecordIO file format, after we have visually inspected the annotation quality from our labelling job.

## Initial Configuration and Deployment of the CDK Stack

Note that deploying this stack may incur charges on your AWS account. See the section 'Cleaning Up' for instructions on 
how to remove the stack when you have finished with it.

    Define 3 names for your infrastructure in config.json:
    
    {
          "ecr-repository-name": "my-ecr-repository",
          "image-name": "my-image",
          "stack-id": "my-stack"
    }
   
   Ypu will need to ensure that you have also created an ECR repository matching the name used above (in this case my-ecr-rpository)
   
   Optionally (leave these as they are unless you know you need to change them), define other parameters for your Docker 
   container, such as number of vCPUs and RAM it should consume, in config.json:
    
          "cpu": 4096,
          "memory-limit-mib": 12288,
          "timeout-minutes": 2
          "environment-variables": {}
   
   [Fargate CPU and Memory Limit Documentation](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html)
     
   In deploy.sh, set REGION to teh region you are using for the deployment. The REPO_NAME and IMAGE_NAME should match 
   the values in your config.json:

    '''   
    REPO_NAME=vsi-rosbag-image-repository # Should match the ecr repository name given in config.json
    IMAGE_NAME=my-vsi-ros-image          # Should match the image name given in config.json
    REGION=eu-west-1
    '''
   
   

Extending the code to meet your use case:
    Extend the ./service/app/engine.py file to add more complex transformation logic
    
    Customizing Input
        Add prefix and suffix filters for the S3 notifications in config.json
        
    


deploy.sh with build=true will create an ecr repository in your account, if it does not yet exist, and push your docker image to that repository
Then it will execute the CDK command to deploy all infrastructure defined in app.py and ecs_stack.py 
          
          
The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the .env
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .env
```

After the init process completes and the virtualenv is created, you can use the following
step to test deployment

```
$ bash deploy.sh <cdk-command> <build?>

$ bash deploy.sh deploy true
```


To add additional dependencies, for example other CDK libraries, just add
them to your `requirements.txt` or `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Fine-tuning of the Machine Learning Model

Once you have launched the stack explained above, you can clone the package `object-detection` from this repository into
the SageMaker Notebook Instance named `ros-bag-demo-notebook` that has been created in your account. Once you have 
cloned it, the next steps are outlined in the Notebook `Transfer-Learning.ipynb`.

## Cleaning up

To remove the resources from your account you can run:
'''
$ cdk destroy
'''

note that the S3 buckets will not be deleted unless you empty them first.

## Useful CDK commands

 * `bash deploy.sh default ls false eu-west-1`          list all stacks in the app
 * `bash deploy.sh default synth false eu-west-1`       emits the synthesized CloudFormation template
 * `bash deploy.sh default deploy true eu-west-1`      build and deploy this stack to your default AWS account/region
 * `bash deploy.sh default diff false eu-west-1`        compare deployed stack with current state
 * `bash deploy.sh default docs false eu-west-1`        open CDK documentation

