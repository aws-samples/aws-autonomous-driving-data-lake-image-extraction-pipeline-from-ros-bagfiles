from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3,
    aws_s3_notifications as s3n,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_efs as efs,
    aws_events,
    aws_events_targets as targets,
    aws_iam,
    aws_sns,
    aws_sns_subscriptions as sns_subs,
    aws_sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda,
    aws_lambda_event_sources as les,
    aws_dynamodb as dynamodb,
    aws_sagemaker as sagemaker,
    custom_resources as cr,
    core,
    aws_logs,
)

import boto3
import os
import json


account = boto3.client("sts").get_caller_identity().get("Account")
region = boto3.session.Session().region_name


class Fargate(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        image_name: str,
        ecr_repository_name: str,
        environment_vars: dict,
        memory_limit_mib: int,
        cpu: int,
        timeout_minutes: int,
        **kwargs,
    ) -> None:

        super().__init__(scope, id, **kwargs)

        src_bucket = aws_s3.Bucket(
            self,
            removal_policy=core.RemovalPolicy.DESTROY,
            id="src-bucket",
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
        )

        dest_bucket = aws_s3.Bucket(
            self,
            id="dest-bucket",
            removal_policy=core.RemovalPolicy.DESTROY,
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
        )

        # Create VPC and Fargate Cluster
        # NOTE: Limit AZs to avoid reaching resource quotas
        vpc = ec2.Vpc(self, f"MyVpc", max_azs=2)

        private_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE)

        # EFS
        fs = efs.FileSystem(
            self,
            "efs",
            vpc=vpc,
            encrypted=True,
            removal_policy=core.RemovalPolicy.DESTROY,
            throughput_mode=efs.ThroughputMode.BURSTING,
            performance_mode=efs.PerformanceMode.MAX_IO,
        )

        access_point = fs.add_access_point(
            "AccessPoint",
            path="/",
            create_acl=efs.Acl(owner_uid="0", owner_gid="0", permissions="750"),
            posix_user=efs.PosixUser(uid="0", gid="0"),
        )

        # ECS Task Role
        arn_str = "arn:aws:s3:::"

        ecs_task_role = aws_iam.Role(
            self,
            "ecs_task_role2",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchFullAccess"
                )
            ],
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:ReEncrypt*",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey",
                ],
                resources=["*"],
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(actions=["s3:Get*", "s3:List*"], resources=["*"])
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:PutObject*"], resources=["*"]
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["*"], resources=[access_point.access_point_arn]
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "elasticfilesystem:ClientMount",
                    "elasticfilesystem:ClientWrite",
                    "elasticfilesystem:DescribeMountTargets",
                ],
                resources=["*"],
            )
        )

        # Define task definition with a single container
        # The image is built & published from a local asset directory
        print(fs.file_system_id)
        vc = ecs.EfsVolumeConfiguration(
            file_system_id=fs.file_system_id,
            authorization_config=ecs.AuthorizationConfig(
                access_point_id=access_point.access_point_id, iam="ENABLED"
            ),
            transit_encryption="ENABLED",
        )
        task_definition = ecs.FargateTaskDefinition(
            self,
            f"{image_name}_task_definition",
            family=f"{image_name}-family",
            cpu=cpu,
            memory_limit_mib=memory_limit_mib,
            task_role=ecs_task_role,
            volumes=[ecs.Volume(name="rosbagVolume", efs_volume_configuration=vc)],
        )

        repo = ecr.Repository.from_repository_name(
            self, id=id, repository_name=ecr_repository_name
        )
        img = ecs.EcrImage.from_ecr_repository(repository=repo, tag="latest")

        logs = ecs.LogDriver.aws_logs(
            stream_prefix="ecs",
            log_group=aws_logs.LogGroup(self, f"{image_name}-log-group2"),
        )

        container_name = f"{image_name}-container"

        container_def = task_definition.add_container(
            container_name,
            image=img,
            memory_limit_mib=memory_limit_mib,
            environment={"topics_to_extract": "/tf"},
            logging=logs,
        )
        mp = ecs.MountPoint(
            container_path="/root/efs", read_only=False, source_volume="rosbagVolume"
        )
        container_def.add_mount_points(mp)

        # Define an ECS cluster hosted within the requested VPC
        cluster = ecs.Cluster(
            self,
            "cluster",
            cluster_name=f"{image_name}-cluster",
            container_insights=True,
            vpc=vpc,
        )

        run_task = tasks.EcsRunTask(
            self,
            "fargatetask",
            assign_public_ip=False,
            subnets=private_subnets,
            cluster=cluster,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.VERSION1_4
            ),
            task_definition=task_definition,
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=task_definition.default_container,
                    environment=[
                        tasks.TaskEnvironmentVariable(
                            name=k, value=sfn.JsonPath.string_at(v)
                        )
                        for k, v in environment_vars.items()
                    ],
                )
            ],
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            input_path=sfn.JsonPath.entire_payload,
            output_path=sfn.JsonPath.entire_payload,
            timeout=core.Duration.minutes(timeout_minutes),
        )
        run_task.add_retry(
            backoff_rate=1, interval=core.Duration.seconds(60), max_attempts=1920
        )

        fs.connections.allow_default_port_from(run_task.connections)

        state_logs = aws_logs.LogGroup(self, "stateLogs")
        state_machine = sfn.StateMachine(
            self,
            "RunTaskStateMachine",
            definition=run_task,
            timeout=core.Duration.minutes(timeout_minutes),
            logs=sfn.LogOptions(destination=state_logs),
        )

        state_machine.grant_task_response(ecs_task_role)

        input_bag_queue = aws_sqs.Queue(
            self, "inputBagQueue", visibility_timeout=core.Duration.minutes(5)
        )
        # send .png object created events to our SQS input queue
        src_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(input_bag_queue),
            aws_s3.NotificationKeyFilter(suffix="bag"),
        )

        # Create the SQS queue for input/results jobs and SNS for job completion notifications
        dlq = aws_sqs.Queue(self, "dlq")
        rek_job_queue = aws_sqs.Queue(
            self, "rekJobQueue", visibility_timeout=core.Duration.minutes(2)
        )
        rek_results_queue = aws_sqs.Queue(
            self, "rekResultQueue", visibility_timeout=core.Duration.minutes(5)
        )

        ## TESTING - this lamda is for development and allows us to push a bunch of .bag files through
        # without having to copy them into teh src bucket. Create a manifest and then use that with
        # an S3 batch job to run this.
        s3_batch_lambda = aws_lambda.Function(
            self,
            "S3Batchprocessor",
            code=aws_lambda.Code.from_asset("./infrastructure/S3Batch"),
            environment={
                "bag_queue_url": input_bag_queue.queue_url,
                "job_queue_url": rek_job_queue.queue_url,
            },
            memory_size=3008,
            timeout=core.Duration.minutes(5),
            vpc=vpc,
            retry_attempts=0,
            handler="s3batch.lambda_handler",
            runtime=aws_lambda.Runtime("python3.7", supports_inline_code=True),
            security_groups=fs.connections.security_groups,
        )

        input_bag_queue.grant_send_messages(s3_batch_lambda.role)
        rek_job_queue.grant_send_messages(s3_batch_lambda.role)
        s3_batch_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:ReEncrypt*",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey",
                ],
                resources=["*"],
            )
        )
        s3_batch_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:Get*", "s3:PutObject"], resources=["*"]
            )
        )

        bag_queue_lambda = aws_lambda.Function(
            self,
            "BagQueueProcessor",
            code=aws_lambda.Code.from_asset("./infrastructure/bag-queue-proc"),
            environment={
                "bag_queue_url": input_bag_queue.queue_url,
                "state_machine_arn": state_machine.state_machine_arn,
                "dest_bucket": dest_bucket.bucket_name,
                "topics_to_extract": "/gps",
            },
            memory_size=3008,
            timeout=core.Duration.minutes(5),
            vpc=vpc,
            retry_attempts=0,
            handler="bag-queue-proc.lambda_handler",
            runtime=aws_lambda.Runtime("python3.7", supports_inline_code=True),
            security_groups=fs.connections.security_groups,
        )
        # SQS queue of .bag files to be processed
        bag_queue_lambda.add_event_source(les.SqsEventSource(input_bag_queue))
        input_bag_queue.grant_consume_messages(s3_batch_lambda.role)
        bag_queue_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:ReEncrypt*",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey",
                ],
                resources=["*"],
            )
        )
        bag_queue_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:Get*", "s3:PutObject"], resources=["*"]
            )
        )
        bag_queue_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn],
            )
        )

        ## setup the rekogition labelling pipeline..

        # create a DynamoDB table to hold the rseults
        rek_labels_db = dynamodb.Table(
            self,
            "RekResultsTable2",
            partition_key=dynamodb.Attribute(
                name="timestamp", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="camera", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # create a DynamoDB table to monitor/debug the pipeline
        rek_monitor_db = dynamodb.Table(
            self,
            "RekMonitor",
            partition_key=dynamodb.Attribute(
                name="img_file", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # send .png object created events to our SQS input queue
        dest_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(rek_job_queue),
            aws_s3.NotificationKeyFilter(suffix="png"),
        )

        # Lambda to call Rekogition DetectLabels API syncronously
        process_rek_sync_lambda = aws_lambda.Function(
            self,
            "RekSyncProcessor",
            code=aws_lambda.Code.from_asset("./infrastructure/process-queue-sync"),
            environment={
                "job_queue_url": rek_job_queue.queue_url,
                "results_table": rek_labels_db.table_name,
                "monitor_table": rek_monitor_db.table_name,
                "frame_duration": "67",
            },
            memory_size=3008,
            # reserved_concurrent_executions=20,
            timeout=core.Duration.minutes(2),
            vpc=vpc,
            retry_attempts=0,
            handler="process-queue-sync.lambda_handler",
            runtime=aws_lambda.Runtime("python3.7", supports_inline_code=True),
            security_groups=fs.connections.security_groups,
        )

        process_rek_sync_lambda.add_event_source(les.SqsEventSource(rek_job_queue))
        rek_labels_db.grant_read_write_data(process_rek_sync_lambda.role)
        rek_monitor_db.grant_read_write_data(process_rek_sync_lambda.role)
        process_rek_sync_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "rekognition:DetectLabels",
                ],
                resources=["*"],
            )
        )
        process_rek_sync_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:ReEncrypt*",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey",
                ],
                resources=["*"],
            )
        )
        process_rek_sync_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:Get*", "s3:PutObject"], resources=["*"]
            )
        )

        # Lambda to anonymize the images which contain a VRU
        anon_labelling_imgs = aws_s3.Bucket(
            self,
            id="anon-labelling-imgs",
            removal_policy=core.RemovalPolicy.DESTROY,
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
        )

        pillow_layer = aws_lambda.LayerVersion(
            self,
            "PillowLayer",
            code=aws_lambda.Code.from_asset("./infrastructure/pillow-layer"),
            compatible_runtimes=[aws_lambda.Runtime("python3.6")],
        )

        select_labelling_imgs = aws_lambda.Function(
            self,
            "SelectLabellingImgs",
            code=aws_lambda.Code.from_asset("./infrastructure/select-labelling-imgs"),
            environment={
                "image_bucket": anon_labelling_imgs.bucket_name,
            },
            memory_size=3008,
            timeout=core.Duration.minutes(10),
            vpc=vpc,
            retry_attempts=0,
            handler="select-labelling-imgs.lambda_handler",
            runtime=aws_lambda.Runtime("python3.6"),
            security_groups=fs.connections.security_groups,
        )

        select_labelling_imgs.add_layers(pillow_layer)

        dest_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(select_labelling_imgs),
            aws_s3.NotificationKeyFilter(suffix="json"),
        )

        select_labelling_imgs.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:ReEncrypt*",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey",
                ],
                resources=["*"],
            )
        )
        select_labelling_imgs.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:Get*", "s3:PutObject"], resources=["*"]
            )
        )
        select_labelling_imgs.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["rekognition:DetectText", "rekognition:DetectFaces"],
                resources=["*"],
            )
        )

        ## Set up sagemaker notebook

        sm_nb_role = aws_iam.Role(
            self,
            "SmNbRole",
            assumed_by = aws_iam.ServicePrincipal('sagemaker.amazonaws.com'),
            managed_policies = [aws_iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSageMakerFullAccess')]
        )
        sm_nb_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                resources=["*"]
            )
        )
        sm_notebook = sagemaker.CfnNotebookInstance(
            self,
            id = "ros-bag-demo-notebook",
            role_arn = sm_nb_role.role_arn,
            notebook_instance_name = 'ros-bag-demo-notebook',
            instance_type = 'ml.t2.medium'
        )
