data "aws_elastic_beanstalk_solution_stack" "docker" {
  most_recent = true
  name_regex  = "^64bit Amazon Linux 2023 .* running Docker$"
}

resource "random_password" "analysis_api" {
  length  = 40
  special = false
}

resource "random_password" "origin_verify" {
  length  = 40
  special = false
}

resource "random_password" "viewer" {
  length  = 24
  special = false
}

resource "aws_ssm_parameter" "analysis_api_token" {
  name  = "/${var.project_name}/web/analysis-api-token"
  type  = "SecureString"
  value = random_password.analysis_api.result
}

resource "aws_ssm_parameter" "origin_verify" {
  name  = "/${var.project_name}/web/origin-verify"
  type  = "SecureString"
  value = random_password.origin_verify.result
}

resource "aws_ecs_cluster" "analysis" {
  name = "${var.project_name}-analysis"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

resource "aws_cloudwatch_log_group" "analysis" {
  name              = "/ecs/${var.project_name}-analysis"
  retention_in_days = 1
}

resource "aws_ecs_task_definition" "analysis" {
  family                   = "${var.project_name}-analysis"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.analysis_task_cpu)
  memory                   = tostring(var.analysis_task_memory)
  execution_role_arn       = aws_iam_role.analysis_execution.arn
  task_role_arn            = aws_iam_role.analysis.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name      = "analysis"
      image     = local.analysis_image
      essential = true
      command   = ["python", "-m", "analysis.cloud_runner"]
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "AWS_JOB_TABLE", value = aws_dynamodb_table.jobs.name },
        { name = "DATA_BUCKET", value = aws_s3_bucket.data.id },
        { name = "INPUT_KEY", value = local.input_key },
        { name = "RESULT_POINTER_KEY", value = local.result_pointer_key },
        { name = "RESULT_PREFIX_ROOT", value = local.result_prefix_root },
        { name = "SPARK_LOCAL_IP", value = "127.0.0.1" },
        { name = "SPARK_MASTER", value = "local[1]" },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.analysis.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "analysis"
        }
      }
    }
  ])

  depends_on = [
    aws_iam_role_policy.analysis,
    aws_iam_role_policy.analysis_execution,
    aws_route_table_association.public,
  ]
}

resource "aws_elastic_beanstalk_application" "app" {
  name        = var.project_name
  description = "CS5224 follower analysis frontend and API"
}

resource "aws_elastic_beanstalk_application_version" "app" {
  name        = "${var.project_name}-${var.image_tag}"
  application = aws_elastic_beanstalk_application.app.name
  bucket      = aws_s3_bucket.data.id
  key         = aws_s3_object.eb_bundle.key

  depends_on = [aws_s3_object.eb_bundle]
}

resource "aws_elastic_beanstalk_environment" "app" {
  name                = "${var.project_name}-web"
  application         = aws_elastic_beanstalk_application.app.name
  solution_stack_name = data.aws_elastic_beanstalk_solution_stack.docker.name
  tier                = "WebServer"
  version_label       = aws_elastic_beanstalk_application_version.app.name

  setting {
    namespace = "aws:elasticbeanstalk:environment"
    name      = "EnvironmentType"
    value     = "SingleInstance"
  }

  setting {
    namespace = "aws:elasticbeanstalk:environment"
    name      = "ServiceRole"
    value     = aws_iam_role.eb_service.arn
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "IamInstanceProfile"
    value     = aws_iam_instance_profile.web.name
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "InstanceType"
    value     = var.eb_instance_type
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "SecurityGroups"
    value     = aws_security_group.web.id
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "DisableDefaultEC2SecurityGroup"
    value     = "true"
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "DisableIMDSv1"
    value     = "true"
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "LaunchTemplateTagPropagationEnabled"
    value     = "true"
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "RootVolumeType"
    value     = "gp3"
  }

  setting {
    namespace = "aws:autoscaling:launchconfiguration"
    name      = "RootVolumeSize"
    value     = tostring(var.eb_root_volume_gib)
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "VPCId"
    value     = aws_vpc.main.id
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "Subnets"
    value     = aws_subnet.public.id
  }

  setting {
    namespace = "aws:ec2:vpc"
    name      = "AssociatePublicIpAddress"
    value     = "true"
  }

  setting {
    namespace = "aws:elasticbeanstalk:healthreporting:system"
    name      = "SystemType"
    value     = "enhanced"
  }

  setting {
    namespace = "aws:elasticbeanstalk:managedactions"
    name      = "ManagedActionsEnabled"
    value     = "false"
  }

  dynamic "setting" {
    for_each = {
      AWS_ECS_CLUSTER           = aws_ecs_cluster.analysis.arn
      AWS_ECS_CONTAINER_NAME    = "analysis"
      AWS_ECS_SECURITY_GROUP_ID = aws_security_group.analysis.id
      AWS_ECS_SUBNET_ID         = aws_subnet.public.id
      AWS_ECS_TASK_DEFINITION   = aws_ecs_task_definition.analysis.arn
      AWS_JOB_TABLE             = aws_dynamodb_table.jobs.name
      AWS_REGION                = var.aws_region
      DATA_BUCKET               = aws_s3_bucket.data.id
      INPUT_KEY                 = local.input_key
      JOB_HISTORY_TTL_DAYS      = tostring(var.job_history_ttl_days)
      RESULT_POINTER_KEY        = local.result_pointer_key
      RESULT_PREFIX_ROOT        = local.result_prefix_root
    }
    content {
      namespace = "aws:elasticbeanstalk:application:environment"
      name      = setting.key
      value     = setting.value
    }
  }

  setting {
    namespace = "aws:elasticbeanstalk:application:environmentsecrets"
    name      = "ANALYSIS_API_TOKEN"
    value     = aws_ssm_parameter.analysis_api_token.arn
  }

  setting {
    namespace = "aws:elasticbeanstalk:application:environmentsecrets"
    name      = "ORIGIN_VERIFY_SECRET"
    value     = aws_ssm_parameter.origin_verify.arn
  }

  depends_on = [
    aws_iam_role_policy.web_application,
    aws_iam_role_policy_attachment.eb_enhanced_health,
    aws_iam_role_policy_attachment.eb_managed_updates,
    aws_iam_role_policy_attachment.web_tier,
    aws_route_table_association.public,
    aws_s3_object.input,
  ]
}
