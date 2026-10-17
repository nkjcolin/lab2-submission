data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "elasticbeanstalk_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["elasticbeanstalk.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = ["elasticbeanstalk"]
    }
  }
}

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eb_service" {
  name               = "${var.project_name}-eb-service"
  assume_role_policy = data.aws_iam_policy_document.elasticbeanstalk_assume.json
}

resource "aws_iam_role_policy_attachment" "eb_enhanced_health" {
  role       = aws_iam_role.eb_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSElasticBeanstalkEnhancedHealth"
}

resource "aws_iam_role_policy_attachment" "eb_managed_updates" {
  role       = aws_iam_role.eb_service.name
  policy_arn = "arn:aws:iam::aws:policy/AWSElasticBeanstalkManagedUpdatesCustomerRolePolicy"
}

resource "aws_iam_role" "web" {
  name               = "${var.project_name}-web"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "web_tier" {
  role       = aws_iam_role.web.name
  policy_arn = "arn:aws:iam::aws:policy/AWSElasticBeanstalkWebTier"
}

data "aws_iam_policy_document" "web_application" {
  statement {
    sid       = "EcrAuthorization"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "PullWebImages"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [
      aws_ecr_repository.frontend.arn,
      aws_ecr_repository.backend.arn,
    ]
  }

  statement {
    sid       = "ListPublishedResults"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.result_prefix_root}/*", local.result_pointer_key]
    }
  }

  statement {
    sid       = "ReadPublishedResults"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.data.arn}/results/*"]
  }

  statement {
    sid       = "ReadElasticBeanstalkBundle"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.data.arn}/deployments/*"]
  }

  statement {
    sid    = "ManageAnalysisJobs"
    effect = "Allow"
    actions = [
      "dynamodb:DeleteItem",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:TransactWriteItems",
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.jobs.arn]
  }

  statement {
    sid       = "RunOnlyLabAnalysisTask"
    effect    = "Allow"
    actions   = ["ecs:RunTask"]
    resources = [aws_ecs_task_definition.analysis.arn]

    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [aws_ecs_cluster.analysis.arn]
    }
  }

  statement {
    sid       = "ReadLabAnalysisTaskStatus"
    effect    = "Allow"
    actions   = ["ecs:DescribeTasks"]
    resources = ["arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:task/${aws_ecs_cluster.analysis.name}/*"]
  }

  statement {
    sid     = "PassOnlyLabEcsTaskRoles"
    effect  = "Allow"
    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.analysis.arn,
      aws_iam_role.analysis_execution.arn,
    ]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }

  statement {
    sid     = "ReadWebSecrets"
    effect  = "Allow"
    actions = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = [
      aws_ssm_parameter.analysis_api_token.arn,
      aws_ssm_parameter.origin_verify.arn,
    ]
  }

  statement {
    sid       = "SetContainerSafeImdsHopLimit"
    effect    = "Allow"
    actions   = ["ec2:ModifyInstanceMetadataOptions"]
    resources = ["arn:aws:ec2:${var.aws_region}:${data.aws_caller_identity.current.account_id}:instance/*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/elasticbeanstalk:environment-name"
      values   = ["${var.project_name}-web"]
    }
  }
}

resource "aws_iam_role_policy" "web_application" {
  name   = "${var.project_name}-web-application"
  role   = aws_iam_role.web.id
  policy = data.aws_iam_policy_document.web_application.json
}

resource "aws_iam_instance_profile" "web" {
  name = "${var.project_name}-web"
  role = aws_iam_role.web.name
}

resource "aws_iam_role" "analysis" {
  name               = "${var.project_name}-analysis"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "analysis" {
  statement {
    sid       = "ReadInput"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.data.arn}/${local.input_key}"]
  }

  statement {
    sid       = "ListOwnDataPrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "input",
        "input/*",
        "${local.result_prefix_root}/*",
        local.result_pointer_key,
      ]
    }
  }

  statement {
    sid    = "PublishValidatedResults"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = [
      "${aws_s3_bucket.data.arn}/${local.result_prefix_root}/*",
      "${aws_s3_bucket.data.arn}/${local.result_pointer_key}",
    ]
  }

  statement {
    sid    = "UpdateOwnJob"
    effect = "Allow"
    actions = [
      "dynamodb:UpdateItem",
    ]
    resources = [aws_dynamodb_table.jobs.arn]
  }

}

resource "aws_iam_role_policy" "analysis" {
  name   = "${var.project_name}-analysis"
  role   = aws_iam_role.analysis.id
  policy = data.aws_iam_policy_document.analysis.json
}

resource "aws_iam_role" "analysis_execution" {
  name               = "${var.project_name}-analysis-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "analysis_execution" {
  statement {
    sid       = "EcrAuthorization"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid    = "PullAnalysisImage"
    effect = "Allow"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
    ]
    resources = [aws_ecr_repository.analysis.arn]
  }

  statement {
    sid    = "WriteAnalysisLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.analysis.arn}:*"]
  }
}

resource "aws_iam_role_policy" "analysis_execution" {
  name   = "${var.project_name}-analysis-execution"
  role   = aws_iam_role.analysis_execution.id
  policy = data.aws_iam_policy_document.analysis_execution.json
}
