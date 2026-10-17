resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data" {
  bucket        = "${var.project_name}-data-${random_id.suffix.hex}"
  force_destroy = var.force_destroy

  depends_on = [terraform_data.prerequisites]
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  rule {
    id     = "expire-old-result-versions"
    status = "Enabled"

    filter {
      prefix = "results/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.data]
}

resource "aws_s3_object" "input" {
  count = var.upload_input_file ? 1 : 0

  bucket       = aws_s3_bucket.data.id
  key          = local.input_key
  source       = local.input_file_path
  source_hash  = filemd5(local.input_file_path)
  content_type = "text/plain"

  depends_on = [
    aws_s3_bucket_ownership_controls.data,
    aws_s3_bucket_public_access_block.data,
    aws_s3_bucket_server_side_encryption_configuration.data,
  ]
}

resource "aws_dynamodb_table" "jobs" {
  name         = "${var.project_name}-jobs"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "job_id"

  attribute {
    name = "job_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  server_side_encryption {
    enabled = true
  }

  point_in_time_recovery {
    enabled = false
  }

  depends_on = [terraform_data.prerequisites]
}

resource "aws_ecr_repository" "frontend" {
  name                 = "${var.project_name}-frontend"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.force_destroy

  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }

  depends_on = [terraform_data.prerequisites]
}

resource "aws_ecr_repository" "backend" {
  name                 = "${var.project_name}-backend"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.force_destroy

  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }

  depends_on = [terraform_data.prerequisites]
}

resource "aws_ecr_repository" "analysis" {
  name                 = "${var.project_name}-analysis"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.force_destroy

  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }

  depends_on = [terraform_data.prerequisites]
}

resource "aws_ecr_lifecycle_policy" "repositories" {
  for_each = {
    frontend = aws_ecr_repository.frontend.name
    backend  = aws_ecr_repository.backend.name
    analysis = aws_ecr_repository.analysis.name
  }

  repository = each.value
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the three newest images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}

data "archive_file" "eb_bundle" {
  type        = "zip"
  output_path = "${path.module}/.terraform/${var.project_name}-${var.image_tag}.zip"

  source {
    filename = "docker-compose.yml"
    content  = local.eb_compose
  }

  source {
    filename = ".platform/hooks/predeploy/01-imdsv2-container-hop.sh"
    content  = local.eb_imds_hook
  }
}

resource "aws_s3_object" "eb_bundle" {
  bucket      = aws_s3_bucket.data.id
  key         = "deployments/${var.image_tag}/application.zip"
  source      = data.archive_file.eb_bundle.output_path
  source_hash = data.archive_file.eb_bundle.output_base64sha256

  depends_on = [
    aws_s3_bucket_ownership_controls.data,
    aws_s3_bucket_server_side_encryption_configuration.data,
  ]
}

data "aws_iam_policy_document" "data_bucket" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "data" {
  bucket = aws_s3_bucket.data.id
  policy = data.aws_iam_policy_document.data_bucket.json
}
