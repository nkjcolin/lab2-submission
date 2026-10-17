variable "aws_profile" {
  description = "AWS CLI shared-credentials profile used by Terraform."
  type        = string
  default     = "colin"

  validation {
    condition     = var.aws_profile == "colin"
    error_message = "This lab deployment must use the AWS CLI profile colin."
  }
}

variable "aws_region" {
  description = "Region for all regional application resources."
  type        = string
  default     = "ap-southeast-2"

  validation {
    condition     = var.aws_region == "ap-southeast-2"
    error_message = "This deployment is intentionally pinned to ap-southeast-2."
  }
}

variable "project_name" {
  description = "Lowercase name used in resource names."
  type        = string
  default     = "cs5224-lab2"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project_name))
    error_message = "project_name must be 3-31 lowercase letters, digits, or hyphens."
  }
}

variable "eb_instance_type" {
  description = "Always-on Elastic Beanstalk instance. Keep small for the demo."
  type        = string
  default     = "t3.micro"

  validation {
    condition     = contains(["t3.micro", "t3.small"], var.eb_instance_type)
    error_message = "eb_instance_type must be t3.micro or t3.small."
  }
}

variable "eb_root_volume_gib" {
  description = "Elastic Beanstalk encrypted gp3 root volume size."
  type        = number
  default     = 12
}

variable "analysis_task_cpu" {
  description = "Fargate CPU units for PySpark. 1024 keeps the one-off lab run reasonably short."
  type        = number
  default     = 1024

  validation {
    condition     = contains([256, 512, 1024], var.analysis_task_cpu)
    error_message = "analysis_task_cpu must be 256, 512, or 1024."
  }
}

variable "analysis_task_memory" {
  description = "Fargate task memory in MiB. Four GiB gives Spark and the S3A client safe headroom."
  type        = number
  default     = 4096

  validation {
    condition = (
      (var.analysis_task_cpu == 256 && contains([512, 1024, 2048], var.analysis_task_memory)) ||
      (var.analysis_task_cpu == 512 && contains([1024, 2048, 3072, 4096], var.analysis_task_memory)) ||
      (var.analysis_task_cpu == 1024 && var.analysis_task_memory >= 2048 && var.analysis_task_memory <= 8192 && var.analysis_task_memory % 1024 == 0)
    )
    error_message = "analysis_task_memory must be a Fargate-supported value for analysis_task_cpu."
  }
}

variable "image_tag" {
  description = "Immutable tag shared by frontend, backend, and analysis images."
  type        = string
  default     = "v1"

  validation {
    condition     = can(regex("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$", var.image_tag))
    error_message = "image_tag must be a valid non-empty Docker tag."
  }
}

variable "input_file_path" {
  description = "Path to twitter_combined.txt. Null uses ../../upload/twitter_combined.txt."
  type        = string
  default     = null
  nullable    = true
}

variable "upload_input_file" {
  description = "Upload the input dataset as a private Terraform-managed S3 object."
  type        = bool
  default     = true
}

variable "viewer_username" {
  description = "Username for the CloudFront Basic Authentication gate."
  type        = string
  default     = "labadmin"

  validation {
    condition     = length(var.viewer_username) > 0 && !strcontains(var.viewer_username, ":")
    error_message = "viewer_username must be non-empty and cannot contain a colon."
  }
}

variable "cloudfront_price_class" {
  description = "PriceClass_100 is the lowest-cost CloudFront class."
  type        = string
  default     = "PriceClass_100"

  validation {
    condition     = contains(["PriceClass_100", "PriceClass_200", "PriceClass_All"], var.cloudfront_price_class)
    error_message = "cloudfront_price_class is invalid."
  }
}

variable "job_history_ttl_days" {
  description = "TTL applied by the application to completed DynamoDB job records."
  type        = number
  default     = 7
}

variable "force_destroy" {
  description = "Permit final cleanup to remove non-empty S3/ECR resources. Enable only for teardown."
  type        = bool
  default     = false
}
