output "live_url" {
  description = "HTTPS review URL. The same origin serves React and /api."
  value       = "https://${aws_cloudfront_distribution.app.domain_name}"
}

output "cloudfront_distribution_id" {
  value = aws_cloudfront_distribution.app.id
}

output "elastic_beanstalk_environment" {
  value = aws_elastic_beanstalk_environment.app.name
}

output "elastic_beanstalk_origin" {
  description = "Origin DNS is deliberately HTTP-only and security-group restricted to CloudFront."
  value       = aws_elastic_beanstalk_environment.app.cname
}

output "data_bucket" {
  value = aws_s3_bucket.data.id
}

output "job_table" {
  value = aws_dynamodb_table.jobs.name
}

output "analysis_ecs_cluster" {
  value = aws_ecs_cluster.analysis.name
}

output "analysis_task_definition" {
  value = aws_ecs_task_definition.analysis.arn
}

output "frontend_ecr_repository" {
  value = aws_ecr_repository.frontend.repository_url
}

output "backend_ecr_repository" {
  value = aws_ecr_repository.backend.repository_url
}

output "analysis_ecr_repository" {
  value = aws_ecr_repository.analysis.repository_url
}

output "viewer_username" {
  value = var.viewer_username
}

output "viewer_password" {
  description = "Retrieve only when needed: terraform output -raw viewer_password"
  value       = random_password.viewer.result
  sensitive   = true
}

output "verification_commands" {
  value = <<-TEXT
    curl --fail --user '${var.viewer_username}:<terraform-output-viewer-password>' 'https://${aws_cloudfront_distribution.app.domain_name}/api/health'
    aws --profile ${var.aws_profile} --region ${var.aws_region} ecs list-tasks --cluster ${aws_ecs_cluster.analysis.name} --desired-status RUNNING
    aws --profile ${var.aws_profile} --region ${var.aws_region} s3 ls s3://${aws_s3_bucket.data.id}/results/ --recursive
  TEXT
}
