data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ec2_managed_prefix_list" "cloudfront" {
  name = "com.amazonaws.global.cloudfront.origin-facing"
}

resource "aws_vpc" "main" {
  cidr_block           = "10.52.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags       = { Name = "${var.project_name}-vpc" }
  depends_on = [terraform_data.prerequisites]
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project_name}-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.52.1.0/24"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true

  tags = { Name = "${var.project_name}-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${var.project_name}-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "web" {
  name        = "${var.project_name}-cloudfront-only"
  description = "Elastic Beanstalk HTTP origin reachable only by CloudFront"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project_name}-web" }
}

resource "aws_vpc_security_group_ingress_rule" "cloudfront_http" {
  security_group_id = aws_security_group.web.id
  description       = "HTTP from the AWS-managed CloudFront origin-facing prefix"
  prefix_list_id    = data.aws_ec2_managed_prefix_list.cloudfront.id
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "web_all" {
  security_group_id = aws_security_group.web.id
  description       = "ECR, S3, DynamoDB, ECS, SSM Parameter Store, and package endpoints"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "analysis" {
  name        = "${var.project_name}-analysis-no-ingress"
  description = "On-demand Fargate analysis task with no inbound rules"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project_name}-analysis" }
}

resource "aws_vpc_security_group_egress_rule" "analysis_all" {
  security_group_id = aws_security_group.analysis.id
  description       = "Outbound HTTPS and image/package retrieval; no inbound access"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
