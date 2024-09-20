# Configure the AWS provider
# provider "aws" {
#   region = "ap-southeast-1"
# }

# Define variables
variable "stage" {
  default = "dev"
}

# DynamoDB Table
resource "aws_dynamodb_table" "dynamodb_table" {
  name         = "tsanghan-ce6-dynamodb-table-${var.stage}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }
}

# SQS Queue
resource "aws_sqs_queue" "sqs_queue" {
  name = "tsanghan-ce6-sqs-queue-${var.stage}"
}

# IAM Role for Lambda
data "aws_iam_policy_document" "lambda_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_role" {
  name               = "lambda_role_${var.stage}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
}

# IAM Policy for DynamoDB and SQS access
data "aws_iam_policy_document" "lambda_policy" {
  statement {
    effect = "Allow"

    actions = [
      "dynamodb:PutItem"
    ]

    resources = [
      aws_dynamodb_table.dynamodb_table.arn
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "sqs:SendMessage"
    ]

    resources = [
      aws_sqs_queue.sqs_queue.arn
    ]
  }

  # Basic Lambda execution permissions
  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["arn:aws:logs:*:*:*"]
  }
}

# Attach IAM Policy to the Role
resource "aws_iam_role_policy" "lambda_policy_attachment" {
  name   = "lambda_policy_${var.stage}"
  role   = aws_iam_role.lambda_role.id
  policy = data.aws_iam_policy_document.lambda_policy.json
}

data "archive_file" "lambda_zip" {
  type = "zip"

  source_file = "${path.module}/lambda_function.py"
  output_path = "${path.module}/lambda_function.zip"
}

# AWS Lambda Function
resource "aws_lambda_function" "lambda_function" {
  function_name = "tsanghan-ce6-dynamodb-sqs-lambda-${var.stage}"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.12"

  # Package your Python code as a zip file named 'lambda_function.zip'
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = filebase64sha256("lambda_function.zip")

  environment {
    variables = {
      TABLE_NAME = aws_dynamodb_table.dynamodb_table.name
      QUEUE_URL  = aws_sqs_queue.sqs_queue.id
    }
  }

  depends_on = [
    aws_iam_role_policy.lambda_policy_attachment
  ]
}

# API Gateway (HTTP API)
resource "aws_apigatewayv2_api" "http_api" {
  name          = "tsanghan-ce6-http-api-${var.stage}"
  protocol_type = "HTTP"
}

# Lambda Integration with API Gateway
resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id                 = aws_apigatewayv2_api.http_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.lambda_function.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

# API Gateway Route
resource "aws_apigatewayv2_route" "lambda_route" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "POST /"

  target = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

# API Gateway Stage
resource "aws_apigatewayv2_stage" "default_stage" {
  api_id      = aws_apigatewayv2_api.http_api.id
  name        = "$default"
  auto_deploy = true
}

# Permission for API Gateway to Invoke Lambda
resource "aws_lambda_permission" "apigw_lambda_permission" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_function.function_name
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.http_api.execution_arn}/*/*"
}
