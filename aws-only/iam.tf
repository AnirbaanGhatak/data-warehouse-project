resource "aws_iam_role" "glue_role" {
  name = "glue-service-cm"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "glue_s3_policy" {
  name = "Glue-s3-worker"

  policy = jsonencode(({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket"]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}",
          "${aws_s3_bucket.data_lake.arn}/raw/erm/*",
          "${aws_s3_bucket.data_lake.arn}/raw/crm/*",
          "${aws_s3_bucket.data_lake.arn}/scripts/*"
        ]
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/curated/*",
          "${aws_s3_bucket.data_lake.arn}/quarantine/*",
          "${aws_s3_bucket.data_lake.arn}/temp/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups"
        ],
        "Resource" : "*"
      }
    ]
  }))
}

resource "aws_iam_role_policy_attachment" "glue_attach_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_service_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

#--------------------------------------LAMBDA-----------------------------------------


resource "aws_iam_role" "lambda_role" {
  name = "lambda-audit-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "lambda_policy" {
  name = "Lambda_audit_policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:UpdateItem"],
        Resource = aws_dynamodb_table.audit_table.arn
      },
      {
        Effect   = "Allow",
        Action   = ["glue:StartJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}
