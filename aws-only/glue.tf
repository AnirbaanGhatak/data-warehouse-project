resource "aws_glue_connection" "vpc_connection" {
  name = "glu-VPC-connection"

  physical_connection_requirements {
    availability_zone = aws_subnet.private.availability_zone
    subnet_id = aws_subnet.private.id
    security_group_id_list = [aws_security_group.glue_sg.id]
  }
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.data_lake.id
  key = "scripts/main.py"
  source = "${path.module}/src/glue_etl.py"
  etag = filemd5("${path.module}/src/glue_etl.py")
}

resource "aws_glue_job" "etl_job" {
  name = "ETL_job"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/main.py"
    python_version = "3.10"
  }

  default_arguments = {
    "--job-language" = "python"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--enable-metrics" = "true"
    "--enable-continuous-cloudwatch-log" = "true"

    "--bucket-name" = aws_s3_bucket.data_lake.id
  }

  number_of_workers = 2
  worker_type = "G.1X"

  connections = [aws_glue_connection.vpc_connection.name]
}