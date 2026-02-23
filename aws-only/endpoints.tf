resource "aws_vpc_endpoint" "s3" {
  vpc_id = aws_vpc.main.id

  service_name = "com.amazonaws.ap-south-1.s3"

  vpc_endpoint_type = "Gateway"

  route_table_ids = [aws_route_table.private.id]

  tags = {
    Name = "S3-Gateway-Endpoint"
  }
}

resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id = aws_vpc.main.id

  service_name = "com.amazonaws.ap-south-1.dynamodb"

  vpc_endpoint_type = "Gateway"

  route_table_ids = [aws_route_table.private.id]

  tags = {
    Name = "DyanmoDB-Gateway-Endpoint"
  }
}