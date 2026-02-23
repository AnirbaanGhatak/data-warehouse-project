resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/20"
  enable_dns_support = true
  enable_dns_hostnames = true

  tags = {
    Name = "Datalake_VPC"
  }
}

resource "aws_subnet" "private" {
  vpc_id = aws_vpc.main.id
  cidr_block = "10.0.0.0/24"
  availability_zone = "ap-south-1a"

  tags = {
    Name = "Private-Subnet-A"
  }
}


resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "Provate-Route-Table"
  }
}

resource "aws_route_table_association" "private_assoc" {
  subnet_id = aws_subnet.private.id
  route_table_id = aws_route_table.private.id
}
