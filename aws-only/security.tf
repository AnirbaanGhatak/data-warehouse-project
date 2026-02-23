resource "aws_security_group" "glue_sg" {
  name = "glue-internal-sg"
  description = "Allow Glue internal traffic"

  vpc_id = aws_vpc.main.id

  ingress {
    from_port = 0
    to_port = 65535
    protocol = "tcp"
    self = true
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}