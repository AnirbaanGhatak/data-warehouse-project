resource "aws_s3_bucket" "data_lake" {
  bucket = var.s3_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls = true
  block_public_policy = true
  ignore_public_acls = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "raw_crm" {
  bucket = aws_s3_bucket.data_lake.id
  key = "raw/crm/"
  source = "./dataset/crm/"

}


resource "aws_s3_object" "raw_erp" {
  bucket = aws_s3_bucket.data_lake.id
  key = "raw/erp"
  source = "./dataset/erp/"
}

#post glue processing where files will live
resource "aws_s3_object" "processed_multi_cloud" {
  bucket = aws_s3_bucket.data_lake.id
  key = "processed/"
}