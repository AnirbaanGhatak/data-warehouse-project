#explaination about this


resource "aws_iam_user" "bq_dts" {
  name = var.big_query_transfer_name
}

resource "aws_iam_access_key" "bq_dts_access_key" {
  user = aws_iam_user.bq_dts.name
}

# DEFINE PERMISSIONS (Least Privilege)
# ONLY read from bucket. nothing else.

resource "aws_iam_user_policy" "bq_dts_policy" {
  name = "allow_s3_read"
  user = aws_iam_user.bq_dts.name

  policy = jsonencode(
    {
      "Action" : [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Version" : "2025-11-29",
      "Statement" : [
        {
          "Effect" : "Allow",

          "Resource" : [
            "${aws_s3_bucket.data_lake.arn}",
            "${aws_s3_bucket.data_lake.arn}/*"
          ]
        }
      ]
    }
  )


}
