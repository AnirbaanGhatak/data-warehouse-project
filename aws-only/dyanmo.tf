resource "aws_dynamodb_table" "audit_table" {
    name = "Pipeline-audit-log"
#   billing_mode = "PAY"
    hash_key = "file_id"

    attribute {
      name = "file_id"
      type = "S"
    }

    point_in_time_recovery {
      enabled = true
    }
}

