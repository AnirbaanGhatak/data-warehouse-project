resource "aws_athena_workgroup" "dl_wg"{
    name = "dl_workgroup"

    configuration {
      result_configuration {
        output_location = "s3://${aws_s3_bucket.data_lake}/athena-results/"
      }
    }
}

resource "aws_glue_catalog_database" "data_lake_db" {
  name = "dl_db"
}