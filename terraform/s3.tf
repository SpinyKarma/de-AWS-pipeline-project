
resource "aws_s3_bucket" "raw_csv_data_bucket" {
    bucket_prefix = "terrific-totes-ingestion-bucket"
}

resource "aws_s3_bucket" "processed-paraquet-data" {
    bucket_prefix = "terrifc-totes-processed-bucket"
}

