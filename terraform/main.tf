terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# BigQuery Dataset
resource "google_bigquery_dataset" "marketing" {
  dataset_id  = "marketing"
  description = "Marketing data warehouse"
  location    = var.region

  labels = {
    environment = var.environment
    team        = "data-engineering"
  }
}

# Product table
resource "google_bigquery_table" "product" {
  dataset_id          = google_bigquery_dataset.marketing.dataset_id
  table_id            = "product"
  deletion_protection = false

  schema = jsonencode([
    { name = "sku_id", type = "STRING", mode = "REQUIRED" },
    { name = "sku_description", type = "STRING", mode = "NULLABLE" },
    { name = "price", type = "FLOAT64", mode = "REQUIRED" }
  ])
}

# Sales table (partitioned by date)
resource "google_bigquery_table" "sales" {
  dataset_id          = google_bigquery_dataset.marketing.dataset_id
  table_id            = "sales"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "orderdate_utc"
  }

  clustering = ["sku_id"]

  schema = jsonencode([
    { name = "order_id", type = "STRING", mode = "REQUIRED" },
    { name = "sku_id", type = "STRING", mode = "REQUIRED" },
    { name = "orderdate_utc", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "sales", type = "INT64", mode = "REQUIRED" }
  ])
}

# Revenue table (partitioned + clustered)
resource "google_bigquery_table" "revenue" {
  dataset_id          = google_bigquery_dataset.marketing.dataset_id
  table_id            = "revenue"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "date_id"
  }

  clustering = ["sku_id"]

  schema = jsonencode([
    { name = "sku_id", type = "STRING", mode = "REQUIRED" },
    { name = "date_id", type = "DATE", mode = "REQUIRED" },
    { name = "price", type = "FLOAT64", mode = "REQUIRED" },
    { name = "sales", type = "INT64", mode = "REQUIRED" },
    { name = "revenue", type = "FLOAT64", mode = "REQUIRED" }
  ])
}