output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.marketing.dataset_id
}

output "revenue_table" {
  description = "Revenue table full ID"
  value       = "${var.project_id}.${google_bigquery_dataset.marketing.dataset_id}.${google_bigquery_table.revenue.table_id}"
}