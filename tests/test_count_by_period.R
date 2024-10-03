library(dplyr)
library(dbplyr)
library(bigrquery)
library(DBI)

# Establish a connection to BigQuery
project_id <- "nih-nci-dceg-connect-stg-5519"
dataset_id <- "FlatConnect"
con <- dbConnect(
  bigrquery::bigquery(),
  project = project_id,
  dataset = dataset_id,
  use_legacy_sql = FALSE
)

# Reference your BigQuery table
your_table <- tbl(con, "FlatConnect.participants_JP")

# Aggregate counts by week
weekly_counts <- count_by_period(
  tbl = your_table,
  date_col = d_471593703,
  period = "week",
  date_type = "string"
)
weekly_counts_local <- weekly_counts %>% collect()
print(weekly_counts_local)

# Aggregate counts by month
monthly_counts <- count_by_period(
  tbl = your_table,
  date_col = d_471593703,
  period = "month",
  date_type = "string"
)
monthly_counts_local <- monthly_counts %>% collect()
print(monthly_counts_local)

# Aggregate counts by week starting on Sunday
quarterly_counts <- count_by_period(
  tbl = your_table,
  date_col = d_471593703,
  period = "quarter",
  date_type = "string"
)
quarterly_counts_local <- quarterly_counts %>% collect()
print(quarterly_counts_local)


weekly_counts_by_site_region <- count_by_period(
  tbl = your_table,
  date_col = "d_471593703",
  period = "week",
  week_start = "SUNDAY",
  date_type = "string",
  group_vars = c("d_827220437", "d_564964481")
)
weekly_counts_by_site_region_local <- weekly_counts_by_site_region %>% collect()
print(weekly_counts_by_site_region_local)
