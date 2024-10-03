#' Aggregate Records by Specified Time Period
#'
#' Transforms a date column in a \code{dbplyr} table connected to BigQuery by truncating each date to the start of the specified period (week, month, or quarter) and aggregates the data by counting the number of records in each period.
#'
#' @param tbl A \code{dbplyr} table object connected to BigQuery. Typically obtained using \code{tbl(con, "table_name")}.
#' @param date_col A quoted or unquoted column name representing the date to be transformed and aggregated. Utilizes tidy evaluation for flexibility.
#' @param period A character string specifying the aggregation period. Acceptable values are \code{"week"}, \code{"month"}, and \code{"quarter"}. Defaults to \code{"week"}.
#' @param week_start A character string indicating the starting day of the week when \code{period = "week"}. Acceptable values are \code{"MONDAY"} and \code{"SUNDAY"}. Defaults to \code{"MONDAY"}. This parameter is ignored when \code{period} is \code{"month"} or \code{"quarter"}.
#' @param date_type A character string specifying the data type of the \code{date_col}. Acceptable values are \code{"string"} and \code{"timestamp"}. Defaults to \code{"string"}.
#'
#' @return A \code{dbplyr} table object containing two columns:
#' \describe{
#'   \item{\code{period_start}}{The start date of the aggregation period (Monday for weeks, the first day of the month for months, and the first day of the quarter for quarters).}
#'   \item{\code{num_records}}{The count of records within each aggregation period.}
#' }
#'
#' @details
#' The \code{count_by_period} function is designed to streamline the process of aggregating time-series data by specified periods. It leverages the \code{dplyr} and \code{dbplyr} packages to translate R code into optimized SQL queries compatible with BigQuery.
#'
#' \strong{Key Features:}
#' \itemize{
#'   \item **Flexible Aggregation:** Supports weekly, monthly, and quarterly aggregations.
#'   \item **Custom Week Start:** Allows specification of the week's starting day (Monday or Sunday) for weekly aggregations.
#'   \item **Tidy Evaluation:** Accepts both quoted and unquoted column names for ease of use.
#'   \item **Performance Optimized:** Designed to work efficiently with large datasets in BigQuery by translating operations into SQL.
#' }
#'
#' \strong{Note:} Ensure that the \code{date_col} contains valid date values to prevent errors during the transformation process. The function automatically filters out \code{NULL} dates.
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#' library(dbplyr)
#' library(bigrquery)
#' library(DBI)
#'
#' # Establish a connection to BigQuery
#' project_id <- "nih-nci-dceg-connect-stg-5519"
#' dataset_id <- "FlatConnect"
#' con <- dbConnect(
#'   bigrquery::bigquery(),
#'   project = project_id,
#'   dataset = dataset_id,
#'   use_legacy_sql = FALSE
#' )
#'
#' # Reference your BigQuery table
#' your_table <- tbl(con, "FlatConnect.participants_JP")
#'
#' # Aggregate counts by week
#' weekly_counts <- count_by_period(
#'   tbl = your_table,
#'   date_col = d_471593703,
#'   period = "week",
#'   date_type = "string"
#' )
#' weekly_counts_local <- weekly_counts %>% collect()
#' print(weekly_counts_local)
#'
#' # Aggregate counts by month
#' monthly_counts <- count_by_period(
#'   tbl = your_table,
#'   date_col = d_471593703,
#'   period = "month",
#'   date_type = "string"
#' )
#' monthly_counts_local <- monthly_counts %>% collect()
#' print(monthly_counts_local)
#'
#' # Aggregate counts by quarter
#' quarterly_counts <- count_by_period(
#'   tbl = your_table,
#'   date_col = d_471593703,
#'   period = "quarter",
#'   date_type = "string"
#' )
#' quarterly_counts_local <- quarterly_counts %>% collect()
#' print(quarterly_counts_local)
#'
#' # Aggregate counts by week starting on Sunday
#' weekly_sunday_counts <- count_by_period(
#'   tbl = your_table,
#'   date_col = d_471593703,
#'   period = "week",
#'   week_start = "SUNDAY",
#'   date_type = "string"
#' )
#' weekly_sunday_counts_local <- weekly_sunday_counts %>% collect()
#' print(weekly_sunday_counts_local)
#' }
#'
#' @export
count_by_period <- function(tbl, date_col, period = "week", week_start = "MONDAY", date_type = "string") {
  # Validate 'period' argument
  valid_periods <- c("week", "month", "quarter")
  period <- tolower(period)
  if (!(period %in% valid_periods)) {
    stop("`period` must be one of 'week', 'month', or 'quarter'.")
  }

  # Validate 'week_start' if period is 'week'
  if (period == "week") {
    valid_starts <- c("MONDAY", "SUNDAY")
    week_start <- toupper(week_start)
    if (!(week_start %in% valid_starts)) {
      stop("`week_start` must be either 'MONDAY' or 'SUNDAY'.")
    }
  }

  # Validate 'date_type' argument
  valid_types <- c("string", "timestamp")
  date_type <- tolower(date_type)
  if (!(date_type %in% valid_types)) {
    stop("`date_type` must be either 'string' or 'timestamp'.")
  }

  # Capture 'date_col' using tidy evaluation
  date_col_sym <- ensym(date_col)
  date_col_name <- as_string(date_col_sym)

  # Begin transformation: filter out NULL dates and select only the date column
  tbl_transformed <- tbl %>%
    filter(!is.na(!!date_col_sym)) %>%
    select(!!date_col_sym)

  # Conditionally cast 'date_col' to TIMESTAMP if it's a string
  if (date_type == "string") {
    # Ensure column name is correctly referenced without adding extra quotes
    cast_expr <- paste0("CAST(`", date_col_name, "` AS TIMESTAMP)")
  } else {
    # Directly reference the TIMESTAMP column without casting
    cast_expr <- paste0("`", date_col_name, "`")
  }

  # Construct the truncation expression based on 'period'
  trunc_expr <- switch(
    period,
    "week" = paste0("TIMESTAMP_TRUNC(", cast_expr, ", WEEK(", week_start, "))"),
    "month" = paste0("TIMESTAMP_TRUNC(", cast_expr, ", MONTH)"),
    "quarter" = paste0("TIMESTAMP_TRUNC(", cast_expr, ", QUARTER)")
  )

  # Inject the truncation expression as raw SQL using dbplyr::sql()
  trunc_expr_sql <- dbplyr::sql(trunc_expr)

  # Apply truncation and aggregate
  tbl_aggregated <- tbl_transformed %>%
    mutate(
      period_start = trunc_expr_sql
    ) %>%
    group_by(period_start) %>%
    summarise(
      num_records = n(),
      .groups = "drop"
    ) %>%
    arrange(period_start)

  return(tbl_aggregated)
}
