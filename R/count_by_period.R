#' Aggregate Records by Specified Time Period with Optional Grouping Variables
#'
#' Transforms a date column in a \code{dbplyr} table connected to BigQuery by truncating each date to the start of the specified period (week, month, or quarter) and aggregates the data by counting the number of records in each period, optionally grouping by additional variables.
#'
#' @param tbl A \code{dbplyr} table object connected to BigQuery. Typically obtained using \code{tbl(con, "table_name")}.
#' @param date_col A quoted or unquoted column name representing the date to be transformed and aggregated. Utilizes tidy evaluation for flexibility.
#' @param period A character string specifying the aggregation period. Acceptable values are \code{"week"}, \code{"month"}, and \code{"quarter"}. Defaults to \code{"week"}.
#' @param week_start A character string indicating the starting day of the week when \code{period = "week"}. Acceptable values are \code{"MONDAY"} and \code{"SUNDAY"}. Defaults to \code{"MONDAY"}. Ignored when \code{period} is \code{"month"} or \code{"quarter"}.
#' @param date_type A character string specifying the data type of the \code{date_col}. Acceptable values are \code{"string"} and \code{"timestamp"}. Defaults to \code{"string"}.
#' @param group_vars An optional character vector of column names to group by in addition to \code{period_start}. Defaults to \code{NULL}.
#'
#' @return A \code{dbplyr} table object containing aggregated data with columns:
#' \describe{
#'   \item{\code{period_start}}{The start date of the aggregation period.}
#'   \item{\code{num_records}}{The count of records within each aggregation group.}
#'   \item{Additional columns}{If \code{group_vars} are specified, the grouping variables are included as additional columns.}
#' }
#'
#' @details
#' The \code{count_by_period} function simplifies the process of aggregating time-series data by specified periods, with the flexibility to include additional grouping variables. It leverages the \code{dplyr} and \code{dbplyr} packages to translate R code into optimized SQL queries compatible with BigQuery.
#'
#' **Key Features:**
#' \itemize{
#'   \item **Flexible Aggregation:** Supports weekly, monthly, and quarterly aggregations.
#'   \item **Custom Week Start:** Allows specifying the week's starting day (Monday or Sunday) for weekly aggregations.
#'   \item **Optional Grouping Variables:** Enables grouping by additional variables in the dataset.
#'   \item **Tidy Evaluation:** Accepts both quoted and unquoted column names for ease of use.
#'   \item **Performance Optimized:** Designed to work efficiently with large datasets in BigQuery by translating operations into SQL.
#' }
#'
#' **Note:** Ensure that the \code{date_col} contains valid date values to prevent errors during the transformation process. The function automatically filters out \code{NULL} dates.
#'
#' @examples
#' \dontrun{
#' # Load necessary libraries
#' library(dplyr)
#' library(dbplyr)
#' library(bigrquery)
#' library(DBI)
#' library(rlang)
#'
#' # Establish a connection to BigQuery
#' project_id <- "your-project-id"
#' dataset_id <- "your_dataset"
#' con <- dbConnect(
#'   bigrquery::bigquery(),
#'   project = project_id,
#'   dataset = dataset_id,
#'   use_legacy_sql = FALSE
#' )
#'
#' # Reference your BigQuery table
#' your_table <- tbl(con, "your_table_name")
#'
#' # Suppose your date column is named 'd_471593703', and grouping column is 'd_827220437'
#'
#' # Example 1: Aggregate counts by week using the date column
#' weekly_counts <- count_by_period(
#'   tbl = your_table,
#'   date_col = "d_471593703",  # Pass the date column as a string
#'   period = "week",
#'   date_type = "string"
#' )
#' weekly_counts_local <- weekly_counts %>% collect()
#' print(weekly_counts_local)
#'
#' # Example 2: Aggregate counts by month, grouping by 'd_827220437' (e.g., site)
#' monthly_counts_by_site <- count_by_period(
#'   tbl = your_table,
#'   date_col = "d_471593703",
#'   period = "month",
#'   date_type = "string",
#'   group_vars = c("d_827220437")
#' )
#' monthly_counts_by_site_local <- monthly_counts_by_site %>% collect()
#' print(monthly_counts_by_site_local)
#'
#' # Example 3: Aggregate counts by week starting on Sunday, grouping by 'd_827220437' and 'd_564964481'
#' weekly_counts_by_site_region <- count_by_period(
#'   tbl = your_table,
#'   date_col = "d_471593703",
#'   period = "week",
#'   week_start = "SUNDAY",
#'   date_type = "string",
#'   group_vars = c("d_827220437", "d_564964481")
#' )
#' weekly_counts_by_site_region_local <- weekly_counts_by_site_region %>% collect()
#' print(weekly_counts_by_site_region_local)
#' }
#'
#' @export
count_by_period <- function(tbl, date_col, period = "week", week_start = "MONDAY",
                            date_type = "string", group_vars = NULL) {
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
  date_col_sym <- rlang::ensym(date_col)
  date_col_name <- rlang::as_string(date_col_sym)

  # Process 'group_vars'
  if (!is.null(group_vars)) {
    if (!is.character(group_vars)) {
      stop("`group_vars` must be a character vector of column names.")
    }
    group_vars_syms <- rlang::syms(group_vars)
  } else {
    group_vars_syms <- NULL
  }

  # Begin transformation: filter out NULL dates and select necessary columns
  tbl_transformed <- tbl %>%
    filter(!is.na(!!date_col_sym)) %>%
    select(!!date_col_sym, !!!group_vars_syms)

  # Conditionally cast 'date_col' to TIMESTAMP if it's a string
  if (date_type == "string") {
    cast_expr <- paste0("CAST(`", date_col_name, "` AS TIMESTAMP)")
  } else {
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
    )

  if (!is.null(group_vars_syms)) {
    tbl_aggregated <- tbl_aggregated %>%
      group_by(period_start, !!!group_vars_syms)
  } else {
    tbl_aggregated <- tbl_aggregated %>%
      group_by(period_start)
  }

  tbl_aggregated <- tbl_aggregated %>%
    summarise(
      num_records = n(),
      .groups = "drop"
    ) %>%
    arrange(period_start)

  return(tbl_aggregated)
}
