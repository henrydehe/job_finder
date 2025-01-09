# ***********************************************
# Title       : 02 Data Enrichment
# Description : Extracts additional details on jobs where not extracted as such
# ***********************************************


# Setup -------------------------------------------------------------------

# Libraries:
library(nanoparquet)
library(tidyverse)

# Import Data -------------------------------------------------------------

read_data <- function(data_dirs) {
  map(data_dirs, ~ list.files(.,
                              pattern = ".parquet",
                              full.names = T,
                              recursive = T)) |>
    list_c() |>
    map(read_parquet) |>
    bind_rows()
}

data_dirs <- "data/un_jobs"
data <- read_data(data_dirs)

# Standardise Data --------------------------------------------------------

# Not necessary given we currently only have one data source.
# TODO: Once we have another job board

# Enrich Data -------------------------------------------------------------

# Enrichment step 1: get deadlines where they are not supplied


str_extract_deadline <- function(string, date_only = F) {

  # Create a comprehensive regex pattern for date extraction with expanded trigger words
  # and date formats
  regex <- str_c(
    # Case insensitive flag
    "(?i)",
    # Start of trigger words/phrases - using non-capturing groups (?:)
    "(?:",
    # Basic deadline variations
    "deadline(?:\\s+(?:date|time|for\\s+submission))?|",

    # Due date variations with more qualifiers
    "due(?:\\s+(?:date|time|before|by|no\\s+later\\s+than))?|",

    # Closing variations with expanded options
    "closing(?:\\s+(?:date|time|period|window))?|",

    # Submission related triggers with expanded options
    "submit(?:ted)?(?:\\s+(?:by|before|no\\s+later\\s+than|",
    "documents?\\s+(?:by|before|until))?)|",

    # Application related triggers with more variations
    "applications?(?:\\s+(?:close|closes|closing|",
    "must\\s+be\\s+(?:in|received|submitted))|\\s+deadline)|",

    # Materials and documentation triggers
    "materials?(?:\\s+(?:due|deadline|submission))?|",
    "documents?(?:\\s+(?:due|deadline|required\\s+by))?|",

    # Registration and enrollment triggers
    "registration(?:\\s+(?:deadline|closes|ends))?|",
    "enroll(?:ment)?(?:\\s+(?:deadline|period\\s+ends))?|",

    # Cutoff variations
    "cut[-\\s]off(?:\\s+(?:date|time|deadline))?|",

    # Entry related phrases
    "entries?(?:\\s+(?:close|deadline|must\\s+be\\s+received))?|",

    # Submission instructions
    "please\\s+submit(?:\\s+(?:by|before|no\\s+later\\s+than))?|",
    "(?:kindly|please)\\s+(?:submit|provide)(?:\\s+(?:by|before|prior\\s+to))?|",

    # Must be received variations
    "must\\s+(?:be\\s+)?(?:received|submitted?|completed)",
    "(?:\\s+(?:by|before|no\\s+later\\s+than))?|",

    # Response and return triggers
    "response(?:\\s+(?:required|needed|due))?(?:\\s+(?:by|before))?|",
    "return(?:\\s+(?:by|before|deadline))?|",

    # Validity and expiration triggers
    "valid(?:\\s+(?:until|through|to))?|",
    "expires?(?:\\s+(?:on|at|by))?|",
    "expiration(?:\\s+(?:date|time))?|",
    "valid(?:ity)?(?:\\s+(?:period|date))?\\s+ends?|",

    # Time-sensitive offer triggers
    "offer(?:\\s+(?:valid|expires|ends))?(?:\\s+(?:on|by|until))?|",
    "(?:early\\s+bird|special)\\s+(?:rate|pricing|offer)\\s+(?:ends|expires)|",

    # End date variations
    "end(?:ing|s)?(?:\\s+(?:date|time|on|by))?|",

    # Submission window triggers
    "submissions?(?:\\s+(?:accepted|open)(?:\\s+(?:until|through|before)))?|",

    # Final date variations
    "last(?:\\s+(?:date|day|chance))?(?:\\s+to\\s+(?:apply|submit|register))?|",
    "final(?:\\s+(?:deadline|date|day))?(?:\\s+(?:for|to)\\s+",
    "(?:submission|registration|apply))?",
    ")",

    # Optional separators with expanded variations
    "\\s*[:;\\-–—]?\\s*",

    # Optional prepositions and time indicators before the date
    "(?:(?:on|by|before|until|through|prior\\s+to|no\\s+later\\s+than)\\s+)?",

    # Start of date capture groups
    "(",  # First capture group (includes prepositions)
    "(",  # Second capture group (just the date)
    # Written dates with expanded month formats and flexible spacing
    "[A-Za-z]{3,}\\.?\\s*\\d{1,2}(?:st|nd|rd|th)?,?\\s*,?\\s*\\d{4}|",

    # Alternative order with expanded variations
    "\\d{1,2}(?:st|nd|rd|th)?\\s*[A-Za-z]{3,}\\.?,?\\s*,?\\s*\\d{4}|",

    # Numeric dates with more separator variations
    "\\d{1,2}[-/.\\s]\\d{1,2}[-/.\\s]\\d{2,4}|",

    # Year first format with more separator options
    "\\d{4}[-/.\\s]\\d{1,2}[-/.\\s]\\d{1,2}|",

    # Dates with times including more time format variations
    "[A-Za-z]{3,}\\.?\\s*\\d{1,2}(?:st|nd|rd|th)?,?\\s*,?\\s*\\d{4}\\s*",
    "(?:@|at|,)?\\s*\\d{1,2}(?:[:.]\\d{2})?\\s*",
    "(?:am|pm|AM|PM|EST|EDT|CST|CDT|MST|MDT|PST|PDT|GMT|UTC)?|",

    # ISO format dates with optional milliseconds and timezone
    "\\d{4}-\\d{2}-\\d{2}",
    "(?:T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{3})?(?:Z|[+-]\\d{2}:?\\d{2})?)?",
    ")",  # End of second capture group
    ")"    # End of first capture group
  )

  date_only <- if (date_only == T) {date_only <- 2} else {date_only <- NULL}
  str_extract(string, regex, group = date_only)
}



data_2 <- data |>
  # Next we nest given the str_extract_deadline is so slow we only want
  # to execute it on the data where deadlines are not already collected
  group_by(missing_deadline = is.na(deadline)) |>
  nest() |>
  mutate(data = map2(data, missing_deadline,
                     {\(x,y)
                       if (y) {mutate(x, deadline_extracted = str_extract_deadline(job_desc, T))}
                       else {x}}
  )) |>
  unnest(data) |>
  ungroup() |>
  select(-missing_deadline)






# Enrichment step 2:




# Consolidate Data --------------------------------------------------------





