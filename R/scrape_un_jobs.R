# ***********************************************
# Title       : Scraping UN Jobs.org
# Description : Uses rvest to scrape job data from https://unjobs.org/
# ***********************************************



# Setup -------------------------------------------------------------------

library(rvest)
library(tidyverse)
library(nanoparquet)
library(gha)


## Helper Functions --------------------------------------------------------

# Combination of purrr funs so that the function runs slowly, on error tries
# again, then returns a value
reliably <- function(f,
                     delay = rate_delay(),
                     backoff = rate_backoff(),
                     otherwise = NULL,
                     quiet = FALSE) {
  f |>
    slowly(rate = delay) |>
    insistently(rate = backoff, quiet = quiet) |>
    possibly(otherwise = otherwise, quiet = FALSE)

}

warnings_to_gha <- function() {
  if (!is.null(names(warnings()))) {
    gha_warning(str_c(names(warnings()), collapse = "\n"))
  }
}


# Functions for scraping latest jobs --------------------------------------

get_un_jobs_list <- function(){

  # FUN: scrapes all jobs on a single page from unjobs.org
  get_jobs_tbl <- function(url){
    html_jobs <- read_html(url) |>
      html_elements(".job") |>
      discard(~is.na(html_attr(.x, "id")))

    id <- html_jobs |>
      html_attr("id")
    job_title <- html_jobs |>
      html_elements(".jtitle") |>
      html_text2()
    org <- html_jobs |>
      html_text2() |>
      str_extract("(?<=\\n).+")
    posting_date <- html_jobs |>
      html_element(".timeago") |>
      html_attr("datetime") |>
      ymd_hms()
    link <- html_jobs |>
      html_elements(".jtitle") |>
      html_attr("href")

    tibble(id, job_title, org, posting_date, link)
  }

  # Make the function reliable
  delay <- rate_delay(pause = 0.01)
  backoff <- rate_backoff(pause_base = 0.02,
                          pause_cap = 10,
                          pause_min = 0.01,
                          max_times = 5)

  get_jobs_tbl_reliably <- get_jobs_tbl |>
    reliably(delay = delay, backoff = backoff)

  # https://unjobs.org/ has a limit of 40 pages you can consult
  maxpages <- 40
  # generate the urls for most recent posts
  urls_unjobs_un <- str_glue("https://unjobs.org/New/{1:maxpages}")
  urls_unjobs_non_un <- str_glue("https://unjobs.org/non-un/{1:maxpages}")

  unjobs_un <- urls_unjobs_un |>
    map(get_jobs_tbl_reliably) |>
    bind_rows() |>
    mutate(is_un = TRUE)

  unjobs_non_un <- urls_unjobs_non_un |>
    map(get_jobs_tbl_reliably) |>
    bind_rows() |>
    mutate(is_un = FALSE)

  warnings_to_gha()

  unjobs <- unjobs_un |>
    bind_rows(unjobs_non_un)

}

get_un_jobs_full <- function(un_jobs_list){

  get_job_desc <- function(url) {

    job_desc <- read_html(url) |>
      html_element(".fp-snippet") |>
      html_text2()

    country <- read_html(url) |>
      html_element("#cats") |>
      html_text2() |>
      str_extract("(?<=[Cc]ountry:).+") |>
      str_trim() |>
      countrycode::countrycode("country.name", "iso3c", warn = F)

    location <- read_html(url) |>
      html_element("#cats") |>
      html_text2() |>
      str_extract("(?<=[Ff]ield [Ll]ocation:|[Cc]ity:).+") |>
      str_trim()

    tibble(country, location, job_desc)

  }

  delay <- rate_delay(pause = 0.01)
  backoff <- rate_backoff(pause_base = 0.02,
                          pause_cap = 10,
                          pause_min = 0.01,
                          max_times = 5)
  otherwise <- tibble(country = character(),
                      location = character(),
                      job_desc = character())

  get_job_desc_reliably <- get_job_desc |>
    reliably(delay = delay, backoff = backoff, otherwise = otherwise)

  un_jobs_full <- un_jobs_list |>
    mutate(descriptor = pmap(list(url = link), get_job_desc_reliably)) |>
    unnest(descriptor) |>
    mutate(date_collected = Sys.Date())

  warnings_to_gha()

  return(un_jobs_full)

}

scrape_un_jobs <- function(){

  un_jobs_list <- get_un_jobs_list()

  already_scraped <- list.files("data/un_jobs", full.names = T) |>
    map(read_parquet) |>
    bind_rows() |>
    select(id) |>
    deframe()

  new_jobs <- un_jobs_list |>
    filter(!id %in% already_scraped)

  if (nrow(new_jobs) >= 1) {

    gha_notice(str_glue("Scraping {nrow(new_jobs)} new jobs from unjobs.org on {Sys.Date()}"))
    new_jobs |>
      get_un_jobs_full()

  } else {
    gha_notice("No new jobs posted - skipping scrape_un_jobs process today")
    return(new_jobs)
  }

}

# Workflow --------------------------------------------------------------------


gha_notice(str_glue("Initiating scrape_un_jobs on {Sys.Date()}"))

if (!file.exists(str_glue("data/{Sys.Date()}.parquet"))) {
  scrape_un_jobs() |>
    {\(x) if (nrow(x) >= 1) write_parquet(x, str_glue("data/un_jobs/{Sys.Date()}.parquet"))}()
  gha_notice("Workflow Complete!")
} else {
  gha_warning("scrape_un_jobs already run today, delete data file to run again")
}



