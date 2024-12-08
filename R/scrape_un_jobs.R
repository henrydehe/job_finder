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
  backoff <- rate_backoff(max_times = 5)

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

  # FUN: gets all elements from a single job page on unjobs
  get_job_page <- function(url) {

    # FUN: gets description for job
    job_desc <- function(job_page){
      job_page |>
        html_element(".fp-snippet") |>
        html_text2()
    }
    job_desc <- job_desc |> possibly(otherwise = NA_character_)

    # FUN: gets country listed for job
    job_country <- function(job_page){
      job_page |>
        html_element("#cats") |>
        html_text2() |>
        str_extract("(?<=[Cc]ountry:).+") |>
        str_trim() |>
        countrycode::countrycode("country.name", "iso3c", warn = F)
    }
    job_country <- job_country |> possibly(otherwise = NA_character_)

    # FUN: gets the city/location listed
    job_location <- function(job_page){
      job_page |>
        html_element("#cats") |>
        html_text2() |>
        str_extract("(?<=[Ff]ield [Ll]ocation:|[Cc]ity:).+") |>
        str_trim()
    }
    job_location <- job_location |> possibly(otherwise = NA_character_)

    # FUN: gets the listed deadline
    job_deadline <- function(job_page){
      job_page |>
        html_elements(xpath = "/html/body/script") |>
        keep( ~ isTRUE(html_attr(., "type") == "text/javascript")) |>
        html_text2() |>
        keep( ~ str_detect(., "(?<=var e[:alnum:]{6}p[di] = )\\d+(?=;)")) |>
        pluck(1) |>
        str_extract_all("(?<=var e[:alnum:]{6}p[di] = )\\d+(?=;)") |>
        list_c() |>
        parse_number() |>
        sum() |>
        {\(x) x/1000}() |>
        as_datetime(tz = "UTC") |>
        {\(x) if_else(x - now() > dmonths(10), NA_Date_, x)}()
    }
    job_deadline <- job_deadline |> possibly(otherwise = NA_Date_)

    # FUN: gets listed keywords
    job_keywords <- function(job_page){
      job_page |>
        html_elements(".md-chip-hover a") |>
        html_text2() |>
        str_replace_all(";", ",") |>
        str_c(collapse = ";")
    }
    job_keywords <- job_keywords |> possibly(otherwise = NA_character_)


    job_page <- read_html(url)

    tibble("country" = job_country(job_page),
           "location" = job_location(job_page),
           "deadline" = job_deadline(job_page),
           "source_keywords" = job_keywords(job_page),
           "job_desc" = job_desc(job_page))

  }

  # Parameters for making the function reliable
  delay <- rate_delay(pause = 0.01)
  backoff <- rate_backoff(max_times = 5)
  # Note that given the blank table below unnest() will drop the row
  # To preserve errors change the below such that it produces a row.
  otherwise <- tibble(country = character(),
                      location = character(),
                      job_desc = character())

  get_job_page_reliably <- get_job_page |>
    reliably(delay = delay, backoff = backoff, otherwise = otherwise)

  un_jobs_full <- un_jobs_list |>
    mutate(descriptor = pmap(list(url = link), get_job_page_reliably)) |>
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



