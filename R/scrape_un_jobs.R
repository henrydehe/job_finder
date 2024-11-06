# ***********************************************
# Title       : Scraping UN Jobs.org
# Description : Contains a series of functions using rvest to scrape job data
#               from https://unjobs.org/
# ***********************************************



# Setup -------------------------------------------------------------------

library(rvest)
library(tidyverse)
library(nanoparquet)


# Functions for scraping latest jobs --------------------------------------

get_un_jobs_list <- function(){

  # FUN: scrapes all jobs on a single page from unjobs.org
  get_jobs_tbl <- function(url, pg_sleep = 0.05){
    Sys.sleep(pg_sleep)
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

  # https://unjobs.org/ has a limit of 40 pages you can consult
  maxpages <- 40
  # generate the urls for most recent posts
  urls_unjobs_un <- str_glue("https://unjobs.org/New/{1:maxpages}")
  urls_unjobs_non_un <- str_glue("https://unjobs.org/non-un/{1:maxpages}")

  unjobs_un <- urls_unjobs_un |>
    map(get_jobs_tbl) |>
    bind_rows() |>
    mutate(is_un = TRUE)

  unjobs_non_un <- urls_unjobs_non_un |>
    map(get_jobs_tbl) |>
    bind_rows() |>
    mutate(is_un = FALSE)

  unjobs <- unjobs_un |>
    bind_rows(unjobs_non_un)

}

get_un_jobs_full <- function(un_jobs_list){

  get_job_desc <- function(url, pg_sleep = 0.1) {

    Sys.sleep(pg_sleep)

    job_desc <- read_html(url) |>
      html_element(".fp-snippet") |>
      html_text2()

    country <- read_html(url) |>
      html_element("#cats") |>
      html_text2() |>
      str_extract("(?<=[Cc]ountry:).+") |>
      str_trim() |>
      countrycode::countrycode("country.name", "iso3c")

    location <- read_html(url) |>
      html_element("#cats") |>
      html_text2() |>
      str_extract("(?<=[Ff]ield [Ll]ocation:|[Cc]ity:).+") |>
      str_trim()

    tibble(country, location, job_desc)

  }

  unjobs |>
    mutate(descriptor = pmap(list(url = link), get_job_desc)) |>
    unnest(descriptor) |>
    mutate(date_collected = Sys.Date())

}

scrape_un_jobs <- function(){

  un_jobs_list <- get_un_jobs_list()

  already_scraped <- list.files("data/", full.names = T) |>
    map(read_parquet) |>
    bind_rows() |>
    select(id) |>
    deframe()

  un_jobs_list |>
    filter(!id %in% already_scraped) |>
    get_un_jobs_full()

}

# Workflow --------------------------------------------------------------------

if(!file.exists(str_glue("data/{Sys.Date()}.parquet"))) {
  scrape_un_jobs() |>
    {\(x) if (nrow(x) >= 1) write_parquet(x, str_glue("data/un_jobs/{Sys.Date()}.parquet"))}
}







