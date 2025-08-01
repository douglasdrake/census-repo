# Download Census files, process and save results -------------------------

library(curl)
library(fs)

# Load the files to process each decade
source("process1970.R")
source("process1980.R")
source("process1990.R")
source("process2000.R")
source("process2010.R")
source("process2020.R")

options(dplyr.summarise.inform = FALSE)

# Source URLs -------------------------------------------------------------

get_source_urls <- function(decade) {
  stopifnot(decade %in% seq(1970, 2020, by = 10))

  if (decade == 1970) {
    source_urls <- c("https://www2.census.gov/programs-surveys/popest/tables/1900-1980/counties/asrh/co-asr-7079.csv")
  } else if (decade == 1980) {
    source_urls <- c("https://www2.census.gov/programs-surveys/popest/datasets/1980-1990/counties/asrh/pe-02.csv")
  } else if (decade == 1990) {
    base_90 <- "https://www2.census.gov/programs-surveys/popest/tables/1990-2000/intercensal/st-co/stch-icen"
    source_urls <- 1990:1999 %>%
      map_chr(\(x) (paste0(base_90, x, ".txt")))
  } else if (decade == 2000) {
    file_base_name <- "co-est00int-alldata-"
    file_base_url <-
      "ftp://ftp2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/"
    file_state_ids <- sort(c(
      as.vector(outer(0:4, 0:9, paste0)),
      as.vector(outer(5, 0:6, paste0))
    ))
    unused_state_ids <- c("00", "03", "07", "14", "43", "52")
    file_state_ids <- subset(file_state_ids, !(file_state_ids %in% unused_state_ids))
    file_extensions <- ".csv"
    ## Map would return a list of lists - use map_chr to get a vector of strings
    source_urls <- file_state_ids |>
      map_chr(\(x) paste0(file_base_url, file_base_name, x, file_extensions))
  } else if (decade == 2010) {
    source_urls <- c("https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/asrh/CC-EST2020-ALLDATA6.csv")
  } else {
    source_urls <- c("https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/asrh/cc-est2024-alldata.csv")
  }
  return(source_urls)
}

# Create download / processed file directories -------------------------------

create_download_directories <- function() {
  # possibly add a path to allow creation outside of current working directory
  decades <- seq(1970, 2020, by = 10)

  src_directory <- decades %>%
    map_chr(\(x) (paste0("src/", x)))
  data_directory <- decades %>%
    map_chr(\(x) (paste0("data/", x)))

  if (!all(dir_exists(src_directory))) {
    message("creating src directories...", appendLF = FALSE)
    dir_create(src_directory)
    message("done")
  } else {
    message("src directories exist...")
  }

  if (!all(dir_exists(data_directory))) {
    message("creating data directories...", appendLF = FALSE)
    dir_create(data_directory)
    message("done")
  } else {
    message("data directories exist...")
  }
}

# Download by decade ------------------------------------------------------

download_decade <- function(decade, refresh = FALSE) {
  # if the directory does not exist - create it.
  if (!dir_exists(file.path("src", as.character(decade)))) {
    message("creating directory...")
    dir_create(file.path("src", as.character(decade)))
  }

  source_urls <- get_source_urls(decade)

  for (url in source_urls) {
    destination_path <- file.path("src", as.character(decade), basename(url))

    # do not download if it already exists and refresh = FALSE
    if (!file_exists(destination_path) || (refresh == TRUE)) {
      tryCatch(
        {
          curl_download(url, destination_path, mode = "w", quiet = FALSE)
          message(paste("downloaded...", basename(url)))
        },
        error = function(e) {
          stop(paste("failed to download...", basename(url), ":", e$message))
        }
      )
    } else {
      if (file_exists(destination_path)) {
        message(paste(basename(url), "...already exists"))
        message(paste("use refresh = TRUE to download again"))
      }
    }
  }
}

# Download all ------------------------------------------------------------

download_all <- function(refresh = FALSE) {
  download_decade(decade = 1970, refresh = refresh)
  download_decade(decade = 1980, refresh = refresh)
  download_decade(decade = 1990, refresh = refresh)
  download_decade(decade = 2000, refresh = refresh)
  download_decade(decade = 2010, refresh = refresh)
  download_decade(decade = 2020, refresh = refresh)
}

# Process files -----------------------------------------------------------

process_decade <- function(decade) {
  stopifnot(decade %in% seq(1970, 2020, by = 10))

  source_files <- basename(get_source_urls(decade))
  source_paths <- file.path("src", as.character(decade), source_files)

  # print(source_paths)

  destination_file <- paste0("states-asrpe", as.character(decade), ".csv.gz")
  destination_path <- file.path("data", as.character(decade), destination_file)

  if (!all(file_exists(source_paths))) {
    missing_files <- source_paths[!file_exists(source_paths)]
    stop(paste("missing source files", " : ", missing_files))
  }

  if (decade == 1970) {
    message("processing...", appendLF = FALSE)
    process_1970(source_paths, destination_path)
    message(paste0(" writing ", destination_path), appendLF = FALSE)
    message(" done")
  } else if (decade == 1980) {
    message("processing...", appendLF = FALSE)
    process_1980(source_paths, destination_path)
    message(paste0(" writing ", destination_path), appendLF = FALSE)
    message(" done")
  } else if (decade == 1990) {
    message("processing...", appendLF = FALSE)
    process_1990(source_paths, destination_path)
    message(paste0(" writing ", destination_path), appendLF = FALSE)
    message(" done")
  } else if (decade == 2000) {
    message("processing...", appendLF = FALSE)
    process_2000(source_paths, destination_path)
    message(paste0(" writing ", destination_path), appendLF = FALSE)
    message(" done")
  } else if (decade == 2010) {
    message("processing...", appendLF = FALSE)
    process_2010(source_paths, destination_path)
    message(paste0(" writing ", destination_path), appendLF = FALSE)
    message(" done")
  } else if (decade == 2020) {
    message("processing...", appendLF = FALSE)
    process_2020(source_paths, destination_path)
    message(paste0(" writing ", destination_path), appendLF = FALSE)
    message(" done")
  }
}

process_all <- function() {
  decades <- seq(1970, 2020, by = 10)

  for (decade in decades) {
    tryCatch(
      {
        process_decade(decade)
      },
      error = function(e) {
        stop(paste("failed to process...", decade, ":", e$message))
      }
    )
  }

  combine_decades()
}

# Combine decades to write the final product ------------------------------

combine_decades <- function() {
  # For now until all decades are working...
  # return()

  decades <- seq(1970, 2020, by = 10)
  source_files <- decades %>%
    map_chr(\(x) (paste0("states-asrpe", as.character(x), ".csv.gz")))

  source_paths <- file.path("data", as.character(decades), source_files)

  destination_file <- paste0("states-asrpe.csv.gz")
  destination_path <- file.path("data", destination_file)

  combinded_df <- source_paths %>%
    map(\(x) (read_csv(x, col_types = "iccccci"))) %>%
    list_rbind()

  write_csv(combinded_df, destination_path)
}

make_state_names <- function() {
  state_names <- tidycensus::fips_codes %>%
    as_tibble() %>%
    filter(state_code <= 56) %>%
    select(state_fips = state_code, state_name) %>%
    distinct() %>%
    arrange(state_fips)

  write_csv(state_names, "data/state_names.csv")
  invisible(state_names)
}

# Function to set up directories, download files and process --------------

create_census_repo <- function(refresh = FALSE) {
  create_download_directories()
  make_state_names()
  download_all(refresh)
  process_all()
}
