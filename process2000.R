# Process 2000 files ---------------------------------------------------
#
# 1. read the downloaded files
# 2. process from 18 age groups to 5
# 3. create a race + ethnicity variable from race / origin data
# 4. aggregate to state and national level
# 5. use consistent variable names and ordering for later joins.
# 6. combine results of states and US (US fips code 00)
# 7. write to .csv.gz
#

# The key for Age group code is as follows:
#  99 = a total column
#   0 = <1 year
#   1 = 1-4 years
#   2 = 5-9 years
#   3 = 10-14 years
#   4 = 15-19 years
#   5 = 20-24 years
#   6 = 25-29 years
#   7 = 30-34 years
#   8 = 35-39 years
#   9 = 40-44 years
#   10 = 45-49 years
#   11 = 50-54 years
#   12 = 55-59 years
#   13 = 60-64 years
#   14 = 65-69 years
#   15 = 70-74 years
#   16 = 75-79 years
#   17 = 80-84 years
#   18 = 85 years and over

# Required libraries, paths, globals --------------------------------------
suppressPackageStartupMessages({
  library(tidyverse)
  library(duckdb)
  library(duckplyr)
})

# The files were already downloaded --
# base_2000 <- "../src/2000/"
# all_files <- dir("../src/2000")
# src_files <- all_files[grepl("co-est00int-alldata-\\d{2}.csv", all_files)]
# src_files <- paste0(base_2000, src_files)
#
process_2000 <- function(source_files, destination_path) {
  age_names <- c(
    "0", "1-4",
    paste0(seq(5, 80, by = 5), rep("-", 16), seq(9, 84, by = 5)),
    "85+"
  )
  col_types <- str_flatten(c(rep("c", 5), rep("i", 45)))

  # Process the 2000 files --------------------------------------------------

  df2000 <- source_files %>%
    map(\(y) (read_csv(y, col_types = col_types))) %>%
    list_rbind()

  state_names <- read_csv("data/state_names.csv")

  # Create state summary file with race_p_ethnicity -------------------------

  df2000 %>%
    filter((YEAR %in% c(2:11)) & (AGEGRP != 99)) %>%
    mutate(
      year = 1998 + YEAR, # this gives years from 2000 - 2009
      age = ordered(AGEGRP, levels = 0:18, labels = age_names),
      new_age = forcats::fct_collapse(age,
        "0-19" = c("0", "1-4", "5-9", "10-14", "15-19"),
        "20-34" = c("20-24", "25-29", "30-34"),
        "35-49" = c("35-39", "40-44", "45-49"),
        "50-64" = c("50-54", "55-59", "60-64"),
        "65+" = c("65-69", "70-74", "75-79", "80-84", "85+")
      )
    ) %>%
    select(
      year, STATE, COUNTY, age, new_age,
      NHWA_MALE, NHWA_FEMALE, NHBA_MALE, NHBA_FEMALE,
      NHIA_MALE, NHIA_FEMALE, NHAA_MALE, NHAA_FEMALE,
      NHNA_MALE, NHNA_FEMALE, NHTOM_MALE, NHTOM_FEMALE,
      H_MALE, H_FEMALE
    ) %>%
    pivot_longer(
      cols = starts_with(c("NH", "H")),
      names_sep = "_",
      names_to = c("rpe", "sex"),
      values_to = "population"
    ) %>%
    group_by(year, STATE, COUNTY, new_age, sex, rpe) %>%
    # eliminate fine age categories
    summarize(population = sum(population)) %>%
    ungroup() %>%
    group_by(year, STATE, new_age, sex, rpe) %>%
    # eliminate county level
    summarize(population = sum(population)) -> states_df

  race_levels <- c("H", "NHWA", "NHBA", "NHAA", "NHIA", "NHNA", "NHTOM")
  race_names <- c(
    "Hispanic", "NH_White", "NH_Black", "NH_Asian", "NH_AIAN",
    "NH_NHPI", "NH_TOM"
  )

  states_2000 <- states_df %>%
    mutate(
      sex = str_to_title(sex),
      race_p_ethnicity = factor(rpe, levels = race_levels, labels = race_names),
      .keep = "unused"
    ) %>%
    rename(state_fips = STATE, age = new_age) %>%
    left_join(state_names, by = join_by(state_fips)) %>%
    relocate(year, state_fips, state_name, age, sex, race_p_ethnicity, population)

  # Create US summary file with race_p_ethnicity ----------------------------

  us_2000 <- states_2000 %>%
    group_by(year, age, sex, race_p_ethnicity) %>%
    summarize(population = sum(population)) %>%
    mutate(state_fips = "00", state_name = "United States")

  combined_df <- rbind(states_2000, us_2000)
  write_csv(combined_df, destination_path)
}
