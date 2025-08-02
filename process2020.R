# Process 2020 files ---------------------------------------------------
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
#   0 = Total
#   1 = 0-4 years
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
#

# Required libraries, paths, globals --------------------------------------
suppressPackageStartupMessages({
  library(tidyverse)
  library(duckdb)
  library(duckplyr)
})
# Process the 2020 file --------------------------------------------------

process_2020 <- function(source_files, destination_file) {
  src_file <- source_files[1]
  col_types <- str_flatten(c(rep("c", 5), rep("i", 75)))

  age_names <- c(
    paste0(seq(0, 80, by = 5), rep("-", 17), seq(4, 84, by = 5)),
    "85+"
  )

  df2020 <- read_csv(src_file, col_types = col_types)
  state_names <- read_csv("data/state_names.csv", col_types = "cc")

  # Create state summary file with race_p_ethnicity -------------------------

  df2020 %>%
    filter((YEAR %in% c(2:6)) & (AGEGRP != 0)) %>%
    mutate(
      year = 2018 + YEAR, # this gives years from 2020 - 2024
      age = ordered(AGEGRP, levels = 1:18, labels = age_names),
      new_age = forcats::fct_collapse(age,
        "0-19" = c("0-4", "5-9", "10-14", "15-19"),
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

  states_2020 <- states_df %>%
    mutate(
      sex = str_to_title(sex),
      race_p_ethnicity = factor(rpe, levels = race_levels, labels = race_names),
      .keep = "unused"
    ) %>%
    rename(state_fips = STATE, age = new_age) %>%
    left_join(state_names, by = join_by(state_fips)) %>%
    relocate(year, state_fips, state_name, age, sex, race_p_ethnicity, population)

  # Create US summary file with race_p_ethnicity ----------------------------

  us_2020 <- states_2020 %>%
    group_by(year, age, sex, race_p_ethnicity) %>%
    summarize(population = sum(population)) %>%
    mutate(state_fips = "00", state_name = "United States") %>%
    relocate(year, state_fips, state_name, age, sex, race_p_ethnicity, population)

  combined_df <- rbind(states_2020, us_2020)
  write_csv(combined_df, destination_file)
}
