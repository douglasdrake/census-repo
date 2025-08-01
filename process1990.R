# Preprocess 1990 files ---------------------------------------------------
# The key for Age group code is as follows:
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
#
#   The key for the race-sex group is as follows:
#     1 = White male
#   2 = White female
#   3 = Black male
#   4 = Black female
#   5 = American Indian or Alaska Native male
#   6 = American Indian or Alaska Native female
#   7 = Asian or Pacific Islander male
#   8 = Asian or Pacific Islander female
#
#   The key for the Ethnic origin code is as follows:
#     1 = not Hispanic or Latino
#   2 = Hispanic or Latino

# Required libraries, paths, globals --------------------------------------
suppressPackageStartupMessages({
  library(tidyverse)
  library(duckdb)
  library(duckplyr)
})

# base_1990 <- "src/1990/stch-icen"

process_1990 <- function(source_files, destination_path) {
  age_names <- c(
    "0", "1-4",
    paste0(seq(5, 80, by = 5), rep("-", 16), seq(9, 84, by = 5)),
    "85+"
  )

  # Process the 1990 files --------------------------------------------------

  df1990 <- source_files %>%
    map(\(y) (
      read_table(
        file = y,
        col_names = c("year", "fips", "age", "race_sex", "origin", "population"),
        col_types = "iciiii"
    ))) %>%
    list_rbind()

  state_names <- read_csv("data/state_names.csv")

  df1990 %>%
    mutate(
      year = 1900 + year,
      fips = if_else(str_length(fips) < 5, paste0("0", fips), fips),
      sex = case_when(
        race_sex %in% c(2, 4, 6, 8) ~ "Female",
        race_sex %in% c(1, 3, 5, 7) ~ "Male"
      ),
      race = case_when(
        race_sex %in% c(1, 2) ~ "White",
        race_sex %in% c(3, 4) ~ "Black",
        race_sex %in% c(5, 6) ~ "AIAN",
        race_sex %in% c(7, 8) ~ "API"
      ),
      origin = if_else(origin == 1, "NH", "Hispanic"),
      age = ordered(age, levels = 0:18, labels = age_names),
      new_age = forcats::fct_collapse(age,
        "0-19" = c("0", "1-4", "5-9", "10-14", "15-19"),
        "20-34" = c("20-24", "25-29", "30-34"),
        "35-49" = c("35-39", "40-44", "45-49"),
        "50-64" = c("50-54", "55-59", "60-64"),
        "65+" = c("65-69", "70-74", "75-79", "80-84", "85+")
      ),
      .keep = "unused",
      .before = population,
    ) %>%
    group_by(year, fips, new_age, sex, race, origin) %>% # getting rid of old age
    summarize(population = sum(population)) %>%
    mutate(age = ordered(new_age,
      levels = c("0-19", "20-34", "35-49", "50-64", "65+"),
      labels = c("0-19", "20-34", "35-49", "50-64", "65+")
    )) %>%
    mutate(state_fips = substr(fips, 1, 2), .after = fips, .keep = "unused") %>%
    group_by(year, state_fips, age, sex, race, origin) %>% # take to the state level
    summarize(population = sum(population)) -> states_1990

  # Create state summary file with race_p_ethnicity -------------------------

  states_1990_rpe <- states_1990 %>%
    pivot_wider(
      names_from = c(origin, race), values_from = population,
      names_sep = "_"
    ) %>%
    mutate(
      H = Hispanic_AIAN + Hispanic_API + Hispanic_Black + Hispanic_White,
      .keep = "unused"
    ) %>%
    pivot_longer(!(year:sex),
      names_to = "race_p_ethnicity",
      values_to = "population"
    ) %>%
    left_join(state_names, by = join_by(state_fips)) %>%
    relocate(year, state_fips, state_name, age, sex, race_p_ethnicity, population)


  # Create US summary file with race_p_ethnicity ----------------------------

  us_df_1990 <- states_1990_rpe %>%
    group_by(year, age, sex, race_p_ethnicity) %>%
    summarize(population = sum(population)) %>%
    mutate(state_fips = "00", state_name = "United States") %>%
    relocate(year, state_fips, state_name, age, sex, race_p_ethnicity, population)

  combined_df <- rbind(states_1990_rpe, us_df_1990)
  write_csv(combined_df, destination_path)
}
# Save the work to compare with pandas results ----------------------------

# write_csv(states_df_1990, "tests/states_1990_asrpe.csv")
# write_csv(us_df_1900, "tests/us_1990_asrpe.csv")

# Read the state files back for R and pandas to compare -------------------

# states_py <- read_csv("../with-pandas/data/1990/states_rpe.csv", col_types = "iicccci")
# states_py <- states_py %>%
#   select(!id) %>%
#   mutate(
#     race_p_ethnicity = if_else(race_p_ethnicity == "Hispanic", "H", race_p_ethnicity),
#     age = ordered(age, levels = c("0-19", "20-34", "35-49", "50-64", "65+"))
#   ) %>%
#   arrange(year, state_fips, sex, age, race_p_ethnicity)
#
# states_r <- read_csv("states_1990_asrpe.csv", col_types = "icccci")
# states_r <- states_r %>%
#   mutate(
#     age = ordered(age, levels = c("0-19", "20-34", "35-49", "50-64", "65+"))
#   ) %>%
#   arrange(year, state_fips, sex, age, race_p_ethnicity)

# Comparing the two approaches --------------------------------------------

# > all.equal(states_py, states_r)
# [1] TRUE
# > sum(abs(states_r$population - states_py$population))
# [1] 0
