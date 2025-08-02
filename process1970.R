# Process the 1970 source files -------------------------------------------
#
# 1. read the downloaded files
# 2. process from 18 age groups to 5
# 3. create a race + ethnicity variable from race / origin data
# 4. aggregate to state and national level
# 5. use consistent variable names and ordering for later joins.
# 6. combine results of states and US (US fips code 00)
# 7. write to .csv.gz
#

suppressPackageStartupMessages(library(tidyverse))

process_1970 <- function(source_paths, destination_path) {
  age_names <- c(
    paste0(rep("age", 17), seq(0, 80, by = 5), rep("_", 17), seq(4, 84, by = 5)),
    "age85"
  )

  src1970 <- source_paths[1] # There is only one file.

  df1970 <- read_csv(src1970,
    col_names = c("year", "fips", "race_sex", age_names),
    col_types = str_flatten(c("ici", rep("i", 18)))
  )

  state_names <- read_csv("data/state_names.csv",
    col_names = c("state_fips", "state_name"),
    col_types = "cc"
  )

  states_df <- df1970 %>%
    mutate(
      sex = case_when(
        race_sex %in% c(1, 3, 5) ~ "Male",
        race_sex %in% c(2, 4, 6) ~ "Female",
      ),
      race_p_ethnicity = case_when(
        race_sex %in% c(1, 2) ~ "White",
        race_sex %in% c(3, 4) ~ "Black",
        race_sex %in% c(5, 6) ~ "Other"
      ),
      age0_19 = age0_4 + age5_9 + age10_14 + age15_19,
      age20_34 = age20_24 + age25_29 + age30_34,
      age35_49 = age35_39 + age40_44 + age45_49,
      age50_64 = age50_54 + age55_59 + age60_64,
      age65p = age65_69 + age70_74 + age75_79 + age80_84 + age85,
      .keep = "unused"
    ) %>%
    pivot_longer(
      cols = starts_with("age"),
      names_to = "age",
      names_prefix = "age",
      values_to = "population"
    ) %>%
    mutate(age = ordered(age,
      levels = c("0_19", "20_34", "35_49", "50_64", "65p"),
      labels = c("0-19", "20-34", "35-49", "50-64", "65+")
    )) %>%
    mutate(state_fips = substr(fips, 1, 2), .after = fips, .keep = "unused") %>%
    group_by(year, state_fips, sex, race_p_ethnicity, age) %>%
    summarize(population = sum(population)) %>%
    left_join(state_names, by = join_by(state_fips)) %>%
    relocate(year, state_fips, state_name, age, sex, race_p_ethnicity, population)

  us_df <- states_df %>%
    group_by(year, sex, race_p_ethnicity, age) %>%
    summarize(population = sum(population)) %>%
    mutate(state_fips = "00", state_name = "United States") %>%
    relocate(year, state_fips, state_name, age, sex, race_p_ethnicity, population)

  combined_df <- rbind(states_df, us_df)
  write_csv(combined_df, destination_path)
}
