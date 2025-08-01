# Process 1980 source file ------------------------------------------------

suppressPackageStartupMessages(library(tidyverse))

# src1980 <- "src/1980/co-asr-1980-1989.csv"

process_1980 <- function(source_files, destination_path) {
  age_names <- c(
    paste0(rep("age", 17), seq(0, 80, by = 5), rep("_", 17), seq(4, 84, by = 5)),
    "age85"
  )

  src1980 <- source_files[1]

  df1980 <- vroom::vroom(
    src1980,
    skip = 14,
    skip_empty_rows = TRUE,
    col_names = c("year", "fips", "race_sex", age_names),
    col_types = str_flatten(c("icc", rep("i", 18))),
    delim = ","
  )

  state_names <- read_csv("data/state_names.csv",
    col_names = c("state_fips", "state_name"),
    col_types = "cc"
  )

  states_df_1980 <- df1980 %>%
    mutate(
      fips = if_else(str_length(fips) < 5, paste0("0", fips), fips),
      sex = str_to_title(str_split_i(race_sex, " ", -1)),
      race_p_ethnicity = str_split_i(race_sex, " ", 1),
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

  us_df_1980 <- states_df_1980 %>%
    group_by(year, sex, race_p_ethnicity, age) %>%
    summarize(population = sum(population)) %>%
    mutate(state_fips = "00", state_name = "United States") %>%
    relocate(year, state_fips, state_name, age, sex, race_p_ethnicity, population)

  combined_df <- rbind(states_df_1980, us_df_1980)
  write_csv(combined_df, destination_path)
}

# write_csv(states_df_1980, "tests/states_1980_reduced_age.csv")
# write_csv(us_df_1980, "tests/us_1980_reduced_age.csv")
# read_csv("with-pandas/data/1980/us_1980.csv") -> us_py
# us_py %>% select(!id) -> us_py
# us_py %>% mutate(year = as.integer(year), age=ordered(age, levels=c("0-19", "20-34", "35-49", "50-64", "65+")), population = as.integer(population)) -> us_py
# all.equal(us_py, us_df_1980 %>% ungroup())
# states_py <- read_csv("with-pandas/data/1980/states_1980.csv", col_types="iicccci")
# states_py
# states_py %>% select(!id) -> states_py
# states_py %>% mutate(age = ordered(age, levels = c("0-19", "20-34", "35-49", "50-64", "65+"))) -> states_py
# states_df_1980 %>% ungroup() -> states_df_1980
# all.equal(states_df_1980, states_py)
# sum(states_df_1980$population - states_py$population)
# > all.equal(states_df_1980, states_py)
# [1] TRUE
# > sum(states_df_1980$population - states_py$population)
# [1] 0
