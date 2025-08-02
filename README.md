# Census Repo

## Overview
For reproducibility of data used in apps and dashbords, these functions
are provided.  `process-census-files.R` contains the main function: 
```r
create_census_repo <- function(refresh = FALSE) {
    # use refresh = TRUE to update existing download files.
    create_download_directories()
    make_state_names()
    download_all(refresh)
    process_all()
}
```
   1. Create a directory structure to store downloads and processed files.
   2. Download the files with curl, if the files do not exist.   If the 
   files exist, one can use `refresh = TRUE` to update the files.
   3. Process the data by decade since the formats and contents of the
      files differ by decade.
   4. Combine the 6 decade files to produce one for use later.
Editing the `processYEAR.R` files will allow one to change what results
are saved from the original files.

**Note** The URLs and formats of the files may change over time.   The files `processYEAR.R`
will need to be adjusted if this occurs.

The data are population estimates (1970–2024) 
produced by the U.S. Census Bureau’s [Population Estimates Division](https://www.census.gov/programs-surveys/popest/data/data-sets.html). 
The focus is on state and national data from 1970 to 2024.  

## Population Estimates

- [Postcensal Estimates](https://www.census.gov/programs-surveys/popest/guidance.html): 
Calculated after each decennial census using data on births, deaths, and migration.
- [Intercensal Estimates](https://www.census.gov/programs-surveys/popest/technical-documentation/research/intercensal-estimates.html): Adjust postcensal figures to align with the subsequent 
census count. The adjustment is distributed across the decade to match April 1 
census totals.

Population estimates reflect the population as of **July 1** each year, 
which may differ from April 1 census counts during census years.

## Data Sources

Data are pulled from publicly available datasets. The following URLs **download**
portions of the data used.

*  [1970-1979](https://www2.census.gov/programs-surveys/popest/tables/1900-1980/counties/asrh/co-asr-7079.csv)
*  [1980-1989](https://www2.census.gov/programs-surveys/popest/datasets/1980-1990/counties/asrh/pe-02.csv)
*  [1990-1999](https://www2.census.gov/programs-surveys/popest/tables/1990-2000/intercensal/st-co/stch-icen1990.txt)
*  [2000-2009, Vintage 2011](https://www2.census.gov/programs-surveys/popest/datasets/2000-2010/intercensal/county/co-est00int-alldata-01.csv)
*  [2010-2019, Vintage 2020](https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/asrh/CC-EST2020-ALLDATA6.csv)
*  [2020-2024, Vintage 2024]("https://www2.census.gov/programs-surveys/popest/datasets/2020-2024/counties/asrh/cc-est2024-alldata.csv")

The vintage corresponds to the last year available in the time series estimates.
File organization differs by year or state.

