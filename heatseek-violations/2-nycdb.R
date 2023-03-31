# install.packages(c("dotenv", "tidyverse", "DBI", "RPostgres", "fs"))

library(tidyverse)
library(DBI)
library(RPostgres)
library(dotenv)
library(fs)

# NYCDB credentials pulled from hidden file
load_dot_env("../.env")

con <- dbConnect(
  Postgres(),
  host= Sys.getenv("NYCDB_HOST"),
  port = Sys.getenv("NYCDB_PORT"),
  dbname = Sys.getenv("NYCDB_DBNAME"),
  user = Sys.getenv("NYCDB_USER"),
  password = Sys.getenv("NYCDB_PASSWORD")
)

# Input geocoded address (with BBL) from first script
addresses_geocoded <- read_rds(path("data", "addresses_geocoded.rds"))

# Load into the database as a temporary table (read-only HDC users can do this)
dbWriteTable(con, "heatseek_bbls", addresses_geocoded, temporary=TRUE)

# Join our addresses with HPD violations by bbl, keep only records where the
# inspection happened during this heat season and the violation is heat-related
# (law sections provided by Noelle), then aggregate by BBL getting the count of
# violations
nycdb_query <- "
  select
    hs.bbl,
    count(*)::numeric as tot_heat_viols
  from heatseek_bbls as hs
  left join hpd_violations as v using(bbl)
  where inspectiondate between '2022-10-01' and '2023-05-31' -- heat season
    and novdescription ~ '27-20(2[8-9]|3[0-3])' -- heat violations
  group by hs.bbl
"

# Run the query on the database and download the results into a dataframe
# (reformated for nicer printing)
violations <- dbGetQuery(con, nycdb_query) |> as_tibble()

# Since addresses without violations won't be included in the query result, we
# need to join back to the full list of addresses and fill in 0s
heatseek_violations <- addresses_geocoded |> 
  left_join(violations, by = "bbl") |> 
  mutate(tot_heat_viols = coalesce(tot_heat_viols, 0)) |> 
  # add some helpful links
  mutate(
    wow_link = str_glue("https://whoownswhat.justfix.org/bbl/{bbl}"),
    dap_link = str_glue("https://portal.displacementalert.org/property/{bbl}")
  )

# Export the data - we're done!
write_csv(heatseek_violations, path("data", "heatseek_violations.csv"))

dbDisconnect(con)
