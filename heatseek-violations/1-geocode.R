# install.packages(c("dotenv", "tidyverse", "janitor", "remotes", "fs", "readxl"))
# remotes::install_github("austensen/geoclient")

library(tidyverse)
library(janitor)
library(geoclient)
library(dotenv)
library(readxl)
library(fs)

# Geoclient API credentials pulled from hidden file
load_dot_env("../.env")

# You can acquire your Geoclient API Key by first registering with the NYC's API
# Portal at https://api-portal.nyc.gov/, then adding a "subscription" to the
# Geoclient User API.
geoclient_api_key(Sys.getenv("GEOCLIENT_KEY"))

# We'll need the full address in a single field for best geocoding results
addresses_raw <- read_excel(path("raw-data", "Heat Seek Building Addresses 2022-23 (1).xlsx")) |> 
  clean_names() |> 
  mutate(address_full = str_c(address, ", ", zip_code)) |> 
  rename(
    address_raw = address,
    zip_code_raw = zip_code
  )

# Geocode all the results, input column gets preserved and renamed as "input_location"
geocode_results <- addresses_raw |> 
  geo_search_data(address_full)

# Check for geocoding failures (only 2!)
tabyl(geocode_results, no_results)

# Get the addresses and look up manually (I used Who Owns What)
geocode_results |> 
  filter(no_results) |> 
  select(input_location)

# Copy over the addresses and BBL from WOW
manual_geocode <- tribble(
  ~address_full, ~bbl_manual,
  "90-10 149th St, 11213", "4096780042",
  "1230 Ave Y Brooklyn, NY 11235, 11235", "3074330013"
)  

# Join everything back up, and remove duplicdes (there were known duplicates in raw data)
addresses_geocoded <- addresses_raw |> 
  left_join(geocode_results, by = c("address_full" = "input_location")) |> 
  left_join(manual_geocode, by = "address_full") |> 
  transmute(
    address_raw,
    zip_code_raw,
    bbl = coalesce(bbl, bbl_manual)
  ) |> 
  distinct(bbl, .keep_all = TRUE)

# Confirm all records have BBL
count(addresses_geocoded, is.na(bbl))

# Export for next step of getting violations
write_rds(addresses_geocoded, path("data", "addresses_geocoded.rds"))
