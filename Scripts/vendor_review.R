
# Vendor Review

# Packages and Connection -------------------------------------------------
pacman::p_load(
  rio,          # for importing data
  here,         # for file paths
  janitor,      # for data cleaning
  lubridate,    # for working with dates
  flextable,    # for making pretty tables
  tidyverse,    # for data management
  skimr,        # for reviewing data
  gtsummary,    # for creating tables
  dplyr,        # just in case
  DBI,          # datamart database connection
  odbc         # datamart database connection
)

datamart <- dbConnect(odbc::odbc(), dsn = 'BioSense_Platform')  # connection to datamart


# Import Data -------------------------------------------------------------
# Pull MFT into mft_raw df. This includes all facility status.
    # Be sure to change FROM to your jurisdiction's MFT table
mft_raw <- dbGetQuery(datamart, 
                  "SELECT *
                  FROM ks_mft
                  ") %>% 
  clean_names()

# A list of vendors
# vendors <- c("athenahealth, Inc.", 
#              "Azalea Health", 
#              "CPSI (Computer Programs and Systems), Inc.", 
#              "Epic Systems Corporation", 
#              "Medical Information Technology, Inc. (MEDITECH)"
#             )


# Clean Data --------------------------------------------------------------
# Clean mft_raw. Keep Active and Onboarding status.
mft <- mft_raw %>%
  filter(facility_type == "Emergency Care",
         facility_status == "Active") %>% 
  select(facility_name,
         facility_status,
         vendor_name,
         feed_name,
         parent_organization,
         )


# Vendor Analysis ---------------------------------------------------------

# Count the number of facilities served by each vendor
vendor_count <- mft %>% 
  tabyl(vendor_name) %>% 
  mutate(percent = round(percent * 100))

# Print the facility names by vendor
vendor_list <- mft %>% 
  group_by(vendor_name) %>% 
  summarise(facility_name = paste(sort(unique(facility_name)),collapse=", "))


# Export Data -------------------------------------------------------------
export(vendor_count, "vendor_count.xlsx")
export(vendor_list, "vendor_list.xlsx")

  
