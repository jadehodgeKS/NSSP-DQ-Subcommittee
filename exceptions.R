
# Exploring data in the Exception tables

# Packages and Connections
library(dplyr)
library(DBI)
library(tidyverse)

library(odbc)
datamart <- dbConnect(odbc::odbc(), dsn = 'BioSense_Platform')

# Create list of accessible tables in Datamart -------------------------------------------
table_list <- as.data.frame(dbListTables(datamart))

# Create a df of Except Table Column Names --------------------------------------------------
except_col_names <- as.data.frame(dbListFields(datamart, "KS_PR_Except"))   # Use your state initials
except_reason_col_names <- as.data.frame(dbListFields(datamart, "KS_PR_Except_Reason")) # Use your state initials

# Pulling Site MFT -------------------------------------------------------
    # Use your state initials in FROM
mft <- dbGetQuery(datamart, 
  "SELECT c_biosense_facility_id, facility_name
   FROM ks_mft          
   ")

# Exceptions Message Count -------------------------------------------------------------
    # Use your state initials in FROM
    # Adjust arrived_date_time 
facility_except_msgs <- dbGetQuery(datamart, 
    "SELECT c_biosense_facility_id, count(message_id) as msg_count
     FROM ks_st_except
     WHERE arrived_date_time > '2023-01-01'
     GROUP BY c_biosense_facility_id
     ")
    # Join Name to Exceptions
        named_except <- left_join(facility_except_msgs, mft, 
                                  by = "c_biosense_facility_id"
                                  )

# Exceptions for One Facility ------------------------------------------------------------
    # Use your State initials in FROM
    # Adjust c_biosense_facility_id
    # Adjust arrived_date_time
one_facility <- dbGetQuery(datamart, 
    "SELECT file_name, message_id, message_control_id, feed_name, sending_facility_id, treating_facility_id, 
          c_facility_id, c_biosense_facility_id, c_biosense_id, visit_id, trigger_event, facility_type_code, 
          facility_type_description, patient_class_code, c_visit_date_time, arrived_date_time, 
          admit_date_time, admit_reason_description, medical_record_number, c_chief_complaint,
          chief_complaint_text, diagnosis_code, diagnosis_description, 
          c_unique_patient_id, processing_id, exceptions_id
   FROM ks_st_except
   WHERE c_biosense_facility_id in (3861) and arrived_date_time > '2023-01-01'")
        
# Viewing All Exceptions Records  ----------------------------------------------------------------
    # Use your State initials in FROM
    # Adjust arrived_date_time
all_except <- dbGetQuery(datamart, 
    "SELECT file_name, message_id, message_control_id, feed_name, sending_facility_id, treating_facility_id, 
          c_facility_id, c_biosense_facility_id, c_biosense_id, visit_id, trigger_event,
          facility_type_code, facility_type_description, patient_class_code, 
          c_visit_date_time, arrived_date_time, 
          admit_date_time, admit_reason_description, medical_record_number, 
          c_chief_complaint, chief_complaint_text, c_unique_patient_id, processing_id
     FROM ks_st_except
     WHERE arrived_date_time > '2023-01-01'
     ")        
      # Useful Code for searching all_except
          # Subsetting the records with NA BioSense Facility ID
          except_na <- all_except[is.na(all_except$c_biosense_facility_id),]
          # Pull all records with specific UUID
          except_sending_id <- all_except[all_except$sending_facility_id == "1689317190",]

# Joining exceptions records to except_reasons  --------------------------------------------------------------
    # 1) Pull records that have arrived in ks_st_except table
        # Use your State initials in FROM
        # Adjust arrived_date_time
join_except <- dbGetQuery(datamart, 
    "SELECT file_name, message_id, message_control_id, feed_name, sending_facility_id, treating_facility_id, 
          c_facility_id, c_biosense_facility_id, c_biosense_id, visit_id, trigger_event,
          facility_type_code, facility_type_description, patient_class_code, 
          c_visit_date_time, arrived_date_time, admit_date_time,  
          medical_record_number, c_unique_patient_id, 
          admit_reason_description, c_chief_complaint, chief_complaint_text, processing_id
     FROM ks_st_except
     WHERE arrived_date_time > '2023-01-01'
     ") 
          
    # 2) Pull records from ks_st_except_reason
        # Use your State initials in FROM
        # Adjust exception_date to be the same as arrived_date_time above
join_except_reason <- dbGetQuery(datamart, 
    "SELECT message_id, exceptions_reason_code, ks_exceptions_reason_id, exception_date
     FROM ks_st_except_reason
     WHERE exception_date > '2023-01-01'
     ")

    # 3) Adds except_reasons_msgs to the end of all_except using message_id
join_msgs_codes <- left_join(join_except, join_except_reason, 
                             by = "message_id")

    # 4) Pull Except Reasons
except_reasons <- dbGetQuery(datamart, 
    "SELECT exceptions_reason_code, exceptions_reason
     FROM except_reasons
     ")

    # 5) Adds the except_reasons to the end of msgs_codes using exceptions_reason_code
except_msgs_reasons <- left_join(join_msgs_codes, except_reasons, 
                                    by = "exceptions_reason_code")


# Raw Exceptions Messages ----------------------------------------------------------------------------------
    # short scripts to retrieve raw msgs from exceptions tables
    # be sure to change State initials, date, table, etc.
    # write.table creates a text file of raw msgs

msgs <- dbGetQuery(datamart, 
    "SELECT message
     FROM ks_st_raw
     WHERE arrived_date_time > '2023-04-10'
     ")
write.table(msgs, file = "msgs.txt", quote = F, row.names=F, col.names=F)

msgs <- dbGetQuery(datamart,
    "select Message
    from ks_pr_except left join ks_pr_raw
      on [ks_pr_except].Message_ID=[ks_pr_raw].Message_ID
    where C_Visit_Date > '2023-01-01' and C_Biosense_Facility_ID='3837'")

msgs <- dbGetQuery(datamart,
    "select Message
    from ks_st_except left join ks_st_raw
      on [ks_st_except].Message_ID=[ks_st_raw].Message_ID
    where C_Visit_Date > '2023-01-01' and C_Biosense_Facility_ID='3800'")
write.table(msgs, file = "msgs.txt", quote = F, row.names=F, col.names=F)
