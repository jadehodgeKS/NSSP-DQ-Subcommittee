
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
except_col_names <- as.data.frame(dbListFields(datamart, "KS_PR_Except"))
except_reason_col_names <- as.data.frame(dbListFields(datamart, "KS_PR_Except_Reason"))

# Pulling Site MFT -------------------------------------------------------
mft <- dbGetQuery(datamart, 
  "SELECT c_biosense_facility_id, facility_name
   FROM ks_mft
   ")

# Except Reasons -------------------------------------------------------------

except_reasons <- dbGetQuery(datamart, 
    "SELECT exceptions_reason_code, exceptions_reason
     FROM except_reasons
     ")

# Exceptions Message Count

all_exceptions <- dbGetQuery(datamart, 
    "SELECT c_biosense_facility_id, count(message_id) as msg_count
     FROM ks_st_except
     WHERE arrived_date_time > '2023-02-20'
     GROUP BY c_biosense_facility_id
     ")
    ## Join Name to Exceptions
        named_except <- left_join(all_exceptions, mft, 
                                  by = "c_biosense_facility_id"
                                  )

# Exceptions for One Facility

exceptions <- dbGetQuery(datamart, 
  "SELECT file_name, message_id, message_control_id, feed_name, sending_facility_id, treating_facility_id, 
          c_facility_id, c_biosense_facility_id, c_biosense_id, visit_id, trigger_event, facility_type_code, 
          facility_type_description, patient_class_code, c_visit_date_time, arrived_date_time, 
          admit_date_time, admit_reason_description, medical_record_number, c_chief_complaint,
          chief_complaint_text, diagnosis_code, diagnosis_description, 
          c_unique_patient_id, processing_id, patient_account_number, exceptions_id
   FROM ks_pr_except
   WHERE c_biosense_facility_id in (3893) and arrived_date_time > '2023-02-01'")

# All Exceptions

except <- dbGetQuery(datamart, 
    "SELECT file_name, message_id, message_control_id, feed_name, sending_facility_id, treating_facility_id, 
          c_facility_id, c_biosense_facility_id, c_biosense_id, visit_id, trigger_event,
          facility_type_code, facility_type_description, patient_class_code, 
          c_visit_date_time, arrived_date_time, 
          admit_date_time, admit_reason_description, medical_record_number, 
          c_chief_complaint, chief_complaint_text, c_unique_patient_id, processing_id, 
          patient_account_number
     FROM ks_st_except
     WHERE feed_name = 'KSAdvent' and arrived_date_time > '2023-03-01'
     ")

# Subsetting the records with NA BioSense Facility ID
except_na <- except[is.na(except$c_biosense_facility_id),]

# Merge with Exceptions Reasons

except_reason_msgs <- dbGetQuery(datamart, 
    "SELECT message_id, exceptions_reason_code, ks_exceptions_reason_id, exception_date
     FROM ks_st_except_reason
     WHERE exception_date > '2023-03-01'
     ")

msgs_reasons <- left_join(except, except_reason_msgs, 
                             by = "message_id")

except_code_reason <- left_join(msgs_reasons, except_reasons, 
                                    by = "exceptions_reason_code")


# Raw Exceptions Messages

msgs <- dbGetQuery(datamart, 
    "SELECT message
     FROM ks_st_raw
     WHERE feed_name = 'KSAdvent' and arrived_date_time > '2023-02-20'
     ")
write.table(msgs, file = "msgs.txt", quote = F, row.names=F, col.names=F)

msgs <- dbGetQuery(datamart,
    "select Message
    from ks_st_except left join ks_st_raw
      on [ks_st_except].Message_ID=[ks_st_raw].Message_ID
    where C_Visit_Date > '2023-01-01' and C_Biosense_Facility_ID='3858'")

msgs <- dbGetQuery(datamart,
    "select Message
    from ks_st_except left join ks_st_raw
      on [ks_st_except].Message_ID=[ks_st_raw].Message_ID
    where C_Visit_Date > '2023-02-20'")
write.table(msgs, file = "msgs.txt", quote = F, row.names=F, col.names=F)
