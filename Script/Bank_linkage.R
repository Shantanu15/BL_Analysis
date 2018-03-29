library(dplyr)
library(RPostgreSQL)
library(stringr)
library(tidyr)

### Connect to the PostGre Server#### 

drv <- dbDriver("PostgreSQL")  
db <- "postgres"  
host_db <- "localhost"
db_port <- "5432"
db_user <- "postgres"
db_password <- '123456'

con <- dbConnect(drv, dbname=db, host=host_db, port=db_port, user=db_user, password=db_password)


dbExistsTable(con, "postgres")


##Extracting member loan data 
loan_to_member <- dbGetQuery(con, "select id, or_shg_member_code,loan_amount,nrlm_ts_mst_loan_purpose_id from nrlm_ts_txn_loan_to_member_11122017")

##Spreading the data 

#Adding an index column 

loan_to_member$s_no <- seq.int(nrow(loan_to_member))

#Putting the index as the first column 
loan_to_member <- select(loan_to_member,s_no,id,loan_amount,nrlm_ts_mst_loan_purpose_id)


#spreading the data 

loan_to_member_spread <- loan_to_member%>%
  spread(nrlm_ts_mst_loan_purpose_id,loan_amount)

#aggregating the data
loan_to_member_spread_agg <- loan_to_member_spread %>%
  group_by(id)%>%
  summarise_all(funs(sum), na.rm=TRUE)

#removing the index column
loan_to_member_spread_agg <- select(loan_to_member_spread_agg, - s_no)



##
shg_member_db <- dbGetQuery(con, "select shg_code,id from or_shg_member")
#####loan_to_member <- rename(loan_to_member, id = or_shg_member_code)

loan_to_member_merge <- left_join(loan_to_member_spread_agg,shg_member_db,by="id")
loan_to_member_merge <- filter(loan_to_member_merge,shg_code != "NA")


loan_master <- dbGetQuery(con, "select * from nrlm_ts_mst_loan_purpose")

BL_Merged_data <-
  read.csv(
    "E://Google Drive//R//Raw_Data//Output_Files//BL_NRLM_9.csv",
    header = TRUE,
    sep = ","
  )

BL_Merged_Edit <- select(BL_Merged_data,state_name,state_code,district_name,district_code,block_name,block_code,grampanchayat_name,village_name,entity_code,group_name,shg_code,bank_account_no,loan_acc_no.y,ifsc_code.y)

BL_Merged_tbdas <- left_join(BL_Merged_Edit,loan_to_member_merge, by = "shg_code")

BL_Merged_tbdas_clean <- filter(BL_Merged_tbdas,id != "NA")
