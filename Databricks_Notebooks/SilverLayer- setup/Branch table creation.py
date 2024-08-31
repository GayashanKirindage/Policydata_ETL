# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC use schema silverlayer

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE OR REPLACE table silverlayer.Branch(
# MAGIC
# MAGIC    branch_id INT,
# MAGIC    branch_country string,
# MAGIC    branch_city string,
# MAGIC    merged_timestamp TIMESTAMP
# MAGIC  ) USING DELTA LOCATION '/mnt/silverlayer/Branch'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC  CREATE OR REPLACE TABLE Silverlayer.Claim (
# MAGIC
# MAGIC  claim_id integer,
# MAGIC  policy_id integer,
# MAGIC  date_of_claim DATE,
# MAGIC  claim_amount decimal(18,0),
# MAGIC  claim_status string,
# MAGIC  LastUpdatedTimeStamp timestamp,
# MAGIC  merged_timestamp timestamp
# MAGIC
# MAGIC  ) USING DELTA LOCATION '/mnt/silverlayer/Claim'
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC
# MAGIC  CREATE OR REPLACE table silverlayer.Customer (
# MAGIC  customer_id int,first_name string ,last_name string ,email string ,phone string ,country string,city string,registration_date timestamp, date_of_birth timestamp, gender string, merged_timestamp TIMESTAMP
# MAGIC  ) USING Delta location '/mnt/silverlayer/Customer'

# COMMAND ----------

# MAGIC  %sql
# MAGIC
# MAGIC  CREATE TABLE silverlayer.Policy (
# MAGIC
# MAGIC  policy_id integer,
# MAGIC  policy_type string,
# MAGIC  customer_id integer,
# MAGIC  start_date timestamp,
# MAGIC  end_date timestamp,
# MAGIC  premium double,
# MAGIC  coverage_amount double,
# MAGIC  merged_timestamp TIMESTAMP
# MAGIC  ) USING DELTA LOCATION '/mnt/silverlayer/Policy' 