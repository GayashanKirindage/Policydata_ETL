# Databricks notebook source
# MAGIC %md
# MAGIC <b> Sales By Policy Type and Month: </b>
# MAGIC This table would contain the total sales for each policy type and each month. It would be used to analyze the performance of different policy types over time.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table goldlayer.sales_by_policy_type_and_month(
# MAGIC   policy_type string,
# MAGIC   sale_month string,
# MAGIC   total_premium integer,
# MAGIC   updated_timestamp timestamp
# MAGIC )USING DELTA LOCATION '/mnt/goldlayer/sales_by_policy_type_and_month'

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Claims By Policy Type and Status:</b>
# MAGIC  This table would contain the number and amount of claims by policy type and claim status. It would be used to monitor the claims process and identify any trends or issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table goldlayer.claims_by_policy_type_and_status(
# MAGIC   policy_type string,
# MAGIC   claim_status string,
# MAGIC   total_claims integer,
# MAGIC   total_claim_amount integer,
# MAGIC   updated_timestamp timestamp
# MAGIC )USING DELTA LOCATION '/mnt/goldlayer/claims_by_policy_type_and_status'

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Analyze the claim data based on the policy type like AVG, MAX, MIN, Count of claim.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table goldlayer.claims_analysis(
# MAGIC     policy_type string,
# MAGIC     claim_status string,
# MAGIC     avg_claim_amount integer,
# MAGIC     max_claim_amount integer,
# MAGIC     min_claim_amount integer,
# MAGIC     total_claims integer,
# MAGIC     updated_timestamp timestamp
# MAGIC )USING DELTA LOCATION '/mnt/goldlayer/claims_analysis'