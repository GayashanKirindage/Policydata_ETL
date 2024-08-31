# Databricks notebook source
# MAGIC %md
# MAGIC <b> Sales By Policy Type and Month: </b>
# MAGIC This table would contain the total sales for each policy type and each month. It would be used to analyze the performance of different policy types over time.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>Claims By Policy Type and Status:</b>
# MAGIC  This table would contain the number and amount of claims by policy type and claim status. It would be used to monitor the claims process and identify any trends or issues.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vw_gold_claims_by_policy_type_and_state as
# MAGIC select p.policy_type,c.claim_status,count(*) as total_claims,sum(claim_amount) as total_claim_amount
# MAGIC from silverlayer.claim c inner join silverlayer.policy p on c.policy_id = p.policy_id
# MAGIC group by p.policy_type,c.claim_status
# MAGIC having p.policy_type is not null 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from vw_gold_claims_by_policy_type_and_state

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO goldlayer.claims_by_policy_type_and_status g USING vw_gold_claims_by_policy_type_and_state s 
# MAGIC ON g.policy_type = s.policy_type AND g.claim_status = s.claim_status
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET g.total_claims = s.total_claims, g.total_claim_amount = s.total_claim_amount, g.updated_timestamp = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (policy_type, claim_status, total_claims, total_claim_amount,updated_timestamp)
# MAGIC values (s.policy_type, s.claim_status, s.total_claims, s.total_claim_amount,current_timestamp()
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Analyze the claim data based on the policy type like AVG, MAX, MIN, Count of claim.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vw_gold_claims_analysis as
# MAGIC select p.policy_type, avg(c.claim_amount) as avg_claim_amount, max(c.claim_amount) as max_claim_amount,min(c.claim_amount) as min_claim_amount, Count(distinct c.claim_id) AS total_claims
# MAGIC from silverlayer.claim c inner join silverlayer.policy p on c.policy_id = p.policy_id 
# MAGIC group by p.policy_type
# MAGIC having p.policy_type is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO goldlayer.claims_analysis as g USING vw_gold_claims_analysis as s on g.policy_type = s.policy_type 
# MAGIC when matched then
# MAGIC update set g.avg_claim_amount = s.avg_claim_amount, g.max_claim_amount = s.max_claim_amount, g.min_claim_amount = s.min_claim_amount, g.total_claims = s.total_claims, g.updated_timestamp = current_timestamp()
# MAGIC when not matched then
# MAGIC insert (policy_type, avg_claim_amount, max_claim_amount, min_claim_amount, total_claims, updated_timestamp)
# MAGIC values (s.policy_type, s.avg_claim_amount, s.max_claim_amount, s.min_claim_amount, s.total_claims, current_timestamp())