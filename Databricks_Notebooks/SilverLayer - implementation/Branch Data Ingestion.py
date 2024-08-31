# Databricks notebook source
# MAGIC  %md
# MAGIC  <b>Remove all where brnach_id not null
# MAGIC  
# MAGIC  <b>Remove all the leading and trailing spaces in Brnach Country and covert it into UPPER CASE

# COMMAND ----------

df = spark.sql("select b.branch_id,b.branch_city, upper(trim(b.branch_country)) as branch_country FROM bronzelayer.Branch b where branch_id is NOT NULL and merge_flag = FALSE")

display(df )

# COMMAND ----------

# MAGIC %md
# MAGIC  <b>Merge into Silver layer table

# COMMAND ----------

df.createOrReplaceTempView("clean_Branch")
spark.sql("MERGE INTO silverlayer.Branch AS T USING clean_branch AS S ON t.branch_id = s.branch_id when MATCHED THEN UPDATE SET t.branch_country = s.branch_country , t.branch_city = s.branch_city, t.merged_timestamp = current_timestamp()  when NOT MATCHED THEN insert (branch_id, branch_country,branch_city,merged_timestamp ) values (s.branch_id, s.branch_country , s.branch_city,current_timestamp())")




# COMMAND ----------

spark.sql("update bronzelayer.Branch set merge_flag = TRUE where merge_flag = FALSE")