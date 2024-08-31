# Databricks notebook source
# MAGIC  %sql
# MAGIC
# MAGIC  create database silverlayer;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema silverlayer;

# COMMAND ----------

# MAGIC %sql
# MAGIC  create or replace table silverlayer.Agent(
# MAGIC agent_id integer,
# MAGIC  agent_name string, 
# MAGIC  agent_email string,
# MAGIC  agent_phone string, 
# MAGIC  branch_id integer, 
# MAGIC  create_timestamp timestamp,
# MAGIC  merged_timestamp TIMESTAMP
# MAGIC  ) USING DELTA LOCATION '/mnt/silverlayer/Agent'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silverlayer.Agent  
# MAGIC