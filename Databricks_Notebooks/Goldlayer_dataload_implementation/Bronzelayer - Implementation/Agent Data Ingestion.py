# Databricks notebook source
from pyspark.sql.functions import lit
schema = "agent_id integer, agent_name string, agent_email string,agent_phone string, branch_id integer, create_timestamp timestamp"
df = spark.read.parquet("/mnt/landing/AgentData/*.parquet")
#df.show()

df_with_flag = df.withColumn("merge_flag", lit(False)) #add extra column to flag the records when the record already loaded or not
                                                        # initially it will be assigned as false, only move to silverlayer it will change to true
#df_with_flag.show()
df_with_flag.write.option("path", "/mnt/bronzelayer/Agent").mode("append").saveAsTable("bronzelayer.Agent") #save data as delta table inside bronzelayer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronzelayer.agent

# COMMAND ----------

#now we need to move the agent data file in to processed folder
#dbutils.fs.mv("dbfs:/mnt/landing/AgentData", "dbfs:/mnt/processed/AgentData")

from datetime import datetime

# get the current time in mm-dd-yyyy format
current_time = datetime.now().strftime('%m-%d-%Y')

# print the current time
print(current_time)

new_folder  = "/mnt/processed/AgentData/"+current_time

dbutils.fs.mv("/mnt/landing/AgentData/", new_folder, True)


# COMMAND ----------

from datetime import datetime

# Get the current time in mm-dd-yyyy format
current_time = datetime.now().strftime('%m-%d-%Y')

# Print the current time
print(current_time)

# Define the new folder path
new_folder = "/mnt/processed/AgentData/" + current_time

# List all files in the source directory and move only the Parquet files
files = dbutils.fs.ls("/mnt/landing/AgentData/")

for file in files:
    if file.path.endswith(".parquet"):
        # Move the file to the new folder
        dbutils.fs.mv(file.path, new_folder + "/" + file.name, True)
