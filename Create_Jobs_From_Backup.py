# Databricks notebook source
dbutils.widgets.text("backupFileName", "", "Backup File name")
#/dbfs/tmp/dbfsservice2/jobbackup/jobsJson_2022_10_31_04_00_44_118374.json

# COMMAND ----------

filename = dbutils.widgets.get("backupFileName")

# COMMAND ----------

import json
import requests

# COMMAND ----------

#function to create job
def createJob(jsonString):
    try:
        data = json.dumps(jsonString)
        res = requests.post(f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/api/2.1/jobs/create"
                        , data = data, headers = {"Authorization" : "Bearer " +  sc.getLocalProperty("spark.databricks.token") 
                                                  , "Content-Type" : "application/json"})
        if res.status_code == 200:
            jobId = json.loads(res.text)['job_id']
            print(f"JobId is {jobId}")
            return jobId
        else :
            print(res.status_code)
            print(res.text)
            raise Exception (f"Failed to create new job from this json data {jsonString} in workspace {host} ====> failed with {res.status_code} and {res.text}")
    except Exception as e:
        raise Exception (e)

# COMMAND ----------

if filename != "":
    with open(filename) as data_file:    
        data = json.load(data_file)
    for i in data:
        print(i)
        print("---")
        createJob(i)
else:
    print(f"Please provide absolute path for the fileName")
        
