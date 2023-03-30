import requests
import json
import time
import sys
import os
import datetime
import re

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.sdk import DbfsService

sourceWorkspaceHost = sys.argv[1]
sourceWorkspaceToken = sys.argv[2]
targetWorkspaceHost = sys.argv[3]
targetWorkspaceToken = sys.argv[4]

print("sourceWorkspaceHost : ", sourceWorkspaceHost)
print("sourceWorkspaceToken : ", sourceWorkspaceToken)
print("targetWorkspaceHost : ", targetWorkspaceHost)
print("targetWorkspaceToken : ", targetWorkspaceToken)


#function to get list of available jobs
def getJobList(host, token) -> list:
    jobList = []
    res = requests.get(f"{host}api/2.1/jobs/list"
                     , headers = {"Authorization" : "Bearer " + token })
    if res.status_code == 200:
        if json.loads(res.text).get('jobs') != None:
            json_data = json.loads(res.text)['jobs']
            for singleJob in json_data:
                jobName = singleJob['settings']['name']
                jobId = singleJob['job_id']
                jobList.append([jobId,jobName])
    else:
        print(res.status_code)
        print(res.text)
        raise Exception (f"Exception in read list of jobs at {host} workspace ==> failed with {res.status_code} and {res.text}")
    return jobList

#function to get job settings
def getJobSetting(host, token, jobId):
    #set data json
    job_id_json_string = {
        "job_id" : jobId
    }
    job_id_data = json.dumps(job_id_json_string)
    
    res = requests.get(f"{host}api/2.1/jobs/get"
                        , data = job_id_data, headers = {"Authorization" : "Bearer " + token
                                               , "Content-Type" : "application/json"})
    
    if res.status_code == 200:
        json_data_string = json.loads(res.text)['settings']
    else:
        print(res.status_code)
        print(res.text)
        raise Exception (f"Exception in getting job settings for Job ID {jobId} at {host} workspace ==> failed with {res.status_code} and {res.text}")
    return json_data_string

#function to filter out eligible jobs to migrate/promote
def filterEligibleJobs(host, token, jobList):
    eligibleJobList = []
    for i in jobList:
        jobId = i[0]
        jobName = i[1]
        jobEligible = getJobSetting(host, token, jobId).get('tags',{}).get('production.ready')
        if jobEligible == 'true':
            eligibleJobList.append([jobId,jobName])

    return eligibleJobList

#function to check if a job already exists in target workspace
def createOrResetJobInTarget(sourceWorkspaceHost, sourceWorkspaceToken, targetWorkspaceHost, targetWorkspaceToken, eligibleJobList):
    targetJobList = getJobList(targetWorkspaceHost, targetWorkspaceToken)
    if eligibleJobList and targetJobList:
        for i in eligibleJobList:
            eligibleJobId = i[0]
            eligibleJobName = i[1]
            for j in targetJobList:
                targetJobId = j[0]
                targetJobName = j[1]
                if eligibleJobName == targetJobName:
                    eligibleJobSetting = getJobSetting(sourceWorkspaceHost, sourceWorkspaceToken, eligibleJobId)
                    resetJob(targetWorkspaceHost, targetWorkspaceToken, targetJobId, eligibleJobSetting)
                else:
                    eligibleJobSetting = getJobSetting(sourceWorkspaceHost, sourceWorkspaceToken, eligibleJobId)
                    createJob(targetWorkspaceHost, targetWorkspaceToken, eligibleJobSetting)
    else:
        for i in eligibleJobList:
            eligibleJobId = i[0]
            eligibleJobName = i[1]
            eligibleJobSetting = getJobSetting(sourceWorkspaceHost, sourceWorkspaceToken, eligibleJobId)
            createJob(targetWorkspaceHost, targetWorkspaceToken, eligibleJobSetting)



#function to create job
def createJob(host, token, jsonString):
    try:
        data = json.dumps(jsonString)
        res = requests.post(f"{host}api/2.1/jobs/create"
                        , data = data, headers = {"Authorization" : "Bearer " + token 
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

#function to reset the existing job
def resetJob(host, token, jobId, jsonString):
    try:
        reset_json_string = {
            "job_id" : jobId,
            "new_settings" : jsonString
        }
        reset_data = json.dumps(reset_json_string)
        res = requests.post(f"{host}api/2.1/jobs/reset"
                      , data = reset_data, headers = {"Authorization" : "Bearer " + token
                                                      , "Content-Type" : "application/json"})
        if res.status_code == 200:
            print(f"Job reset for {jobId} is scuccess")
        else :
            print(res.status_code)
            print(res.text)
            raise Exception (f"Failed to reset job from this json data {jsonString} for {jobId} jobId ====> failed with {res.status_code} and {res.text}")
    except Exception as e:
        raise Exception (e)








jobList = getJobList(sourceWorkspaceHost, sourceWorkspaceToken)
if jobList:
    print(f"Total available job list is {jobList}")
    eligibleJobList = filterEligibleJobs(sourceWorkspaceHost, sourceWorkspaceToken, jobList)
    if len(eligibleJobList) == 0:
        print(f"No Jobs are available for promoting to next workspace")
    else:
        print(f"Total jobs eligible for promoting to next workspace are {eligibleJobList}")
        #call the function to create jobs in target workspace
        createOrResetJobInTarget(sourceWorkspaceHost, sourceWorkspaceToken, targetWorkspaceHost, targetWorkspaceToken, eligibleJobList)
        
        ###############################
        #Job backp in target workspace#
        ###############################
        
        #create timestamp for file
        ct = str(datetime.datetime.now()).replace(" ","_")
        ct = re.sub("[-:.]", "_", ct)
        print("current time:-", ct) 
        
        #make directory and file in local
        os.mkdir("jobbackup")
        pwd = os.getcwd()
        f = open(f'{pwd}/jobbackup/jobsJson_{ct}.json', "w")
        
        #get the list of jobs in target workspace and create json file
        targetJobList = getJobList(targetWorkspaceHost, targetWorkspaceToken)
        data = []
        for i in targetJobList:
            jobId = i[0]
            jobName = i[1]
            jobSetting = getJobSetting(targetWorkspaceHost, targetWorkspaceToken, jobId)
            data.append(jobSetting)
        with f as convert_file:
            convert_file.write(json.dumps(data))
        f.close
        
        #copy file from local directory to dbfs
        api_client = ApiClient(token = targetWorkspaceToken,host = targetWorkspaceHost)
        dbfs_service = DbfsService(api_client)
        dbfs_service.put(src_path = f'{pwd}/jobbackup/jobsJson_{ct}.json', path = f"dbfs:/tmp/dbfsservice2/jobbackup/jobsJson_{ct}.json", overwrite = True)
        
        #display filename
        print(os.listdir())
        print(os.listdir(f'{pwd}/jobbackup/'))
        
        
else:
    print(f"No Jobs are available for promoting to next workspace")    