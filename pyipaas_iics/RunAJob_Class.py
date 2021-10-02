import requests,json, pandas as pd,datetime,sys
import unittest, time, re

###########################################################
######################Parms for Job########################

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
###########################################################

###########################################################

class IICS_Run_Job():
    def __init__(self, org, user, pwd, project_list, run_type, jobid=None,jobtype=None):
        self.org            = org
        self.user           = user
        self.pwd            = pwd
        self.project_list   = project_list
        self.run_type       = run_type
        self.jobid          = jobid
        self.jobtype        = jobtype


    def auth(self) -> dict:
        try:
            self.url            = f"https://{self.org}.informaticacloud.com/ma/api/v2/user/login"
            self.payload        = json.dumps({"@type": "login", "username": f"{self.user}", "password": f"{self.pwd}"})
            self.headers        = {'Content-Type': 'application/json', 'Accept': 'application/json'}
            self.response       = requests.request("POST", self.url, headers=self.headers, data=self.payload)
            self.json_resp      = self.response.json()
            self.SessionId      = self.json_resp['icSessionId']
            self.serverUrl      = self.json_resp['serverUrl']
            self.uuid           = self.json_resp['uuid']
            self.orgUuid        = self.json_resp['orgUuid']
            self.domain         = re.search("\//(.*?)\.", self.serverUrl).group(1) + "-ing." + self.org
            self.run_domain = re.search("\//(.*?)\.", self.serverUrl).group(1) +"."+ self.org
        except Exception as e:
            print(e)
        return {"token": self.SessionId, "URL": self.serverUrl, "orgid": self.orgUuid,"domain":self.domain,"run_domain":self.run_domain}

###################################################################
########################## excecute jobs ##########################
    @property
    def runingestion(self):
        self.runurl = f"https://{self.auth()['run_domain']}.informaticacloud.com/mftsaas/api/v1/job"
        self.runpayload = json.dumps({"taskId": f"{self.jobid}"})
        #"{\"taskId\": \"0JKaH9VwzxVelqrbCY9OoK\"\r\n}"#json.dumps({"taskId": f"{self.jobid}"})
        self.runheaders = {
            "accept": "application/json, text/plain",
            "content-type": "application/json",
            "IDS-SESSION-ID": f"{self.auth()['token']}"
        }
        self.response = requests.request("POST", url=self.runurl, headers=self.runheaders, data=self.runpayload)
        return self.response.json()

    @property
    def rundataintegration(self)->json:
        self.drunurl = f"https://{self.auth()['run_domain']}.informaticacloud.com/saas/api/v2/job"
        self.drunpayload =  json.dumps({"@type":"job","taskFederatedId":f"{self.jobid}","taskType":f"{self.run_type}"})
        self.drunheaders = {
            'content-type': 'application/json',
            "Accept"      : "application/json",
            "icSessionId" : self.auth()['token']
        }
        self.drunresponse = requests.request("POST", self.drunurl, headers=self.drunheaders, data=self.drunpayload)
        return self.drunresponse.json()

    @property
    def runtaskflow(self)->json:

        self.drunurl = f"https://{self.auth()['run_domain']}.informaticacloud.com/active-bpel/odata/repository/v4/OdataRepository/Execute(Id='{self.jobid}')?publish=true"
        self.drunpayload =  json.dumps({})
        self.drunheaders = {
            'content-type': 'application/json',
            "Accept"      : "application/json",
            "cookie": "USER_SESSION=" + self.auth()['token'] + ";" + "XSRF_TOKEN=DIS"
        }
        self.drunresponse = requests.request("GET", self.drunurl, headers=self.drunheaders, data=self.drunpayload)
        return self.drunresponse.json()



###################################################################
########################## monitor jobs ##########################

    @property
    def ingestionjobstatus(self):
        self.ingstatusurl       = f"https://{self.auth()['domain']}.informaticacloud.com/mijobmonitor/api/v1/MIJobs?$count=true&$orderby=deployTime%20desc&$skip=0&$top=1000"
        self.ingstatuspayload   = '{}'
        self.ingstatusheader    = {
                                      "authority": f"{self.auth()['domain']}.informaticacloud.com",
                                      "sec-ch-ua": "'Google Chrome';v='87', ' Not;A Brand';v='99', 'Chromium';v='87'",
                                      "accept": "application/json, text/plain",
                                      "sec-ch-ua-mobile": "?0",
                                      'content-type': 'application/json',
                                      "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
                                      "origin": f"https://{self.auth()['domain']}.informaticacloud.com",
                                      "sec-fetch-site": "same-site",
                                      "sec-fetch-mode": "cors",
                                      "sec-fetch-dest": "empty",
                                      "referer": "https://na1.dm-us.informaticacloud.com/cloudUI/products/monitor/main/ingestion-dashboard",
                                      "accept-language": "en-US,en;q=0.9",
                                      "xsrf_token": "DIS",
                                      "cookie": "USER_SESSION=" + self.auth()['token'] + ";" + "XSRF_TOKEN=DIS"
                                    }
        self.ingstatusresponse          = requests.request("GET", self.ingstatusurl, headers=self.ingstatusheader, data=self.ingstatuspayload)
        self.job_details                = json.loads(self.ingstatusresponse.content)
        df                              = pd.read_json(json.dumps(self.job_details['value'])).sort_values(by=['runId'])

        if self.jobid == None:
            df['processed_by']          = 'DIS'
            df['processed_date']        = datetime.datetime.now()
            df['process_status']        = 'exit'
            df['process_code']          = df['status'].apply(lambda x: '-1' if x == "failed" else '0')
            self.job_info               = df
            print("No job id provided \nall jobs in monitor will be displayed \n",self.job_info[['assetId','assetName','assetType','process_status','process_code']].groupby('assetId').tail(1))
            exit(-1)
        else:
            df['processed_by']          = 'DIS'
            df['processed_date']        = datetime.datetime.now()
            df['process_status']        = df['status'].apply(lambda x: 'in progress' if x != "failed" and x != "completed" else 'exit')
            df['process_code']          = df['status'].apply(lambda x: '-1' if x == "failed" else '0')
            job_filter                  = df['assetId'] == self.jobid
            self.job_info               = df[job_filter][['assetId','assetName','assetType','duration','processed_by','processed_date','process_status','process_code',]]

        return self.job_info.groupby('assetId').tail(1)

    @property
    def mi_monitor(self):
        self.runingestion()
        while True:
            #print('waiting for job to complete')
            time.sleep(2)
            status = self.ingestionjobstatus['process_status']
            status_code = self.ingestionjobstatus['process_code']
            run_details = self.ingestionjobstatus
            if status.item() == 'exit' and status_code.item() == '0':
                #print('job completed succesfully')
                #print(run_details)
                return 'completed'
            elif status.item() == 'exit' and status_code.item() == '-1':
                #print('job completed succesfully')
                #print(run_details)
                return 'failed'

    @property
    def di_job_status(self)->pd.DataFrame:
        self.status_url = f"https://{self.auth()['run_domain']}.informaticacloud.com/saas/api/v2/activity/activityLog"
        self.status_headers = {'Content-Type':'application/json','icSessionId': self.auth()['token'],'Accept':'application/json'}
        self.status_payload = json.dumps({})
        r = requests.request("GET",self.status_url, data=self.status_payload, headers=self.status_headers)
        return pd.json_normalize(r.json())#json.dumps(r.json(), indent=4)

    ########################## orchestrate jobs ##########################

    # informatica has different end points for almost every type of job #
    # the orchestrator reads the job type and assigns correct function  #
    #    to run that type of job via its corresponding rest api call    #


    @property
    def orchestrate(self):
        if 'DSS'        in self.run_type:
            return self.rundataintegration
            #'Data Integration'

        if 'TASKFLOW'   in self.run_type:
            return self.runtaskflow
            #'Data Integration'

        if 'DTEMPLATE'  in self.run_type:
            return self.rundataintegration
            #'Data Integration'

        if 'MTT'        in self.run_type:
            return self.rundataintegration
            #'Data Integration'

        if 'MI_TASK'    in self.run_type:
            return self.runingestion
            #return 'Mass Ingestion'

