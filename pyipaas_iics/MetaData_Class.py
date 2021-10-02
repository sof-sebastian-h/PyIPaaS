import requests,json, pandas as pd,re


################ Pandas Options ####################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
####################################################

class IICS_Job_Metadata():

  def __init__(self,org,user,pwd,project_list,run_type=None,jobname=None):
    self.org            = org
    self.user           = user
    self.pwd            = pwd
    self.project_list   = project_list
    self.run_type       = run_type
    self.jobname        = jobname


  def auth(self)->dict:
    try:
      self.url        = f"https://{self.org}.informaticacloud.com/ma/api/v2/user/login"
      self.payload    = json.dumps({"@type":"login","username":f"{self.user}","password":f"{self.pwd}"})
      self.headers    = {'Content-Type': 'application/json','Accept': 'application/json'}
      self.response   = requests.request("POST", self.url, headers=self.headers, data = self.payload)
      self.json_resp  = self.response.json()
      self.SessionId  = self.json_resp['icSessionId']
      self.serverUrl  = self.json_resp['serverUrl']
      self.uuid       = self.json_resp['uuid']
      self.orgUuid    = self.json_resp['orgUuid']
    except Exception as e:
      print(e)
    return {"token":self.SessionId,"URL":self.serverUrl,"orgid":self.orgUuid}

  def project_directory_list(self)->pd.DataFrame:
    try:
      listofprojdfs = []
      for project in self.project_list:
        self.project_url          = f"{self.auth()['URL']}/public/core/v3/objects?q=location=='{project}'"
        self.project_payload      = '{}'
        self.project_headers      = {'content-type': 'application/json',"Accept": "application/json", "INFA-SESSION-ID":self.auth()['token']}
        self.project_response     = requests.request("GET", self.project_url, headers=self.project_headers, data = self.project_payload)
        self.project_json_resp    = self.project_response.json()['objects']
        self.project_dumps        = json.dumps(self.project_json_resp,indent=4)
        self.project_loads        = json.loads(self.project_dumps)
        self.listofpathids= [i['id'] for i in self.project_loads]
        self.listofpaths  = [i['path'] for i in self.project_loads]
      self.df =pd.DataFrame(self.project_loads)
      folder_filter = self.df['type'] == 'Folder'
      self.final_df =self.df[folder_filter][['path']]
    except Exception as e: print("Fetch folder paths failed \n exception message:",e)
    return self.final_df

  def job_id_list(self) :#-> pd.DataFrame:

      try:
        self.listofjobdfs = []
        for path in self.project_directory_list()['path']:
          self.job_url        = f"{self.auth()['URL']}/public/core/v3/objects?q=location=='{path}'"
          self.job_payload    = '{}'
          self.job_headers    = {'content-type': 'application/json', "Accept": "application/json","INFA-SESSION-ID": self.auth()['token']}
          self.job_response   = requests.request("GET", self.job_url, headers=self.job_headers,data=self.job_payload)
          self.job_json_resp  = self.job_response.json()
          self.job_dumps      = json.dumps(self.job_json_resp,indent=4)
          self.job_loads      = json.loads(self.job_dumps)
          self.listofjobids   = [{'id': i['id'], 'type': i['type'], 'path': i['path'], 'update_date': i['updateTime']} for i in self.job_loads['objects']]
          self.listofjobdfs.append(pd.DataFrame.from_dict(self.listofjobids))

        df = pd.concat(self.listofjobdfs)
        df['project'] = df['path'].apply(lambda x: x[:re.search("\/", x).span()[0]])
        df['path'] = df['path'].apply(lambda x: x[re.search("\/", x).span()[1]:])
        df['jobname'] = df['path'].apply(lambda x: x[re.search("\/", x).span()[1]:])
        df['path'] = df['path'].apply(lambda x: x[:re.search("\/", x).span()[1]])

        ##filters##
        if self.jobname != None:
          filtered_df = df
          jobname_filter =filtered_df['jobname'] == self.jobname
          filtered_df = filtered_df[jobname_filter]
        elif self.run_type!= None:
          filter_type = df.type.isin(self.run_type)
          filtered_df = df[filter_type]
        else:
          filtered_df = df

        if len(filtered_df)<1:
          print('Please check your parms as a succesfull call was made but no data was returned')
          exit(-1)
      except Exception as e:
        print("fecth job ids failed \n exception message:",e)
      return filtered_df




