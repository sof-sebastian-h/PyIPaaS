import sys
from MetaData_Class     import IICS_Job_Metadata
from RunAJob_Class      import IICS_Run_Job


##########################   Parms   #############################

# If you want to pass parameters into script via another program #
if len(sys.argv)    >1:
    project         = sys.argv[1]
    org             = sys.argv[2]
    user            = sys.argv[3]
    pwd             = int(sys.argv[4])

# If you want to hardcode values or read from a parameter file #
else :

    project         = "" # project folder you want to get data from
    org             = "" # org region of your url
    user            = "" # username of non sso user set up in informatica org
    pwd             = "" # pwd for user set up in informatica org
    asset_type      = [] # IE ['TASKFLOW','MI_TASK','DSS']


#######################################################

        ## Generate Metadata Dataframe ##
df = IICS_Job_Metadata(org=org,user=user,pwd=pwd,project_list=project).job_id_list()

## filter your dataframe if you want specific subset ##

'''
filter = df['path'] # add evaluation logic here
df =df[filter]
'''
#######################################################

##  If you want to execute jobs on selection above  ##

'''df['RUN_A_JOB'] = df.apply(lambda x : IICS_Run_Job(
                                                    org=org,
                                                    user=user,
                                                    pwd=pwd,
                                                    project_list=project,
                                                    run_type=x['type'],
                                                    jobid= x['id']).orchestrate,axis=1)'''
print(df)

