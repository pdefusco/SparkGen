# Data Lake

Spark load jobs
These are split into:

- raw - load from Parquet file
- optimise - any intermediate transformations required
- mart - presentation layer for the Data Lake Power BI reports

# Tests
There are python unittests tests are located in folder './test'.
These aim to run through some of the data transformation pipeline logic.


# CDE cli configuration 
Config is written to the file `cli/config/dev/config.yaml` or overridden as an environment variable.
## Update the configured vcluster endpoint
property `vcluster-endpoint` 

To get the vcluster-endpoint for your Cloudera Data Engineering cluster. Navigate to: 
 - the 'Cloudera Data Engineering Overview' page 
 - (Icon) Cluster details 
 - Copy the value in `JOBS API URL`
 - Set `vcluster-endpoint: ${JOBS API URL}` in the file `cli/config/dev/config.yaml`

Or instructions under step 1 here: https://docs.cloudera.com/data-engineering/cloud/api-access/topics/cde-api-list-jobs.html


# CICD
Relies on pipeline variables to be defined (here: https://gitlab.apps.transpower.co.nz/data-lake/spark-load/-/settings/ci_cd)

## PROXY SETTINGS
 - https_proxy: http://$CI_SERVER_HOST:3128
## AWS VARIABLES
 - AWS_ACCESS_KEY_ID: $(access key for AWS IAM user: tpdev-gitlab_cicd)
 - AWS_SECRET_ACCESS_KEY: $(secret access key for AWS IAM user: tpdev-gitlab_cicd)
 - AWS_DEFAULT_REGION: ap-southeast-2
 ## Cloudera Data Engineering Credentials 
 CDE CLI - https://docs.cloudera.com/data-engineering/cloud/cli-access/topics/cde-cli-authentication.html#pnavId2
 
 Generate access key id/secret from the User Management page in cloudera: https://console.cdp.cloudera.com/iam/index.html#/users (3 dots next to user > Generate Access Key)
 
 - CDE_ACCESS_KEY_ID
 - CDE_ACCESS_KEY_SECRET


# DEBUG statements for airflow scheduler
VC_ID=9qtsmv4v
kubectl --kubeconfig=./kubeconfig-cde  set env deployment/dex-app-${VC_ID}-airflow-scheduler -n dex-app-${VC_ID} AIRFLOW__CORE__LOGGING_LEVEL-
kubectl --kubeconfig=./kubeconfig-cde  set env deployment/dex-app-${VC_ID}-airflow-scheduler -n dex-app-${VC_ID} AIRFLOW__CORE__LOGGING_LEVEL=DEBUG



# Deploying
#TODO
 - Gitlab pipelines
 - Manual update of the deployment.yaml
 - Can't have different versions in prod and dev clusters an both gitlab stages for deploying to each use git branch 'master' and file 'deployment.yaml' to get version to deploy
 

 # Development Environment Setup
 - Make sure `Python`(id: ms-python.python) extension is installed in VSCode. 
 - Create python3 virtual environment: run  ```python3 -m venv .venv```
 - Activate virtual environment: run ```. .venv/bin/activate```
 - Install pyspark module to run tests: run ```pip install pyspark===2.4.7```
 - Install below modules to run jupyter notebooks:  
   ```pip install ipython```

   ```pip install jupyter notebook -U```

   ```pip install ipykernel -U  --force-reinstall```
 


# TODO ideas
TODO:
 
* Date base query for 4/30 days of data for one circuit
    * to display MWC - mart.analog_cct_0
    * Response times 


Example queries
 * selects from mart tables
    * x axis - timeseries
    * y axis - filtering/grouping by different columns
        * circuit

# Monitoring
  Kinesis monitors - tune to data (sizes) coming through


# NEW TODO ideas

 [] CI/CD credentials for production environment (AWS and CDE creds)
 [] Make the alerts on Kinesis landing data better
