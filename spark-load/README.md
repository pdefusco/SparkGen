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

Or instructions under step 1 here: https://docs.cloudera.com/data-engineering/cloud/api-access/topics/cde-api-list-jobs.htm
