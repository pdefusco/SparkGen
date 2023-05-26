from datetime import timedelta

from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from dateutil import parser

from airflow import DAG
# from airflow.operators import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

default_args = {
    'owner': 'csso_enterpriseapplicationssup',
    # Review - if idempotent jobs not required
    'depends_on_past': False,
    # Review - when other/following on DAGs are dependent on this one - wait for all dependent DAGs complete before starting this run.
    'wait_for_downstream': False,
    'start_date': parser.isoparse('2021-12-01T00:00:00.000Z').replace(tzinfo=timezone.utc),
    'end_date': parser.isoparse('2039-12-30T00:00:00.000Z').replace(tzinfo=timezone.utc),
    'connection_id': 'cde_runtime_api',
    'user': 'notused',
    'email': ['bede.bignell@transpower.co.nz'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def send_email(**context):
    print("Send email has been called")
    
    # subject = "Job Failure"
    # body = f"""
    #     Hi, <br>
    #     Airflow job failure for load_analog_cct
         
    #     <br> Thank You. <br>
    # """

    # send_email(dag.default_args["email"], subject, body)


# PySpark job/file names
load_data="load_data"
reload_data="reload_data"
branch_constraint_constraintbranch="branch_constraint_constraintbranch"
case_dispatchhistory="case_dispatchhistory"
contingencyviol="contingencyviol"
analogcct="analog-cct"
planbranchnrsl_planbranchnrss_planconstraintnrsl_planconstraintnrss_planconstraintrtd="planbranch-nrsl_planbranch-nrss_planconstraint-nrsl_planconstraint-nrss_planconstraint-rtd"

dag = DAG(
    reload_data,
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False,
    # only run airflow script when externally triggered
    schedule_interval=None,
    on_failure_callback=send_email,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

case_dispatchhistory = CDEJobRunOperator(
    task_id=f"reload-data_{case_dispatchhistory}",
    job_name=f"reload-data_{case_dispatchhistory}",
    dag=dag,
)

planbranch_planconstraint = CDEJobRunOperator(
    task_id=f"reload-data_{planbranchnrsl_planbranchnrss_planconstraintnrsl_planconstraintnrss_planconstraintrtd}",
    job_name=f"reload-data_{planbranchnrsl_planbranchnrss_planconstraintnrsl_planconstraintnrss_planconstraintrtd}",
    dag=dag,
)

branch_constraint_constraintbranch = CDEJobRunOperator(
    task_id=f"reload-data_{branch_constraint_constraintbranch}",
    job_name=f"reload-data_{branch_constraint_constraintbranch}",
    dag=dag,
)

analog_cct = CDEJobRunOperator(
    task_id=f"reload-data_{analogcct}",
    job_name=f"reload-data_{analogcct}",
    dag=dag,
)

contingencyviol = CDEJobRunOperator(
    task_id=f"reload-data_{contingencyviol}",
    job_name=f"reload-data_{contingencyviol}",
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Order of how to run jobs roughly:
#                                                   - contingencyviol ->                        
#                                                 /                      \                       
#          - branch_constraint_constraintbranch ->                         -                     
#         /                                       \                      /   \                  
# start ->                                          -    analog_cct   ->      \   
#         \                                                                    -> end
#          \                                                                  /                  
#           -    case_dispatchhistory    ->    planbranch_planconstraint   ->                     
# 
#
start >> branch_constraint_constraintbranch
start >> case_dispatchhistory >> planbranch_planconstraint
branch_constraint_constraintbranch >> contingencyviol
branch_constraint_constraintbranch >> analog_cct
[contingencyviol, analog_cct, planbranch_planconstraint] >> end
