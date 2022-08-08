from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime as dt, timedelta as td
import time
import pytz
from airflow.models import Param
import dateutil.parser


default_args = {
    "owner": "airflow",
    'params': {
        "initial_dates": "",
        "final_dates": ""
    }
}


#dataInicial = timedelta((datetime.now).replace(minutes=0, seconds=0, milisseconds=0), days=-1)
#dataFinal = (datetime.now).replace(minutes=0, seconds=0, milisseconds=0)
now = dt.now(pytz.timezone("America/Sao_paulo")) 
dataInicial = now.replace(hour=0, minute=0, second=0, microsecond=0)
dataFinal = now.replace(hour=0, minute=0, second=0, microsecond=0)
arg_initial_date = ''
arg_final_date = ''




def preparedates(**kwargs):
    
    arg_initial_date = kwargs['params']['initial_dates']
    arg_final_date = kwargs['params']['final_dates']
    
    print('initial date: ' + arg_initial_date)
    print('final date: ' + arg_final_date)
        
    


def sleep_func(**kwargs):
    preparedates(**kwargs)
    print('Starting sleeping process')
    time.sleep(2)
    print('Sleeping process finished')

with DAG('dag_params_example_3', start_date=dt(2022,8,7), schedule_interval='0 0 * * *', catchup = False, default_args=default_args) as dag :
    
    #Examplo using Datarun in templates
    catch_show_param = BashOperator(
        task_id = 'dag_params_example',
        bash_command = 'echo  "Hello word - {{params["initial_date"]}}"'        
    )
    
    sleep_for_log = PythonOperator(
        task_id = 'sleep_for_log',
        python_callable = sleep_func
    )
    
sleep_for_log >> catch_show_param