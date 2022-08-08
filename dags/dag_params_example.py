from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime as dt, timedelta as td
import time
import pytz


#dataInicial = timedelta((datetime.now).replace(minutes=0, seconds=0, milisseconds=0), days=-1)
#dataFinal = (datetime.now).replace(minutes=0, seconds=0, milisseconds=0)
now = dt.now(pytz.timezone("America/Sao_paulo")) 
dataInicial = now.replace(hour=0, minute=0, second=0, microsecond=0)
dataFinal = now.replace(hour=0, minute=0, second=0, microsecond=0)
arg_initial_date = ''
arg_final_date = ''


def preparedates(**kwargs):
    if(kwargs['dag_run'].conf['initialDate'] and kwargs['dag_run'].conf['finalDate']):
        arg_initial_date = kwargs['dag_run'].conf['initialDate']
        arg_final_date = kwargs['dag_run'].conf['finalDate']
        
        print('initial date: ' + arg_initial_date)
        print('final date: ' + arg_final_date)
        
    


def sleep_func(**kwargs):
    preparedates(**kwargs)
    print('Starting sleeping process')
    time.sleep(2)
    print('Sleeping process finished')

with DAG('dag_params_example', start_date=dt(2022,8,7), schedule_interval='0 0 * * *', catchup = False) as dag :
    
    #Examplo using Datarun in templates
    catch_show_param = BashOperator(
        task_id = 'dag_params_example',
        bash_command = 'echo  "Hello word - {{dag_run.conf["value"]}}"'        
    )
    
    sleep_for_log = PythonOperator(
        task_id = 'sleep_for_log',
        python_callable = sleep_func
    )
    
sleep_for_log >> catch_show_param