from airflow import DAG, utils
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime as dt, timedelta as td
import time
import pytz
from airflow.models import Param
import dateutil.parser
from datetime import datetime





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
        
default_args = {
    "owner": "airflow",
    'params': {
        "initial_dates": "",
        "final_dates": ""
    }
}    


def sleep_func(**kwargs):
    preparedates(**kwargs)
    print('Starting sleeping process')
    time.sleep(2)
    print('Sleeping process finished')

with DAG('dag_params_example_3', start_date=utils.dates.days_ago(1), schedule_interval='0 22 * * *', catchup = False, default_args=default_args) as dag :
    
    tis= "{{ ts }}"
    dis= "{{ ds }}"
    dis_nodash = "{{ ds_nodash }}"
    d_i_s = "{{ data_interval_start }}"
    d_i_e = "{{ data_interval_end }}" 
    
    print("ds = {}, ts = {}, ds_nodash = {}, d_i_s = {}, d_i_e = {}".format(dis, tis, dis_nodash, d_i_s, d_i_e))
    
    #Examplo using Datarun in templates
    catch_show_param = BashOperator(
        task_id = 'catch_show_param',
        bash_command = 'echo  "Hello word - {{ ts }} *** {{ ds }} *** {{ ds_nodash }} *** {{ data_interval_start }} *** {{ data_interval_end }} "'        
    )
    
    sleep_for_log = PythonOperator(
        task_id = 'sleep_for_log',
        python_callable = sleep_func
    )
    
sleep_for_log >> catch_show_param