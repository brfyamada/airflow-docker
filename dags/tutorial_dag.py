from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests 
import json

def captura_conta_dados() :
    url = "http://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

def e_valida(ti) :
    qtd = ti.xcom_pull(task_ids = 'captura_conta_dados')
    if(qtd > 10) :
        return 'valido'
    return 'nvalido'

with DAG('tutorial_dag2', start_date=datetime(2022,7,10), schedule_interval='0 2 * * 1,2,3,4,5', catchup = False) as dag :
    
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable = captura_conta_dados
    )

    e_valida = BranchPythonOperator(
        task_id = 'e_valida',
        python_callable = e_valida
    )

    valido = BashOperator(
        task_id = 'valido',
        bash_command = "echo 'Quanridade OK'"
    )

    nvalido = BashOperator(
        task_id = 'nvalido',
        bash_command = "echo 'Quanridade não OK'"

    )
    
    captura_conta_dados >> e_valida >> [valido, nvalido]




