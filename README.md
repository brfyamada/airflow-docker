# Airflow configuring and running project


* First of all run the next commands in the project path:

```bash
mkdir -p ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

```bash
$ docker-compose up airflow-init
```

* After the database has been created, run docker compose:

```bash
$ docker-compose up -d
```

* access the airflow web:

```bash
http://localhost:8080
```

* configure variables: 
    * Admin > Variables 
    * set the following variables:
        * bucket_name_randomuser = 'Your aws bucker name'
        * compression_randomuser = snappy
        * filename_randomuser = randomuser.parquet
        * path_randomuser = ./data/randomuser/

---

# Fonte de estudo

* Runnig airflow in docker: https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html
* Canal youtube: Danilo Souza - Como extrair dados de uma API e salvar no Datalake com Airflow
    * url: https://www.youtube.com/watch?v=fLl_7S4_P94



