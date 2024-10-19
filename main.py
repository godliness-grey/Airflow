# import necessary modules
from airflow import DAG
import airflow.utils.dates
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# used to access the download URL
from urllib import request

# used to access the airflow PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# instantiate DAG

with DAG(
    dag_id="core_sentiment",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["CDE"],
    # points to path run SQL scripts for PostgresOperator
    template_searchpath="/opt/airflow/dags/CDE_Airflow_Assignment/src/",
) as dag:

    # Extract Stage
    # download zip file from URL

    # a function to access the URL and output path for download
    def _download_data():
        url = "https://dumps.wikimedia.org/other/pageviews/2024/2024-10/pageviews-20241018-150000.gz"
        output_dir = "/opt/airflow/dags/CDE_Airflow_Assignment/src/pageviews.gz"
        request.urlretrieve(url,output_dir)

    # task to download file
    download_data = PythonOperator(
        task_id="download_data",
        python_callable=_download_data,
    )

    # task to unzip file
    unzip_data = BashOperator(
        task_id="unzip_data",
        # unzip with output file name as: wikipageviews.gz
        bash_command="gunzip -f /opt/airflow/dags/CDE_Airflow_Assignment/src/pageviews.gz",
    )

    # Transform Stage
    # fetch data/selected columns

    # a function to fetch the company(site) name and view counts; 
    # also, to write an INSERT INTO statement
    def _fetch_data(companies):
        # list of companies we seek their view counts
        companies = ["Google", "Facebook", "Apple", "Amazon", "Microsoft"]

        data = dict.fromkeys(companies, 0)

        with open(f"/opt/airflow/dags/CDE_Airflow_Assignment/src/pageviews", "r") as file:      
            #for row in file:
                #domain_code, page_title, view_counts, response_size = row.split(" ")
                #if page_title in companies:
                #if domain_code == "en" and page_title in companies:
                    #data[page_title] = view_counts

            for row in file:
                domain_code, page_title, view_counts, response_size = row.split(" ")
                
                # convert view_counts to integer for numerical manipulation
                view_counts = int(view_counts)

                # Check if page_title is in companies
                if page_title in companies:
                    # If page_title already exists in data, add the view_counts
                    if page_title in data:
                        data[page_title] += view_counts  # Aggregate counts
                    # otherwise, initialize view_counts
                    else:
                        data[page_title] = view_counts  # Initialize count
        print(data)

        # create sql insertion script

        with open(f"/opt/airflow/dags/CDE_Airflow_Assignment/src/insert_script.sql", "w") as script:
            for company_name, no_of_view_counts in data.items():
                script.write(
                    "INSERT INTO Core_sentiments VALUES ("
                    f"'{company_name}', {no_of_view_counts}"", NOW());\n"
                )
        
    # task to fetch data
    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=_fetch_data,
        op_args=["companies"],
    )

    # pip install apache-airflow-providers-postgres through terminal, 
    # in order to access PostgresOperator

    # Load Stage

    # task to connect to postgresDB, create table and load data
    load_data = PostgresOperator(
        task_id="load_data",
        # connection values are created in the Airflow UI
        postgres_conn_id="local_pg_connect",
        sql=["create_table.sql", "insert_script.sql"],
    )

    # task dependencies

    download_data >> unzip_data >> fetch_data >> load_data
