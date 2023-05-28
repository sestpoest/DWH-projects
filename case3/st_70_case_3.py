from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import datetime

conn_pg_params = {
            'host': '10.4.49.51',
            'database': 'student70_melyokhin_dv',
            'user': 'airflow',
            'password': 'airflow',
        }

with DAG(
    'st70_case_3',
    default_args={
        'depends_on_past': False,
        #'email': ['developer@yandex.ru'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': datetime.timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='',
     schedule_interval = "@daily",
    start_date=datetime.datetime(2023, 5, 20),
    catchup=False,
    max_active_runs=1,
    tags=['case_3'],
) as dag:

    task_0_dummy = DummyOperator(
        task_id='task_0_dummy',
    )
    
    def connect(params_dic):
        # NOTE подключение к серверу
        conn = None
        try:
            conn = psycopg2.connect(**params_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            exit(1)
        return conn

    def create_tracking(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from oltp_src_system.create_tracking();')
        conn_pg.commit()
        conn_pg.close()

    task_1_create_tracking = PythonOperator(
        task_id='task_1_create_tracking',
        python_callable=create_tracking,
        op_kwargs={},
    )

    def update_existed_tracking(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from oltp_src_system.update_existed_tracking();')
        conn_pg.commit()
        conn_pg.close()

    task_2_update_existed_tracking = PythonOperator(
        task_id='task_2_update_existed_tracking',
        python_callable=update_existed_tracking,
        op_kwargs={},
    )

    def delete_existed_tracking(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from oltp_src_system.delete_existed_tracking();')
        conn_pg.commit()
        conn_pg.close()

    task_3_delete_existed_tracking = PythonOperator(
        task_id='task_3_delete_existed_tracking',
        python_callable=delete_existed_tracking,
        op_kwargs={},
    )

    def load_from_cdc_tracking_products_information(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from dwh_stage.load_from_cdc_tracking_products_information();')
        conn_pg.commit()
        conn_pg.close()

    task_4_load_from_cdc_tracking_products_information = PythonOperator(
        task_id='task_4_load_from_cdc_tracking_products_information',
        python_callable=load_from_cdc_tracking_products_information,
        op_kwargs={},
    )

    def load_from_tracking_products_information_src(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from dwh_ods.load_from_tracking_products_information_src();')
        conn_pg.commit()
        conn_pg.close()

    task_5_load_from_tracking_products_information_src = PythonOperator(
        task_id='task_5_load_from_tracking_products_information_src',
        python_callable=load_from_tracking_products_information_src,
        op_kwargs={},
    )

    def load_dim_date(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from dwh_ods.load_dim_date();')
        conn_pg.commit()
        conn_pg.close()

    task_6_load_dim_date = PythonOperator(
        task_id='task_6_load_dim_date',
        python_callable=load_dim_date,
        op_kwargs={},
    )

    def load_tracking_products_information_actual(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from report.load_tracking_products_information_actual();')
        conn_pg.commit()
        conn_pg.close()

    task_7_load_tracking_products_information_actual = PythonOperator(
        task_id='task_7_load_tracking_products_information_actual',
        python_callable=load_tracking_products_information_actual,
        op_kwargs={},
    )

    def load_tracking_products_information_outdated(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from report.load_tracking_products_information_outdated();')
        conn_pg.commit()
        conn_pg.close()

    task_8_load_tracking_products_information_outdated = PythonOperator(
        task_id='task_8_load_tracking_products_information_outdated',
        python_callable=load_tracking_products_information_outdated,
        op_kwargs={},
    )

    def load_tracking_products_information_out_of_stock(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from report.load_tracking_products_information_out_of_stock();')
        conn_pg.commit()
        conn_pg.close()

    task_9_load_tracking_products_information_out_of_stock = PythonOperator(
        task_id='task_9_load_tracking_products_information_out_of_stock',
        python_callable=load_tracking_products_information_out_of_stock,
        op_kwargs={},
    )

    def load_tracking_products_information_stopped(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from report.load_tracking_products_information_stopped();')
        conn_pg.commit()
        conn_pg.close()

    task_10_load_tracking_products_information_stopped = PythonOperator(
        task_id='task_10_load_tracking_products_information_stopped',
        python_callable=load_tracking_products_information_stopped,
        op_kwargs={},
    )

    def load_tracking_products_information_deleted(**kwargs):
        conn_pg = connect(conn_pg_params)
        conn_pg.cursor().execute('select * from report.load_tracking_products_information_deleted();')
        conn_pg.commit()
        conn_pg.close()

    task_11_load_tracking_products_information_deleted = PythonOperator(
        task_id='task_11_load_tracking_products_information_deleted',
        python_callable=load_tracking_products_information_deleted,
        op_kwargs={},
    )  
    task_0_dummy >> [task_1_create_tracking, task_2_update_existed_tracking] >> task_3_delete_existed_tracking >>\
    task_4_load_from_cdc_tracking_products_information >> task_5_load_from_tracking_products_information_src >>\
    task_6_load_dim_date >> task_7_load_tracking_products_information_actual >>\
    task_8_load_tracking_products_information_outdated >> task_9_load_tracking_products_information_out_of_stock >>\
    task_10_load_tracking_products_information_stopped >> task_11_load_tracking_products_information_deleted
