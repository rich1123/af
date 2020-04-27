from datetime import timedelta
# import boto3
import re
import pandas as pd
import psycopg2
import pdfkit
import sqlalchemy
from airflow import DAG
from airflow.example_dags.example_http_operator import default_args
# from airflow.operators.papermill_operator import PapermillOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
# import matplotlib
import matplotlib.pyplot as plt
import numpy as np

# import seaborn as sns

#
default_args = {
    'owner': 'Rich Maiale',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['richm1123@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
#
dag = DAG(
    'csv_request',
    default_args=default_args,
    description='Fantasy Football ADP DAG',
    schedule_interval=timedelta(days=1),
)


# def upload_file_to_S3(filename, key, bucket_name):
#     s3.Bucket(bucket_name).upload_file(filename, key)
#
#
# with DAG('S3_dag_test', default_args=default_args, schedule_interval='@once') as dag:
#     start_task = DummyOperator(
#         task_id='dummy_start'
#     )
#
#     upload_to_S3_task = PythonOperator(
#         task_id='upload_to_S3',
#         python_callable=upload_file_to_S3,
#         op_kwargs={
#             'filename': '/Users/Rich2/dev/airflow_home/dags',
#             'key': 'def_data.csv',
#             'bucket_name': 'rmfb.bucket',
#         },
#         dag=dag)
#
#
# s3 = boto3.resource('s3')

# Use arrows to set dependencies between tasks

# XCom.set(
#     key='f_name',
#     value='rb',
#     task_id=t1.task_id,
#     dag_id=t1.dag_id,
#     execution_date=PythonOperator)
# def name_data(name: str):
#     f_name = input(name)
#     return f_name

def df_update():
    """Creates/returns adp_temp df from adp.csv"""
    adp_temp = pd.read_csv('/Users/rich/dev/af/d/football_data/ADP.csv')
    # rb_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/rb_data.csv', dtype={'Year': object})
    # rb_adp = adp_temp['Pos'] == 'RB'
    # rb_temp = adp_temp[rb_adp]
    # wr_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/wr_data.csv')
    # qb_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/QB_data.csv')
    # te_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/TE_data.csv')
    # def_temp = pd.read_csv('/Users/Rich2/dev/airflow_home/dags/football_data/def_data.csv')
    # return adp_temp, rb_temp, wr_temp, qb_temp, te_temp, def_temp
    return adp_temp
    # rb_adp = pd.concat([rb_temp, adp_temp], keys=['Pick'], axis=1)
    # wr_adp = pd.concat([wr_temp, adp_temp], keys=['Pick'], axis=1)
    # qb_adp = pd.concat([qb_temp, adp_temp], keys=['Pick'], axis=1)
    # te_adp = pd.concat([te_temp, adp_temp], keys=['Pick'], axis=1)
    # def_adp = pd.concat([def_temp, adp_temp], keys=['Pick'], axis=1)
    # rb_adp.to_csv(r'rb_data.csv', index=False)
    # wr_adp.to_csv(r'wr_data.csv', index=False)
    # qb_adp.to_csv(r'QB_data.csv', index=False)
    # te_adp.to_csv(r'TE_data.csv', index=False)
    # def_adp.to_csv(r'def_data.csv', index=False)
    # print(wr_adp, qb_adp, te_adp, def_adp)


def rb_avg(adp_temp):
    # rb_adp = adp_temp.Pos.str.contains('RB')
    rb_adp = adp_temp['Pos'] == 'RB'
    # rb_adp = adp_temp[adp_temp['Pos'] == 'RB']
    rb_temp = adp_temp[rb_adp]

    rb10 = rb_temp['Year'] == 2010
    rb_2010 = rb_temp[rb10]
    rbp10 = rb_2010['Pick'].mean()

    rb11 = rb_temp['Year'] == 2011
    rb_2011 = rb_temp[rb11]
    rbp11 = rb_2011['Pick'].mean()

    rb12 = rb_temp['Year'] == 2012
    rb_2012 = rb_temp[rb12]
    rbp12 = rb_2012['Pick'].mean()

    rb13 = rb_temp['Year'] == 2013
    rb_2013 = rb_temp[rb13]
    rbp13 = rb_2013['Pick'].mean()

    rb14 = rb_temp['Year'] == 2014
    rb_2014 = rb_temp[rb14]
    rbp14 = rb_2014['Pick'].mean()

    rb15 = rb_temp['Year'] == 2015
    rb_2015 = rb_temp[rb15]
    rbp15 = rb_2015['Pick'].mean()

    rb16 = rb_temp['Year'] == 2016
    rb_2016 = rb_temp[rb16]
    rbp16 = rb_2016['Pick'].mean()

    rb17 = rb_temp['Year'] == 2017
    rb_2017 = rb_temp[rb17]
    rbp17 = rb_2017['Pick'].mean()

    rb18 = rb_temp['Year'] == 2018
    rb_2018 = rb_temp[rb18]
    rbp18 = rb_2018['Pick'].mean()

    rb19 = rb_temp['Year'] == 2019
    rb_2019 = rb_temp[rb19]
    rbp19 = rb_2019['Pick'].mean()

    rb_range = (rbp10, rbp11, rbp12, rbp13, rbp14, rbp15, rbp16, rbp17, rbp18, rbp19)
    rb_avg = pd.Series(rb_range, index=[rbp10, rbp11, rbp12, rbp13, rbp14, rbp15, rbp16, rbp17, rbp18, rbp19])
    rb_avg.to_csv('rb_avg.csv')
    rb_avg.plot()

    # return adp_temp


def qb_avg(adp_temp):
    qb_adp = adp_temp['Pos'] == 'QB'
    qb_pick = adp_temp[qb_adp]

    qb10 = qb_pick['Year'] == 2010
    qb_2010 = qb_pick[qb10]
    qbp10 = qb_2010['Pick'].mean()

    qb11 = qb_pick['Year'] == 2011
    qb_2011 = qb_pick[qb11]
    qbp11 = qb_2011['Pick'].mean()

    qb12 = qb_pick['Year'] == 2012
    qb_2012 = qb_pick[qb12]
    qbp12 = qb_2012['Pick'].mean()

    qb13 = qb_pick['Year'] == 2013
    qb_2013 = qb_pick[qb13]
    qbp13 = qb_2013['Pick'].mean()

    qb14 = qb_pick['Year'] == 2014
    qb_2014 = qb_pick[qb14]
    qbp14 = qb_2014['Pick'].mean()

    qb15 = qb_pick['Year'] == 2015
    qb_2015 = qb_pick[qb15]
    qbp15 = qb_2015['Pick'].mean()

    qb16 = qb_pick['Year'] == 2016
    qb_2016 = qb_pick[qb16]
    qbp16 = qb_2016['Pick'].mean()

    qb17 = qb_pick['Year'] == 2017
    qb_2017 = qb_pick[qb17]
    qbp17 = qb_2017['Pick'].mean()

    qb18 = qb_pick['Year'] == 2018
    qb_2018 = qb_pick[qb18]
    qbp18 = qb_2018['Pick'].mean()

    qb19 = qb_pick['Year'] == 2019
    qb_2019 = qb_pick[qb19]
    qbp19 = qb_2019['Pick'].mean()

    qb_range = (qbp10, qbp11, qbp12, qbp13, qbp14, qbp15, qbp16, qbp17, qbp18, qbp19)
    print(qb_range)
    qb_avg = pd.Series(qb_range, index=[2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019])
    qb_avg.to_csv('qb_avg.csv')
    qb_avg.plot()


def wr_avg(adp_temp):
    wr_adp = adp_temp['Pos'] == 'WR'
    wr_pick = adp_temp[wr_adp]

    wr10 = wr_pick['Year'] == 2010
    wr_2010 = wr_pick[wr10]
    wrp10 = wr_2010['Pick'].mean()

    wr11 = wr_pick['Year'] == 2011
    wr_2011 = wr_pick[wr11]
    wrp11 = wr_2011['Pick'].mean()

    wr12 = wr_pick['Year'] == 2012
    wr_2012 = wr_pick[wr12]
    wrp12 = wr_2012['Pick'].mean()

    wr13 = wr_pick['Year'] == 2013
    wr_2013 = wr_pick[wr13]
    wrp13 = wr_2013['Pick'].mean()

    wr14 = wr_pick['Year'] == 2014
    wr_2014 = wr_pick[wr14]
    wrp14 = wr_2014['Pick'].mean()

    wr15 = wr_pick['Year'] == 2015
    wr_2015 = wr_pick[wr15]
    wrp15 = wr_2015['Pick'].mean()

    wr16 = wr_pick['Year'] == 2016
    wr_2016 = wr_pick[wr16]
    wrp16 = wr_2016['Pick'].mean()

    wr17 = wr_pick['Year'] == 2017
    wr_2017 = wr_pick[wr17]
    wrp17 = wr_2017['Pick'].mean()

    wr18 = wr_pick['Year'] == 2018
    wr_2018 = wr_pick[wr18]
    wrp18 = wr_2018['Pick'].mean()

    wr19 = wr_pick['Year'] == 2019
    wr_2019 = wr_pick[wr19]
    wrp19 = wr_2019['Pick'].mean()

    wr_range = (wrp10, wrp11, wrp12, wrp13, wrp14, wrp15, wrp16, wrp17, wrp18, wrp19)
    print(wr_range)
    wr_avg = pd.Series(wr_range, index=[2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019])
    wr_avg.to_csv('wr_avg.csv')
    wr_avg.plot()


def te_avg(adp_temp):
    te_adp = adp_temp['Pos'] == 'TE'
    te_pick = adp_temp[te_adp]

    te10 = te_pick['Year'] == 2010
    te_2010 = te_pick[te10]
    tep10 = te_2010['Pick'].mean()

    te11 = te_pick['Year'] == 2011
    te_2011 = te_pick[te11]
    tep11 = te_2011['Pick'].mean()

    te12 = te_pick['Year'] == 2012
    te_2012 = te_pick[te12]
    tep12 = te_2012['Pick'].mean()

    te13 = te_pick['Year'] == 2013
    te_2013 = te_pick[te13]
    tep13 = te_2013['Pick'].mean()

    te14 = te_pick['Year'] == 2014
    te_2014 = te_pick[te14]
    tep14 = te_2014['Pick'].mean()

    te15 = te_pick['Year'] == 2015
    te_2015 = te_pick[te15]
    tep15 = te_2015['Pick'].mean()

    te16 = te_pick['Year'] == 2016
    te_2016 = te_pick[te16]
    tep16 = te_2016['Pick'].mean()

    te17 = te_pick['Year'] == 2017
    te_2017 = te_pick[te17]
    tep17 = te_2017['Pick'].mean()

    te18 = te_pick['Year'] == 2018
    te_2018 = te_pick[te18]
    tep18 = te_2018['Pick'].mean()

    te19 = te_pick['Year'] == 2019
    te_2019 = te_pick[te19]
    tep19 = te_2019['Pick'].mean()

    te_range = (tep10, tep11, tep12, tep13, tep14, tep15, tep16, tep17, tep18, tep19)
    print(te_range)
    te_avg = pd.Series(te_range, index=[2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019])
    te_avg.to_csv('te_avg.csv')
    te_avg.plot()

# # @classmethod
# def df_avg(adp_temp):
#     def_adp = adp_temp['Pos'] == 'DEF'
#     def_pick = adp_temp[def_adp]
#     return def_pick


class DfYearAvg:
    def __init__(self, name, year, pos='DEF'):
        self.pos = pos
    #     self.p_hold = p_hold
        self.name = name
        self.year = year
    #     pass

    @staticmethod
    def df_avg(adp_temp=df_update()):
        """takes adp_temp df and isolates position(Pos) column
        and returns a new df that's position specific"""
        def_adp = adp_temp[DfYearAvg.self.pos]
        def_pick = adp_temp[def_adp]
        return def_pick

    # @staticmethod
    def new_df(self, def_pick=None):
        """takes position specific df and isolates
        year and then finds the mean value of the position pick
        for that year"""
        n_df = def_pick[self.year]
        self.name = n_df[self.pos].mean()
        return self.name


d10 = DfYearAvg('defp10', 2010)
d10.new_df()
#
# df_range = []


# df_range.append(DfYearAvg.new_df()

# def_range.append(DfYearAvg.yearReturn(name='defp10', year=2010, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp11', year=2011, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp12', year=2012, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp13', year=2013, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp14', year=2014, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp15', year=2015, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp16', year=2016, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp17', year=2017, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp18', year=2018, def_pick=df_avg(adp_temp=df_update())))
# def_range.append(DfYearAvg.yearReturn(name='defp19', year=2019, def_pick=df_avg(adp_temp=df_update())))

# def_range = (defp10, defp11, defp12, defp13, defp14, defp15, defp16, defp17, defp18, defp19)
# print(def_range)
# def_avg = pd.Series(def_range, index=[2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019])
# def_avg.to_csv('def_avg.csv')
# def_avg.plot()
#

# def df_avg(adp_temp):
#     def_adp = adp_temp['Pos'] == 'DEF'
#     def_pick = adp_temp[def_adp]
#
#     def10 = def_pick['Year'] == 2010
#     def_2010 = def_pick[def10]
#     defp10 = def_2010['Pick'].mean()
#
#     def11 = def_pick['Year'] == 2011
#     def_2011 = def_pick[def11]
#     defp11 = def_2011['Pick'].mean()
#
#     def12 = def_pick['Year'] == 2012
#     def_2012 = def_pick[def12]
#     defp12 = def_2012['Pick'].mean()
#
#     def13 = def_pick['Year'] == 2013
#     def_2013 = def_pick[def13]
#     defp13 = def_2013['Pick'].mean()
#
#     def14 = def_pick['Year'] == 2014
#     def_2014 = def_pick[def14]
#     defp14 = def_2014['Pick'].mean()
#
#     def15 = def_pick['Year'] == 2015
#     def_2015 = def_pick[def15]
#     defp15 = def_2015['Pick'].mean()
#
#     def16 = def_pick['Year'] == 2016
#     def_2016 = def_pick[def16]
#     defp16 = def_2016['Pick'].mean()
#
#     def17 = def_pick['Year'] == 2017
#     def_2017 = def_pick[def17]
#     defp17 = def_2017['Pick'].mean()
#
#     def18 = def_pick['Year'] == 2018
#     def_2018 = def_pick[def18]
#     defp18 = def_2018['Pick'].mean()
#
#     def19 = def_pick['Year'] == 2019
#     def_2019 = def_pick[def19]
#     defp19 = def_2019['Pick'].mean()
#
#     def_range = (defp10, defp11, defp12, defp13, defp14, defp15, defp16, defp17, defp18, defp19)
#     print(def_range)
#     def_avg = pd.Series(def_range, index=[2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019])
#     def_avg.to_csv('def_avg.csv')
#     def_avg.plot()


def psql_adp():
    # conn = psycopg2.connect("host=localhost dbname=ff_fb user=rich")
    conn = sqlalchemy.create_engine('postgresql+psycopg2://rich:Coder20!@localhost:5432/ff_fb')
    df = pd.read_csv('/Users/rich/dev/af/d/football_data/ADP.csv', delimiter=',')
    # conn.connect() as conn, conn.begin():
    df.to_sql(name="adp", con=conn, schema='fantasy_data', if_exists='replace')


def psql_rb():
    # conn = psycopg2.connect("host=localhost dbname=ff_fb user=rich")
    conn = sqlalchemy.create_engine('postgresql+psycopg2://rich:Coder20!@localhost:5432/ff_fb')
    df = pd.read_csv('/Users/rich/dev/af/d/rb_avg.csv', delimiter=',')
    #     with conn.connect() as conn, conn.begin():
    df.to_sql(name="rb", con=conn, schema='fantasy_data', if_exists='replace')


def psql_wr():
    # conn = psycopg2.connect("host=localhost dbname=ff_fb user=rich")
    conn = sqlalchemy.create_engine('postgresql+psycopg2://rich:Coder20!@localhost:5432/ff_fb')
    df = pd.read_csv('/Users/rich/dev/af/d/wr_avg.csv', delimiter=',')
    # with conn.connect() as conn, conn.begin():
    df.to_sql(name="wr", con=conn, schema='fantasy_data', if_exists='replace')


# conn = psycopg2.connect("host=localhost dbname=ff_fb user=rich") 

def psql_qb():
    # conn = psycopg2.connect("host=localhost dbname=ff_fb user=rich")
    conn = sqlalchemy.create_engine('postgresql+psycopg2://rich:Coder20!@localhost:5432/ff_fb')
    df = pd.read_csv('/Users/rich/dev/af/d/qb_avg.csv', delimiter=',')
    # with conn.connect() as conn, conn.begin():
    df.to_sql(name="qb", con=conn, schema='fantasy_data', if_exists='replace')


def psql_te():
    # conn = psycopg2.connect("host=localhost dbname=ff_fb user=rich")
    conn = sqlalchemy.create_engine('postgresql+psycopg2://rich:Coder20!@localhost:5432/ff_fb')
    df = pd.read_csv('/Users/rich/dev/af/d/te_avg.csv', delimiter=',')
    # with conn.connect() as conn, conn.begin():
    df.to_sql(name="te", con=conn, schema='fantasy_data', if_exists='replace')


def psql_def():
    # conn = psycopg2.connect("host=localhost dbname=ff_fb user=rich")
    conn = sqlalchemy.create_engine('postgresql+psycopg2://rich:Coder20!@localhost:5432/ff_fb')
    df = pd.read_csv('/Users/rich/dev/af/d/def_avg.csv', delimiter=',')
    # with conn.connect() as conn, conn.begin():
    df.to_sql(name="def", con=conn, schema='fantasy_data', if_exists='replace')


#
# def comparison():
#     comp_graph = pd.DataFrame(
#         {'x': range(2010, 2020), 'rb': rb_avg, 'te': te_avg, 'qb': qb_avg, 'wr': wr_avg, 'def': df_avg})
#
#     palette = plt.get_cmap('Set1')
# # multiple line plot
#     plt.plot('x', 'rb', data=comp_graph, marker='o', markerfacecolor='blue', markersize=12, color='skyblue', linewidth=4)
#     plt.plot('x', 'te', data=comp_graph, marker='', color='olive', linewidth=3)
#     plt.plot('x', 'qb', data=comp_graph, marker='', color='blue', linewidth=3)
#     plt.plot('x', 'wr', data=comp_graph, marker='', color='yellow', linewidth=3)
#     plt.plot('x', 'def', data=comp_graph, marker='', color='pink', linewidth=3, linestyle='dashed')
# # plt.plot( 'x', 'y3', data=df, marker='', color='olive', linewidth=2, linestyle='dashed', label="toto")
#     plt.legend()
#
#
# def adp_tables():
#     """connects to postgres to present plots from saved png and pdf files"""
#     conn = sqlalchemy.create_engine('postgresql://Rich2:Coder20!@localhost:5432/ff_fb')
#     rb = pd.read_sql_table('rb', conn)
#     AP = rb.loc[rb['Player'] == 'Adrian Peterson']
#     AP.describe()
#     AP_snapshot = {plt.figure(figsize=(8, 6), dpi=240),
#                    plt.xlim(2010, 2019),
#                    plt.ylim(0, 10),
#                    plt.xlabel('Year'), plt.ylabel('Pick'),
#                    plt.title('Year to Pick'),
#                    plt.scatter(x=AP['Year'], y=AP['Pick']),
#                    plt.show(),
#                    }
#     plt.savefig('AP_snapshot.pdf')
#     plt.savefig('AP_snapshot.png')
#     ap_yp = AP.plot(x='Year', y='Pick')
#     plt.savefig(ap_yp + '.pdf')
#     plt.savefig(ap_yp + '.png')
#     ap_year_yds = AP.plot(x='Year', y='Rush Yds')
#     plt.savefig('AP_year_yds.pdf')
#     plt.savefig('AP_year_yds.png')
#
# rb.to_csv()
# # rb.to_string()
# pdfkit.from_file('rb.csv', 'rb.pdf')

#
# data_jawn = PythonOperator(
#     task_id='pdfs',
#     python_callable=adp_tables,
#     provide_context=False,
#     dag=dag,
# )


# dframes = PythonOperator(
#     task_id='dframes',
#     python_callable=df_update,
#     provide_context=False,
#     dag=dag,
# )
#
# t1 = PythonOperator(
#     task_id='psql_adp',
#     python_callable=psql_adp,
#     provide_context=False,
#     # do_xcom_push=True,
#     dag=dag,
# )
#
# t2 = PythonOperator(
#     task_id='psql_rb',
#     python_callable=psql_rb,
#     provide_context=False,
#     dag=dag,
# )
#
# t3 = PythonOperator(
#     task_id='psql_wr',
#     python_callable=psql_wr,
#     provide_context=False,
#     dag=dag,
# )
#
# t4 = PythonOperator(
#     task_id='psql_qb',
#     python_callable=psql_qb,
#     provide_context=False,
#     dag=dag,
# )
#
# t5 = PythonOperator(
#     task_id='psql_def',
#     python_callable=psql_def,
#     provide_context=False,
#     dag=dag,
# )
#
# t6 = PythonOperator(
#     task_id='psql_te',
#     python_callable=psql_te,
#     provide_context=False,
#     dag=dag,
# )


# run_this = PapermillOperator(
#     task_id="run_notebook",
#     input_nb="/Users/Rich2/dev/DataEngineering.Labs.AirflowProject/dags/football.ipynb",
#     start_date=datetime(2018, 1, 1),
#     output_nb="/Users/Rich2/dev/DataEngineering.Labs.AirflowProject/dags/out-{{ execution_date }}.ipynb",
#     parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"}
# )

# run_noteboook = PaperMillOperator(
#     task_id='dframe',
#     python_callable=dframe,
#     provide_context=True,
#     dag=dag)


# dframes.set_downstream([t1, t2, t3, t4, t5, t6])

if __name__ == "__main__":
    df_update()
    rb_avg(df_update())
    qb_avg(df_update())
    wr_avg(df_update())
    te_avg(df_update())
    DfYearAvg.df_avg(adp_temp=df_update())
    # df_avg(df_update())
    # DfYearAvg.yearReturn(def_pick=df_avg(adp_temp=df_update()))
    psql_adp()
    psql_def()
    psql_qb()
    psql_rb()
    psql_te()
    psql_wr()

    # adp_tables()

# adp_tables()


# start_task >> upload_to_S3_task
