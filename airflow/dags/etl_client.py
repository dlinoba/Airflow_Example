import datetime as dt
from  datetime import datetime, date, timedelta
from sqlalchemy import create_engine, select, update, func, and_, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime, String, CHAR, DECIMAL
from sqlalchemy.orm import sessionmaker
import sqlalchemy as db
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

Base = declarative_base()

database_server_stage = Variable.get("database_server_stage")
database_login_stage = Variable.get("database_login_stage")
database_password_stage = Variable.get("database_password_stage")
database_name_stage = Variable.get("database_name_stage")

database_server_oltp = Variable.get("database_server_oltp")
database_login_oltp = Variable.get("database_login_oltp")
database_password_oltp = Variable.get("database_password_oltp")
database_name_oltp = Variable.get("database_name_oltp")

database_server_dw = Variable.get("database_server_dw")
database_login_dw = Variable.get("database_login_dw")
database_password_dw = Variable.get("database_password_dw")
database_name_dw = Variable.get("database_name_dw")

url_connection_stage = "mysql+pymysql://{}:{}@{}/{}".format(
                 str(database_login_stage)
                ,str(database_password_stage)
                ,str(database_server_stage)
                ,str(database_name_stage)
                )

url_connection_oltp = "mysql+pymysql://{}:{}@{}/{}".format(
                 str(database_login_oltp)
                ,str(database_password_oltp)
                ,str(database_server_oltp)
                ,str(database_name_oltp)
                )

url_connection_dw = "mysql+pymysql://{}:{}@{}/{}".format(
                 str(database_login_dw)
                ,str(database_password_dw)
                ,str(database_server_dw)
                ,str(database_name_dw)
                )

engine_stage = db.create_engine(url_connection_stage)
engine_oltp = db.create_engine(url_connection_oltp)
engine_dw = db.create_engine(url_connection_dw)


class LOG_CLIENTE(Base):
    __tablename__ = 'LOG_CLIENTE'

    ID = Column(Integer, primary_key = True)
    IDCLIENTE = Column(Integer)
    OPERACAO = Column(String(10))
    DATA_OPERACAO = Column(DateTime)

class CLIENTE(Base):
    __tablename__ = 'CLIENTE'

    IDCLIENTE = Column(Integer, primary_key = True)
    NOME = Column(String(30))
    SOBRENOME = Column(String(30))
    EMAIL = Column(String(60))
    SEXO = Column(CHAR(1))
    NASCIMENTO = Column(DateTime)

class FATO(Base):
    __tablename__ = 'FATO'

    IDNOTA = Column(Integer, primary_key = True)
    IDCLIENTE = Column(Integer)
    IDVENDEDOR = Column(Integer)
    IDFORMA = Column(Integer)
    IDPRODUTO = Column(Integer)
    IDFORNECEDOR = Column(Integer)
    IDTEMPO = Column(Integer)
    QUANTIDADE = Column(Integer)
    TOTAL_ITEM = Column(DECIMAL(10,2))
    CUSTO_TOTAL = Column(DECIMAL(10,2))
    LUCRO_TOTAL = Column(DECIMAL(10,2))

class DIM_TEMPO(Base):
    __tablename__ = 'DIM_TEMPO'

    IDSK = Column(Integer, primary_key = True)
    DATA = Column(DateTime)
    DIA = Column(CHAR(2))
    DIASEMANA = Column(String(10))
    MES = Column(CHAR(2))
    NOMEMES = Column(String(20))
    QUARTO = Column(Integer)
    NOMEQUARTO = Column(String(20))
    ANO = Column(CHAR(4))
    ESTACAOANO = Column(String(20))
    FIMSEMANA = Column(CHAR(3))
    DATACOMPLETA = Column(String(10))

class DIM_CLIENTE(Base):
    __tablename__ = 'DIM_CLIENTE'

    IDSK = Column(Integer, primary_key = True)
    IDCLIENTE = Column(Integer)
    INICIO = Column(DateTime)
    FIM = Column(DateTime)
    NOME = Column(String(100))
    SEXO = Column(String(20))
    EMAIL = Column(String(60))
    NASCIMENTO = Column(DateTime)
    CIDADE = Column(String(100))
    ESTADO = Column(String(20))
    REGIAO = Column(String(20))

class ST_CLIENTE(Base):
    __tablename__ = 'ST_CLIENTE'

    IDCLIENTE  = Column(Integer, primary_key = True)
    NOME = Column(String(30))
    SOBRENOME = Column(String(30))
    SEXO = Column(CHAR(1))
    EMAIL = Column(String(60))
    NASCIMENTO = Column(DateTime)
    CIDADE = Column(String(100))
    ESTADO = Column(String(20))
    REGIAO = Column(String(20))
    OPERACAO = Column(String(10))


DEFAULT_ARGS = {'owner': 'Airflow',
                'depends_on_past': False,
                'start_date': datetime(2023, 4, 1),
                }

dag = DAG('etl_client', 
          default_args=DEFAULT_ARGS,
          schedule_interval='00 14 * * *'
        )

def trunc_stage_table():
    engine_stage.execute("TRUNCATE TABLE ST_CLIENTE")


def extract_oltp_clients():
    
    Session_dw = sessionmaker(bind=engine_dw)
    session_dw = Session_dw()

    Session_oltp = sessionmaker(bind=engine_oltp)
    session_oltp = Session_oltp()

    max_date = session_dw.query(func.max(DIM_TEMPO.DATA)).join(FATO, FATO.IDTEMPO == DIM_TEMPO.IDSK).first()

    if max_date[0] is None:
        max_date = dt.date(2023,1,1)
    else:
        max_date = max_date[0]

    df = pd.read_sql_query(
        sql = session_oltp.query(CLIENTE,LOG_CLIENTE.OPERACAO).join(CLIENTE, LOG_CLIENTE.IDCLIENTE==CLIENTE.IDCLIENTE).filter(
                        LOG_CLIENTE.DATA_OPERACAO >= max_date).statement,
            con = engine_oltp)
    
    df_uf = pd.read_excel('/opt/airflow/dags/CLIENTES_ESTADOS.xlsx')
    
    df_final = pd.merge(df,df_uf, on="IDCLIENTE")

    df_final = df_final.reindex(columns=['IDCLIENTE','NOME','SOBRENOME','SEXO','EMAIL','NASCIMENTO','CIDADE','ESTADO','REGIAO','OPERACAO'])
    
    df_final.to_sql(name='ST_CLIENTE',con=engine_stage, if_exists='append', index=False)

def transform():
    query = """SELECT * FROM ST_CLIENTE;"""
    df_clientes = pd.read_sql_query(query,engine_stage)
    
    df_clientes['NOME_COMPLETO'] = df_clientes['NOME'].map(str)+' '+df_clientes['SOBRENOME'].map(str)
    df_clientes = df_clientes.drop(columns=['NOME','SOBRENOME'])
    df_clientes['SEXO'] = df_clientes['SEXO'].apply(lambda x: 'Masculino' if x == 'M' else 'Femenino')
    df_clientes = df_clientes.reindex(columns=["IDCLIENTE","NOME_COMPLETO","SEXO","EMAIL","NASCIMENTO","CIDADE","ESTADO","REGIAO","OPERACAO"])
    df_clientes.rename(columns={'NOME_COMPLETO':'NOME'},inplace=True)
    
    df_clientes['EMAIL'] = df_clientes['EMAIL'].str.lower()
    df_clientes['CIDADE'] = df_clientes['CIDADE'].str.title()
    df_clientes['ESTADO'] = df_clientes['ESTADO'].str.title()
    df_clientes['REGIAO'] = df_clientes['REGIAO'].str.title()

    df_clientes.to_csv("/tmp/CLIENTES_TRATADO.csv", encoding='utf-8', index=False)

def load():
    #carrega os dados a partir da área de staging.
    df = pd.read_csv("/tmp/CLIENTES_TRATADO.csv")

    Session_dw = sessionmaker(bind=engine_dw)
    session_dw = Session_dw()

    DT_INICIO = datetime.today().strftime('%Y-%m-%d')

    df_insert = df.loc[df['OPERACAO'] == 'Inclusão']
    df_update = df.loc[df['OPERACAO'] == 'Alteração']
    
    if not df_insert.empty:
        for index, row in df_insert.iterrows():

            cmd_insert = session_dw.query(DIM_CLIENTE.IDSK).filter(
                    DIM_CLIENTE.IDCLIENTE == row['IDCLIENTE'],  
                    DIM_CLIENTE.FIM == None).order_by(DIM_CLIENTE.IDSK.desc()).first()

            if cmd_insert == None:
                newCliente = DIM_CLIENTE(IDCLIENTE = row['IDCLIENTE'],
                               INICIO = DT_INICIO,
                               FIM = None,
                               NOME = row['NOME'],
                               SEXO = row['SEXO'],
                               EMAIL = row['EMAIL'],
                               NASCIMENTO = row['NASCIMENTO'],
                               CIDADE = row['CIDADE'],
                               ESTADO = row['ESTADO'],
                               REGIAO = row['REGIAO'])
                session_dw.add(newCliente)
                session_dw.commit()
    
    if not df_update.empty:
        for index, row in df_update.iterrows():
            updCliente = DIM_CLIENTE(IDCLIENTE = row['IDCLIENTE'],
                               INICIO = DT_INICIO,
                               FIM = None,
                               NOME = row['NOME'],
                               SEXO = row['SEXO'],
                               EMAIL = row['EMAIL'],
                               NASCIMENTO = row['NASCIMENTO'],
                               CIDADE = row['CIDADE'],
                               ESTADO = row['ESTADO'],
                               REGIAO = row['REGIAO'])

            updated = session_dw.query(DIM_CLIENTE.IDSK).filter(
                    DIM_CLIENTE.IDCLIENTE == row['IDCLIENTE'],  
                    DIM_CLIENTE.FIM == None,
                    or_(DIM_CLIENTE.NOME != row['NOME'],DIM_CLIENTE.CIDADE != row['CIDADE'])).order_by(DIM_CLIENTE.IDSK.desc()).first()
            
            if updated != None:
                upd = update(DIM_CLIENTE).\
                      where(DIM_CLIENTE.IDSK == updated[0]).\
                      values(FIM=DT_INICIO)
                session_dw.execute(upd)
                session_dw.add(updCliente)
                session_dw.commit()
    
task_trunc_stage_table = PythonOperator(
    task_id='trunc_table_st_cliente',
    provide_context=True,
    python_callable=trunc_stage_table,
    dag=dag
)

task_extract_oltp_clients = PythonOperator(
    task_id='get_clients',
    provide_context=True,
    python_callable=extract_oltp_clients,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data_to_mysql',
    provide_context=True,
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;",
    dag=dag
)

task_trunc_stage_table >> task_extract_oltp_clients >> transform_task >> load_task >> clean_task
