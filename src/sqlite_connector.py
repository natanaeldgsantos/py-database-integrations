# ***************************************
#
# Created by: Natanael Domingos
# Created at: 14/05/2022
# Version: 01
#
#****************************************


import pandas as pd
import os 
from   datetime import datetime

import sqlite3 as sqlite
from   sqlite3 import Error

WORK_DIR = os.getcwd()                    # working directory
DATA_DIR = os.path.join(WORK_DIR, 'data') # data directory


# Padroniza o nome das tabelas
def clear_name(file_name):

    # in  -> parameter: file_name : recebe o nome da tabela não padronizado
    # out -> return:   table_name : retorna o nome da tabela padronizado

    table_name = 'tb_' + file_name.strip('.csv').replace('olist_','').replace('_dataset','')
    return table_name


# filtra somente os arquivos csv
csv_files   = [ file for file in os.listdir(DATA_DIR) if file.endswith('.csv') ] 


# Gera uma conexão com o banco de dados SQLite
def get_connection():

    # out -> return:   connection : retorna uma conexão validada com o banco de dados

    connection = None    
    try:

        # Cria arquivo local banco de dados ecommerce.db
        connection = sqlite.connect(os.path.join(DATA_DIR,'ecommerce.db'))
        
        cursor =  connection.cursor()
        print('Banco de dados Criado >>> Conexão com SQLite realizada com Sucesso ')

        bd_version_query = 'SELECT sqlite_version();'
        cursor.execute(bd_version_query)
        record = cursor.fetchall()
        print("SQLite Database version = ", record[0])

    except Error as e:
        print('Erro ao conectar com o SQLite ', e)   

    return connection




# Realiza a carga de dados no SQLite
def upload_data():

    my_connection = get_connection()

    try:
        for file in csv_files:

            table_name = clear_name(file) # gera nome das tabelas de forma padronizada

            # Gera o Dataframe
            df_tmp = pd.read_csv( os.path.join(DATA_DIR,file) ) 
            df_tmp['upload_datetime'] = datetime.now().isoformat()
            
            # Carrega o dataframe como tabela sql
            df_tmp.to_sql(table_name, my_connection, if_exists='replace')

        #Sucesso
        print("Carga de dados finalizada com sucesso")

    except Error as e:
        #Erro
        print('Erro ao completar a carga de dados',e)

    
   
upload_data()