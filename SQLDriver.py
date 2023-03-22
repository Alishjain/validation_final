# Databricks notebook source
import os
import config
import adal
import pandas as pd
import struct
import pyodbc
import mysql.connector
from sqlalchemy import create_engine
import hashlib
# from pyspark.sql.functions import md5, concat_ws, withColumn


def GetSQLDF(table_name,User,Password,Database):
    # # input params
    # # tenant = os.getenv('TENANT-ID')
    # server = os.getenv('SQL-SERVER')
    # database = os.getenv('SQL-DATABASE')
    # username = os.getenv('SQL-USERNAME')

    # resource_app_id_url = os.getenv('RESOURCE-APP-ID-URL')
    # authority_host_url = os.getenv('AUTHORITY-HOST-URL')
    # authority_url = authority_host_url + '/' + tenant
    # # client_id = os.getenv('CLIENT-ID')

    # context = adal.AuthenticationContext(authority_url, api_version=None)
    # # context = adal.AuthenticationContext(resource_app_id_url, api_version=None)

    # code = context.acquire_user_code(resource_app_id_url, client_id)
    # # code = context.acquire_user_code(resource_app_id_url)

    # print(code['message'])
    # token = context.acquire_token_with_device_code(resource_app_id_url, code, client_id)
    # # token = context.acquire_token_with_device_code(resource_app_id_url, code)

    # odbcConnstr = "DRIVER={};SERVER={};DATABASE={}".format(
    #     "ODBC Driver 17 for SQL Server", server, database)

    # # get bytes from token obtained
    # tokenb = bytes(token["accessToken"], "UTF-8")
    # exptoken = b''
    # for i in tokenb:
    #     exptoken += bytes({i})
    #     exptoken += bytes(1)

    # tokenstruct = struct.pack("=i", len(exptoken)) + exptoken

    # conn = pyodbc.connect(odbcConnstr, attrs_before={1256: tokenstruct})
    # return conn

    mydb = mysql.connector.connect(
        host = "localhost",
        user = User,
        password = Password,
        database = Database
    )

    cursor = mydb.cursor()
    # Show existing tables
    cursor.execute("SHOW TABLES")
        
    for x in cursor:
    # print(x)   
 
     sql_connection_string = config.sql_connection_string

    # SQLAlchemy connectable
    cnx = create_engine(sql_connection_string).connect()


    df = pd.read_sql_table(table_name, cnx)

    return df 


def ExecuteSQLQuery(table1,table2):

    df_Source = GetSQLDF(table1)
    df_Destination = GetSQLDF(table2)
    
    source_row = df_Source.shape  
    # print("No_of_rows_Source => ", source_row)
    destination_rows = df_Destination.shape  
    # print("No_of_rows_Destination => ", destination_rows)  

    if source_row == destination_rows :

        def hash_row(row):
            hasher = hashlib.sha256()
            for value in row:
                hasher.update(str(value).encode('utf-8'))
            return hasher.hexdigest()

        
        df_Source['hash'] = df_Source.apply(hash_row, axis=1)
        df_Destination['hash'] = df_Destination.apply(hash_row, axis=1)

        # print(df)
        # print(df1)


        df_source = df_Source[['Id','hash']]
        df_destination = df_Destination[['Id','hash']]

        # print(df_source)
        # print(df1_destination)

        # compare = df.compare(df1,align_axis=0,keep_shape=True,keep_equal=True)

        compare = df_source.compare(df_destination, result_names=("Source", "Destination"))

        # print("Result : \n",compare)

        get_data = compare.index

        get_data=[i+1 for i in get_data]    

        source_error_df = df_Source[df_Source.Id.isin(get_data)].sort_index() 
        source_error_df = source_error_df.loc[:, source_error_df.columns != 'hash']

        destination_error_df = df_Destination[df_Destination.Id.isin(get_data)].sort_index() 
        destination_error_df = destination_error_df.loc[:, destination_error_df.columns != 'hash']

        # print("Dataframe_source : \n",source_error_df)
        # print("Dataframe_Destination : \n",destination_error_df)

    else:
        print("DataFrame Row Count Not Matched")


#     # query = 'SELECT TOP 3 name, collation_name FROM sys.databases;'
#     result = pd.read_sql(query, conn)
#     return result.head()


# ExecuteSQLQuery('organizations_record','organizations_record_test')


    

