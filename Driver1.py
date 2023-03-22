import json
import csv
import pandas as pd
import numpy as np
import hashlib
from datetime import datetime
import configparser
from CsvParser import getDFfromCsv, getDFfromXlsxMerge, getDFfromXls, check_dtype, check_ruleValidation,getDFfromXlsx
from JsonParser import GetAllValueByKey, GetRules
from Utility import getUniqueValueList, list_contains
from SQLDriver import GetSQLDF,ExecuteSQLQuery
# from pyspark.sql.functions import md5, concat_ws


def DoubleDataValidation():
        
    configFilePath= "configuration.ini"
    parser = configparser.ConfigParser()
    parser.read(configFilePath)

    # ruleFilePath = request.get('RULE_FILE_PATH')
    # source_data_file_path = parser.get('SOURCE', 'source_data_file_path')
    
    SOURCE_TYPE = parser.get('APP', 'source_type')
    DESTINATION_TYPE = parser.get('APP', 'dest_type')
    # SKIP_ROWS = parser.get('SOURCE', 'SKIP_ROWS') 
     
    subject = 'Data_Validation'
    date = datetime.now().strftime("%Y%m%d_%I%M%S")        
    output_file_path = parser.get('APP', 'output_file')
    reportOutputDir = output_file_path + '/report/'
    errorOutputDir = output_file_path + '/error/'
        
    # print("Report Output path - ", reportOutputDir)
    # print("Error Output path - ", errorOutputDir)

    
    sourcedf = pd.DataFrame()


    ############ Read Source DataFile #######

    ### CSV ####

    if SOURCE_TYPE == 'CSV':
       source_data_file_path = parser.get('SOURCE', 'source_data_file_path')
       skip_rows = parser.get('SOURCE', 'SKIP_ROWS')
       sourcedf = getDFfromCsv(source_data_file_path,skip_rows) 
       source_row,no_of_columns=sourcedf.shape  
      #  print("No_of_rows_Source => ", source_row) 

    ### XLSX ####

    if SOURCE_TYPE == 'XLSX':
        source_data_file_path = parser.get('SOURCE', 'source_data_file_path')
        sheet_name = parser.get('SOURCE', 'sheet_name')
        # Column_address = parser.get('SOURCE', 'Column_address')
        # Column_address1 = parser.get('SOURCE', 'Column_address1')
        skip_rows = parser.get('SOURCE', 'SKIP_ROWS')
        sourcedf= getDFfromXlsx(source_data_file_path,sheet_name,"","",skip_rows)
        source_row,no_of_columns=sourcedf.shape
        #csvdf = getDFfromXlsxMerge(SOURCE_DATA_FILE_PATH, SKIP_ROWS)
    
    ### MySql ####

    if SOURCE_TYPE == 'MYSQL':
       User = parser.get('SOURCE', 'source_user')
       Password = parser.get('SOURCE', 'source_password')
       Database = parser.get('SOURCE', 'source_database')
       SOURCE_SCHEMA_NAME = parser.get('vTurbineMasterData_Source' , 'schema_name_source')
       SOURCE_TABLE_NAME = parser.get('vTurbineMasterData_Source' , 'source_query_filter')
       sourcedf = GetSQLDF(SOURCE_TABLE_NAME,User,Password,Database)
       source_row,no_of_columns=sourcedf.shape
      #  print("No_of_rows_Source => ", source_row)


    # Source_csvColList = sourcedf.columns
    # Source_csvColList = getUniqueValueList(Source_csvColList)

    ############## Read Destination DataFile ####
    
    ### CSV ####
    
    destinationdf = pd.DataFrame()

    if DESTINATION_TYPE == 'CSV':
       destination_data_file_path = parser.get('DEST', 'dest_data_file_path')
       skip_rows = parser.get('DEST', 'SKIP_ROWS')
       destinationdf = getDFfromCsv(destination_data_file_path,skip_rows) 
       destination_rows,no_of_columns=destinationdf.shape  
      #  print("No_of_rows_Destination => ", destination_rows) 

    ### XLSX ####

    if DESTINATION_TYPE == 'XLSX':
        destination_data_file_path = parser.get('DEST', 'dest_data_file_path')
        sheet_name = parser.get('DEST', 'sheet_name')
        # Column_address = parser.get('DEST', 'Column_address')
        # Column_address1 = parser.get('DEST', 'Column_address1')
        skip_rows = parser.get('DEST', 'SKIP_ROWS')
        destinationdf= getDFfromXlsx(destination_data_file_path,sheet_name,"","",skip_rows)
        destination_rows,no_of_columns=sourcedf.shape
        #csvdf = getDFfromXlsxMerge(SOURCE_DATA_FILE_PATH, SKIP_ROWS)
    
    
    ### MySql ####

    if DESTINATION_TYPE == 'MYSQL':
       User = parser.get('DEST', 'dest_user')
       Password = parser.get('DEST', 'dest_password')
       Database = parser.get('DEST', 'dest_database')
       DESTINATION_SCHEMA_NAME = parser.get('vTurbineMasterData_Dest','schema_name_dest')
       DESTINATION_TABLE_NAME = parser.get('vTurbineMasterData_Dest' , 'destination_query_filter')
       destinationdf = GetSQLDF(DESTINATION_TABLE_NAME,User,Password,Database)
       destination_rows,no_of_columns=destinationdf.shape
      #  print("No_of_rows_Destination => ", destination_rows)
      

    Destination_csvColList = destinationdf.columns
    Destination_csvColList = getUniqueValueList(Destination_csvColList)

  
   #  print("Source_column_list => ",Source_csvColList)
   #  print("Destination_column_list => ",Destination_csvColList)

    # ***************** Validation **********************

    if source_row == destination_rows :

      def hash_row(row):
            hasher = hashlib.sha256()
            for value in row:
                hasher.update(str(value).encode('utf-8'))
            return hasher.hexdigest()

        
      sourcedf['hash'] = sourcedf.apply(hash_row, axis=1)
      destinationdf['hash'] = destinationdf.apply(hash_row, axis=1)

      # print("sss",sourcedf)
      # print("ddd",destinationdf)

      Sourcedf = sourcedf[['hash']]
      Destinationdf = destinationdf[['hash']]

      print("Source Hash Code Dataframe",Sourcedf)
      print("Destination Hash Code Dataframe",Destinationdf)

      compare_hash = pd.concat([Sourcedf[~Sourcedf.hash.isin(Destinationdf.hash)], Destinationdf[~Destinationdf.hash.isin(Sourcedf.hash)]])
      # compare = Sourcedf.compare(Destinationdf, result_names=("Source", "Destination"))
      print("Compare Using Hash Code : \n",compare_hash)

      get_index = compare_hash.index.drop_duplicates()

      # Get_index=[i+1 for i in get_index]    

      source_error_df = sourcedf[sourcedf.index.isin(get_index)].sort_index() 
      source_error_df = source_error_df.loc[:, source_error_df.columns != 'hash']

      destination_error_df = destinationdf[destinationdf.index.isin(get_index)].sort_index() 
      destination_error_df = destination_error_df.loc[:, destination_error_df.columns != 'hash']
      
      print("Error Source Dataframe : \n",source_error_df)
      print("Error Destination Dataframe : \n",destination_error_df)

      sample_df_source = sourcedf.sample(frac=0.1).sort_index()
      sample_index = sample_df_source.index 
      # a = [i+1 for i in  sample_df_source.index]       
      sample_df_destination = destinationdf[destinationdf.index.isin(sample_index)].sort_index()

      print("Source Random Record : \n",sample_df_source) 
      print("Destination Random Record : \n",sample_df_destination)

      sample_df_source_row,no_of_columns = sample_df_source.shape
      sample_df_destination_row,no_of_columns = sample_df_destination.shape

      if(sample_df_source_row == sample_df_destination_row):
        # compare = pd.concat([sample_df_source[~sample_df_source.isin(sample_df_destination)], sample_df_destination[~sample_df_destination.isin(sample_df_source)]])
        sample_df_source = sample_df_source.loc[:, sample_df_source.columns != 'hash']
        sample_df_destination = sample_df_destination.loc[:, sample_df_destination.columns != 'hash']
        compare = sample_df_source.compare(sample_df_destination, keep_shape=True, keep_equal=False)
        print("Compare of Random Data Result : \n",compare)
      else:
        print("COUNT NOT MATCHED")
        print("COUNT OF SOURCE DATAFRAME : ",sample_df_source_row)
        print("COUNT OF DESTINATION DATAFRAME : ",sample_df_source_row)
      
      
      try:
        col_name_min = parser.get('SOURCE', 'col_name_min')
        if(sourcedf[col_name_min].min() == destinationdf[col_name_min].min()):
          min = "MIN VALUE MATCH"
          print(min)
        else:
          min = "MIN VALUE NOT MATCH"
          print(min)
      except:  
        min = "Column Name Not Match For Min Condition"
        print(min)      

      try:
        col_name_max = parser.get('SOURCE', 'col_name_max')
        if(sourcedf[col_name_max].max() == destinationdf[col_name_max].max()):
          max = "MAX VALUE MATCH"
          print(max)
        else:
          max = "MAX VALUE NOT MATCH"
          print(max)
      except: 
        max = "Column Name Not Match For Max Condition"
        print(max)

      try:
        col_name_sum = parser.get('SOURCE', 'col_name_sum')  
        if(sourcedf[col_name_sum].astype(int).sum() == destinationdf[col_name_sum].astype(int).sum()):
          sum = "SUM VALUE MATCH"
          print(sum)
        else:
          sum = "SUM VALUE NOT MATCH"
          print(sum)
      except: 
        sum = "Column Name Not Match For Sum Condition"
        print(sum)

      try:        
        col_name_avg = parser.get('SOURCE', 'col_name_avg')  
        if(sourcedf[col_name_avg].astype(int).mean() == destinationdf[col_name_avg].astype(int).mean()):
          avg = "Average VALUE MATCH"
          print(avg)
        else:
          avg = "Average VALUE NOT MATCH"
          print(avg)
      except: 
        avg = "Column Name Not Match For Average Condition"        
        print(avg)
      
      
      fileName = "Report_" + subject + "_" + date + ".csv"

    # with open('Report_Data_Validation_20230219_035750', 'w', newline='') as file:
    #  writer = csv.writer(file)
     
    #  writer.writerow(["Source Value : "])
     
      source_error_df.to_csv(reportOutputDir + fileName, index=False)
      destination_error_df.to_csv(reportOutputDir + fileName, index=False)

      fileName = "Report_" + subject + "_" + date + ".html" 

      html1 = source_error_df.to_html()
      html2 = destination_error_df.to_html()
      html3 = compare.to_html()
      html = f"<html><body><h4>Source DF Value Not Match : </h4><br><table>{html1}</table><br><h4>Destination DF Value Not Match : </h4><br><table>{html2}</table><br><h4>Compare of Random Data(10%) Result : </h4><br><table>{html3}</table><br><h4>Validations : </h4><h4>1 - {min}</h4><h4>2 - {max}</h4><h4>3 - {sum}</h4><h4>4 - {avg}</h4></body></html>"
      filePath =  reportOutputDir
      text_file = open(filePath + fileName, "w")
      text_file.write(html)
      text_file.close()

    else:
      print("DataFrame Row Count Not Match")
      print("COUNT OF SOURCE DATAFRAME : ",source_row)
      print("COUNT OF DESTINATION DATAFRAME : ",destination_rows)

   
  

DoubleDataValidation()