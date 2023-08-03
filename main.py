#Libraries
import requests
import os
import pandas as pd
import json
import datetime
from datetime import datetime
import sys
import pymysql
import config

#Paths
dir_sep = os.path.sep
PATH_PROJECT=os.getcwd() 
PATH_DATA=PATH_PROJECT+dir_sep+"data"
PATH_LOG=PATH_PROJECT+dir_sep+"log"


#Log settings
proccess_date = str(datetime.today().strftime('%Y%m%d%H%M%S'))
proccess_name = "bitcoin_price"
log_file = os.path.join(PATH_LOG,"" ,"log_" + proccess_name+".txt")



# --- FUNCTIONS ---

## function logger
def logger(log_file,msg,status):
        """recieve:
            -log_file
            -msg: message to write in the log_file
            -status: massage category
           return:
            -write message in the log_file 
        """
       
        if status == 0:
           status_flag= "[INFO]"
        elif status == 1:
           status_flag = "[WRN]"
        elif status == 2:
           status_flag = "[ERR]"
       
        message = str( str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')) +" - "+ str(status_flag)+ " - ") + msg + "\n"
        with open(log_file, "a") as file:
            file.write(message)
            print(message)

## function get_data
def get_data(url, file_name, level=None):
    logger(log_file, "Start request to Cryptocurrency API. ", 0)
    try:
        response = requests.get(url)
        if response.status_code == 200:
            if response.content:
                data = response.json()

                if level is not None:
                    for key in level:
                        if key in data:
                            data = data[key]
                        else:
                            logger(log_file, f"The key '{key}' is not present in the data.", 1)
                            return None

                json_data = json.dumps(data)
                with open(os.path.join(PATH_DATA, file_name), 'w') as file:
                    file.write(json_data)
                logger(log_file, "The data was correctly extracted", 0)
                df = pd.read_json(os.path.join(PATH_DATA, file_name))
                return df
            else:
                logger(log_file, "The Cryptocurrency API doesn't content data", 0)
                logger(log_file, "The process finishes with ERROR.", 2)
                sys.exit(1)
    except requests.exceptions.RequestException as e:
        logger(log_file, "An ERROR has occurred consulting the data", 2)
        logger(log_file, f"Error: {e}", 2)


## function conecction 
def create_database(query):
    try:        
        conn = pymysql.connect(host=rds_host,
                              user=user,
                              passwd=password,
                              connect_timeout=20)
        try:
            cursor=conn.cursor()
            cursor.execute(query)
            conn.commit()
            conn.close()
            logger(log_file ,msg="The database was created suscessfully.",status= 0)
        except pymysql.err.OperationalError as e:
            logger(log_file ,msg="An ERROR occured trying to create the table.",status= 2)
            logger(log_file ,msg="Query:\n {}".format(query),status= 2)
            logger(log_file ,msg="{}".format(e),status= 2)
            logger(log_file ,msg="The program finished with error.",status= 2)
            sys.exit(1)    

    except pymysql.MySQLError as e:       
      logger(log_file ,msg="An ERROR occured trying to connect to MySQL.",status= 2)
      logger(log_file ,msg="{}".format(e),status= 2)
      logger(log_file ,msg="The program finished with error.",status= 2)
      sys.exit(1)   

## function create table
def create_table(query):
    try:        
        conn = pymysql.connect(host=rds_host,
                              user=user,
                              passwd=password,
                              connect_timeout=20)
        try:  
            cursor=conn.cursor()
            cursor.execute(query)
            conn.commit()
            conn.close()
            logger(log_file ,msg="The table was created suscessfully.",status= 0)
        except pymysql.err.OperationalError as e:
            logger(log_file ,msg="An ERROR occured trying to create the table.",status= 2)
            logger(log_file ,msg="Query:\n {}".format(query),status= 2)
            logger(log_file ,msg="{}".format(e),status= 2)
            logger(log_file ,msg="The program finished with error.",status= 2)
            sys.exit(1)         
    except pymysql.MySQLError as e:       
      logger(log_file ,msg="An ERROR occured trying to connect to MySQL.",status= 2)
      logger(log_file ,msg="{}".format(e),status= 2)
      logger(log_file ,msg="The program finished with error.",status= 2)
      sys.exit(1) 

## function insert data
def insert_data(query, data, amount=None):
    try:        
        conn = pymysql.connect(host=rds_host,
                              user=user,
                              passwd=password,
                              connect_timeout=20)
        try:  
            cursor = conn.cursor()
            if amount is None:
                cursor.execute(query,data)
            elif amount == "many":
                cursor.executemany(query, data)
            conn.commit()
            conn.close()
            logger(log_file ,msg="The data was inserted suscessfully.",status= 0)
        except pymysql.err.OperationalError as e:
            logger(log_file ,msg="An ERROR occured trying insert the data..",status= 2)
            logger(log_file ,msg="\n",status= 2)
            logger(log_file ,msg="Query:\n{}".format(query),status= 2)
            logger(log_file ,msg="{}".format(e),status= 2)
            logger(log_file ,msg="The program finished with error.",status= 2)
            sys.exit(1)    

    except pymysql.MySQLError as e:       
        logger(log_file ,msg="An ERROR occured trying to connect to MySQL.",status= 2)
        logger(log_file ,msg="{}".format(e),status= 2)
        logger(log_file ,msg="The program finished with error.",status= 2)
        sys.exit(1)  

## function to consult data
def consult_data_db(query):
    try:        
        conn = pymysql.connect(host=rds_host,
                              user=user,
                              passwd=password,
                              connect_timeout=20)
        cursor = conn.cursor()
        try:
           cursor.execute(query)
           data = cursor.fetchall()
           conn.close()
           logger(log_file ,msg="The data was extracted suscessfully.",status = 0)
           return data 
        except pymysql.err.OperationalError as e:
            logger(log_file ,msg="An ERROR occured trying extract the data.",status = 2)
            logger(log_file ,msg="\n",status= 2)
            logger(log_file ,msg="Query:\n{}".format(query),status= 2)
            logger(log_file ,msg="{}".format(e),status= 2)   
            logger(log_file ,msg="The program finished with error.",status= 2)
            sys.exit(1)     
    except pymysql.MySQLError as e:       
        logger(log_file ,msg="An ERROR occured trying to connect to MySQL.",status= 2)
        logger(log_file ,msg="{}".format(e),status= 2)
        logger(log_file ,msg="The program finished with error.",status= 2)
        sys.exit(1)   


if __name__=="__main__":

    #Database Credentials
    rds_host = config.rds_host
    user = config.user 
    password = config.password

    #Endpoints
    ## - url list all coins id, symbol, name
    coins_url = "https://api.coingecko.com/api/v3/coins/list"
    #File name - save data
    file_name = 'coins_list_data.json'
    df_list_currency = get_data(coins_url,file_name)
    #Get bitcoin identifier
    bitcoin_id = df_list_currency.loc[df_list_currency["id"] == "bitcoin","id"].to_list()[0]



    #Set date range for first quarter 2022 
    start_date="2022-01-01"
    end_date="2022-03-31"
    start_date_timestamp=int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_date_timestamp=int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

    #Currency of request
    currency="usd"
    ## - url bitcoin history price
    url_btc_history_price="https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range?vs_currency={currency}&from={start_date}&to={end_date}".format(coin=bitcoin_id, currency=currency, start_date=start_date_timestamp,end_date=end_date_timestamp)
    #File name - save data
    file_name ='bitcoin_price_data.json'
    #Get bitcoin history prices
    df_bitcoin_history_price=get_data(url_btc_history_price,file_name,level=['prices'])
    df_bitcoin_history_price.rename(columns={0:"datetime",1:"priceUSD"},inplace=True)
    df_bitcoin_history_price['datetime']=pd.to_datetime(df_bitcoin_history_price['datetime'], unit='ms')
    df_bitcoin_history_price["priceUSD"]=df_bitcoin_history_price["priceUSD"].astype(float)

    #Database name / Table name
    db_name="test"
    table_name="priceBitcoinFirstQuater2022"

    #Create database
    query_create_db = f"""CREATE DATABASE IF NOT EXISTS {db_name};"""        
    create_database(query_create_db)

    #Create table
    query_create_table = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name}(
                                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                                datetime DATETIME NOT NULL, 
                                priceUSD DECIMAL(10, 2) NOT NULL
                                );"""
    create_table(query_create_table)

    #Insert bitcoin history data
    data_insert_bitcoin_price = list(zip(df_bitcoin_history_price["datetime"], df_bitcoin_history_price["priceUSD"]))

    query_insert_bitcoin_price= f"INSERT INTO {db_name}.{table_name} (datetime, priceUSD) VALUES (%s, %s);"
    insert_data(query_insert_bitcoin_price, data_insert_bitcoin_price, amount = "many")


    #Extract all data from table test.priceBitcoinFirstQuater2022
    query_get_btc_price_usd_fq=f"""SELECT *
                                   FROM {db_name}.{table_name};"""        
    data_btc_price_usd_fq = consult_data_db(query_get_btc_price_usd_fq)

    #Create dataframe from bitcoin history data
    df_btc_history = pd.DataFrame(data= data_btc_price_usd_fq, columns= ["id","datetime","priceUSD"])


    #Create table
    table_name_price_mean_5_d = "priceBitcoinFirstQuater2022_mean_5_d"
    
    query_create_table_price_mean_5_d = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name_price_mean_5_d}(
                                            id INTEGER PRIMARY KEY AUTO_INCREMENT,
                                            datetime DATETIME NOT NULL, 
                                            priceUSD DECIMAL(10, 2) NOT NULL,
                                            price_mean_5_days DECIMAL(10, 2) NOT NULL
                                            );"""
    
    create_table(query_create_table_price_mean_5_d)
        
    #Create window function
    df_btc_history["priceUSD"] = df_btc_history["priceUSD"].astype(float)
    df_btc_history['price_mean_every_five_days'] = df_btc_history['priceUSD'].rolling(window=5).mean()
    query_insert_bitcoin_price_mean_5_d= f"INSERT INTO {db_name}.{table_name} (datetime, priceUSD, price_mean_5_days ) VALUES (%s, %s, %s);"

    #Notworking Insert date from window function into db

    #insert_data(query_insert_bitcoin_price_mean_5_d,zip(df_btc_history["datetime"],df_btc_history["priceUSD"],df_btc_history['price_mean_every_five_days']), amount = "many")
   




