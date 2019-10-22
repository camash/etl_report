#!/usr/env python
# -*- coding: UTF-8 -*-

import argparse
import configparser
import csv
import datetime
from pyhive import hive
from TCLIService.ttypes import TOperationState

def get_row_count(tabName,partDate):
    """ 获取表分区的个数
    :param tabName: 字符串类型，表名称
    :param partDate: 字符型变量，分区日期
    返回值为数值型
    """
    # Assemble query condition based on partition date
    if "_chg" in dbName:
        tmpDate = datetime.datetime.strptime(partDate, '%Y%m%d').strftime('%Y-%m-%d')
        queryCon = " where process_date='" + tmpDate + "'"
    else:
        queryCon =  " where etl_date='" + partDate + "'"

    # Setup hive connection and execute query
    conn = hive.Connection(host=hive_host, port=hive_port, username=hive_user)
    cursor = conn.cursor()
    cursor.execute("select count(1) from " + dbName + "." + tabName + queryCon, async=True)
    
    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        for message in logs:
            print(message)
    
        # If needed, an asynchronous query can be cancelled at any time with:
        # cursor.cancel()
    
        status = cursor.poll().operationState
    
    # Fetch the sql execution result
    columnList = cursor.fetchall()
    #print(columnList)
    rowCount = columnList[0][0]
    
    return rowCount
    
def validate(date_text):
    """ 检查日期格式是否正确
    :date_text: 字符串，日期
    """
    try:
        datetime.datetime.strptime(date_text, '%Y%m%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYYMMDD")

def compute_increse(preCount, currCount):
    """ 获得数据增长的比率
    :preCount: 数字，昨天的数量
    :currCount: 数字，今天分区的数量
    """
    percentage = "0%"
    if preCount <= 0:
        percentage = "init"
    else:
        temp = (currCount - preCount)/preCount
        percentage = "{:.2%}".format(temp)

    return percentage

##############################################################
# Input the database name in Hive
# and partition date,
# the program will iterate all 
# tables' partitions count in
# that database.
# Output the result to a csv file,
# with , as delimeter. 
##############################################################
if __name__ == '__main__':
    # Setup command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument('database_name', help='the database name where table created.')
    parser.add_argument('partition_date', help='the value of table partition, the format must be YYYYMMDD.')

    # get current date
    # currentDate = datetime.datetime.today().strftime('%Y%m%d')

    # Parse arguments.
    args = parser.parse_args()
    dbName = args.database_name
    partDate = args.partition_date

    # check the date format
    #validate(partDate)
    
    # get previous date 
    previousDate = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')

    # Read connection info from config file
    config = configparser.ConfigParser()
    config.read('connection.cfg')
    
    hive_conn = config['hive']
    hive_host = hive_conn['host']
    hive_port = int(hive_conn['port'])
    hive_user = hive_conn['user']
    
    path_cfg = config['path']
    output_path = path_cfg['output']

    # Setup hive connection and execute query
    conn = hive.Connection(host=hive_host, port=hive_port, username=hive_user)
    cursor = conn.cursor()
    cursor.execute("use " + dbName)
    cursor.execute("show tables", async=True)
    
    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        for message in logs:
            print(message)
    
        # If needed, an asynchronous query can be cancelled at any time with:
        # cursor.cancel()
    
        status = cursor.poll().operationState
    
    # Fetch the sql execution result
    columnList = cursor.fetchall()
    #print(columnList)
    #print(len(columnList))

    fileName = output_path + dbName + "_row_count.csv"
    
    with open(fileName, 'w') as csvout:
        csv_writer = csv.writer(csvout, delimiter=',', quoting=csv.QUOTE_NONE)
        # iterate all the tables to get 
        for i in columnList:
            tabName = i[0]
            #print(tabName)
            if not (tabName.startswith("tmp") or tabName.startswith("dim") or "_td_" in tabName):
                pCount = get_row_count(tabName,partDate)
                ppCount = get_row_count(tabName,previousDate)
                pCent = compute_increse(ppCount, pCount)
                #print(pCount)
                csv_writer.writerow([tabName,pCount,ppCount,pCent])

