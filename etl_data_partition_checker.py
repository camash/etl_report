#!/usr/env python
# -*- coding: UTF-8 -*-

import argparse
import configparser
import csv
from pyhive import hive
from TCLIService.ttypes import TOperationState

def get_partition_count(tabName):
    """ 获取表分区的个数
    :param dnName: 字符串类型，数据库名称
    :param tabName: 字符串类型，表名称
    返回值为数值型
    """
    # Setup hive connection and execute query
    conn = hive.Connection(host=hive_host, port=hive_port, username=hive_user)
    cursor = conn.cursor()
    cursor.execute("use " + dbName)
    cursor.execute("show partitions " + tabName, async=True)
    
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
    rowCount = len(columnList)
    
    return rowCount 
    

##############################################################
# Input the database name in Hive,
# the program will iterate all 
# tables' partitions count in
# that database.
# Output the result to a csv file,
# with | as delimeter. 
##############################################################
if __name__ == '__main__':
    # Setup command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument('database_name', help='the database name where table created.')
    #parser.add_argument('table_name', help='the name of table to be checked.')

    # Parse arguments.
    args = parser.parse_args()
    dbName = args.database_name
    #tabName = args.table_name

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

    fileName = output_path + dbName + "_partition_count.csv"
    
    with open(fileName, 'w') as csvout:
        csv_writer = csv.writer(csvout, delimiter=',', quoting=csv.QUOTE_NONE)
        # iterate all the tables to get 
        for i in columnList:
            tabName = i[0]
            print(tabName)
            if not (tabName.startswith("tmp") or tabName.startswith("dim") or "_td_" in tabName):
                pCount = get_partition_count(tabName)
                print(pCount)
                csv_writer.writerow([tabName,pCount])

