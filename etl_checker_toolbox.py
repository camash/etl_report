#!/usr/env python
# -*- coding: UTF-8 -*-

import datetime
import configparser
from pyhive import hive
from TCLIService.ttypes import TOperationState



def date_validate(dateText):
    """ 检查日期格式是否正确
    :dateText: date format string
    """
    try:
        datetime.datetime.strptime(dateText, '%Y%m%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYYMMDD")


def compute_time_cost(startTime, endTime):
    """ compute the time cost between 2 input time
    :startTime: time of log start
    :endTime: time of log end
    """

    try:
        startTimeObj = datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S')
        endTimeObj = datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MI:SS")
    
    timeCost = endTimeObj -  startTimeObj 
    duration = timeCost.total_seconds()

    return duration


def get_dbname_from_tab(tabName):
    dbName = ""
    if tabName.startswith( "ods_" ):
        dbName = "xxx_ods"
    elif tabName.startswith( "sga_" ):
        dbName = "xxx_sga"
    elif tabName.startswith( "mid_" ):
        dbName = "xxx_mid"
    elif tabName.startswith( "dw_" ):
        dbName = "xxx_dw"
    elif tabName.startswith( "chg_" ):
        dbName = "xxx_chg"
    elif tabName.startswith( "c1" ):
        dbName = "xxx"


    return dbName


def get_row_count(dbName,tabName,partDate):
    """ 获取表分区的个数
    :param dbName: 字符串类型，schema名称
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
