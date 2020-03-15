#!/usr/env python
# -*- coding: UTF-8 -*-

import argparse
import configparser
import csv
import sys
import datetime
import mysql.connector
import etl_checker_toolbox as tb


def db_write_count(tabName, queryDate):
    """ query the count from specific partition from a table
    :param tablName: table name
    :param queryDate: equals the partition value of value, the format is YYYYMMDD
    """

    # check date format is yyyymmdd
    tb.date_validate(queryDate)
    
    # get dbName
        
    dbName = tb.get_dbname_from_tab(tabName)
    if dbName == "":
        sys.exit("format of table: " + tabName + " is invalid.")


    # get row count
    rowCount = tb.get_row_count(dbName, tabName, queryDate)

    if rowCount > 0:
       executeStatus = "true"
    else:
       executeStatus = "false"

    # Read Log File location and Regular Expression
    config = configparser.ConfigParser()
    config.read('connection.cfg')


    ## Read mysql connection
    mysqlConn = config['mysql']
    
    dbHost = mysqlConn['db_host']
    dbUser = mysqlConn['db_user']
    dbPass = mysqlConn['db_pass']
    dbName = mysqlConn['db_name']


    mydb = mysql.connector.connect(
      host=dbHost,
      user=dbUser,
      passwd=dbPass,
      database=dbName
    )
    
    mycursor = mydb.cursor()
    
    sql = '''insert into etl_tab_rows(item,partition_name,row_count,log_date)
           values (%s,%s,%s,%s)
           on DUPLICATE KEY UPDATE row_count=%s
           ;'''

    val = (tabName, queryDate, rowCount, queryDate, rowCount)

    mycursor.execute(sql, val)
    
    mydb.commit()
    
    print(mycursor.rowcount, "record inserted.")


# main function start
if __name__ == '__main__':
    # Setup command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument('table_name', help='the name of table to be checked')
    parser.add_argument('query_date', help='the partition value of table with format YYYYMMDD')

    # get current date
    # currentDate = datetime.datetime.today().strftime('%Y%m%d')

    # Parse arguments.
    args = parser.parse_args()
    tabName   = args.table_name
    queryDate = args.query_date

    db_write_count(tabName, queryDate)
