#!/bin/env python
# -*- coding: UTF-8 -*-

from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import datetime
import csv
import argparse
import configparser

def csv_to_dictlist(csv_file,header):
    """
    Read csv file, then return dist list.
    If file not found, still return blank
    list.
    """
    dict_list = []
    try:
        with open(csv_file) as csvfile:
            reader=csv.DictReader(csvfile, fieldnames=header)
            for row in reader:
                dict_list.append(row)
    except IOError:
        print("Cannot open " + csv_file)
    return dict_list


if __name__ == '__main__':
    # Setup command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument('partition_date', help='the value of table partition, the format must be YYYYMMDD.')

    # Parse arguments.
    args = parser.parse_args()
    partDate = args.partition_date

    # Read path info from config file
    config = configparser.ConfigParser()
    config.read('connection.cfg')

    path_info = config['path']
    usage_csv = path_info['usage_csv']
    template_path = path_info['template']
    output_path  = path_info['output']

    # Get the check db names from config file
    check_obj = config['check_object']
    db_name = check_obj['databases']

    db_name_list = db_name.split(',')

    # Create item_list with additional suffix
    item_list = []
    for suff in ['_row', '_partition']:
        for db in db_name_list:
            item_list.append(db + suff)

    # init a template loader
    loader = FileSystemLoader(template_path)

    # init a eniroment with loader
    env = Environment(loader=loader)

    # load file to template object
    template = env.get_template('index2.html')

    current_date = datetime.datetime.strptime(partDate, '%Y%m%d').strftime('%Y-%m-%d')

    # using check item to iterate csv file, put result in a dictionary list 
    item_list_2 = []
    result = []
    for item in item_list:
        item_list_2.append(item)
        result_file = output_path + item + "_count.csv"
        csv_header = ['tab', 'ccnt', 'pcnt', 'change']
        #print(result_file,csv_header)
        temp_list = csv_to_dictlist(result_file, csv_header)
        #print(temp_list)
        result.append(temp_list)

    ####################################################################################
    # result example: dictionary list in 
    #result = [[{'tab': 'vip_info', 'ccnt':100, 'pcnt': 80, 'change': '30%'}
    #          ,{'tab': 'order_detail', 'ccnt':200, 'pcnt': 150, 'change': '40%'}],
    #          [{'tab':'td_marriage', 'ccnt':300, 'pcnt':300, 'change': '0%'}]
    #         ]
    #print(result)
    ###################################################################################

    # get the time/volume check result to dicitionary list
    usage_header = ['start', 'end', 'hdfs', 'log']
    usage_list = csv_to_dictlist(usage_csv, usage_header)
    #print(usage_list[0].get('start'))

    with open(output_path + current_date + ".html", "w") as f:
        f.write(template.render(etl_date=current_date,
        etl_start_time=usage_list[0].get('start'),
        etl_end_time=usage_list[0].get('end'),
        hdfs_volume=usage_list[0].get('hdfs'),
        log_folder_volume=usage_list[0].get('log'),
        databases=item_list_2,
        result=result))

