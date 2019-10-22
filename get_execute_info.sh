#!/bin/bash

# get log file location
k_folder=`grep "kettle_log" connection.cfg | cut -d= -f2`
s_folder=`grep "shell_log"  connection.cfg | cut -d= -f2`
output_file=`grep "usage_csv"  connection.cfg | cut -d= -f2`
hdfs_dir=`grep "hdfs_dir"  connection.cfg | cut -d= -f2`

echo ${k_folder}
echo ${s_folder}

# get date from command line input
if [ ! -n "$1" ] ;then
    echo "please input running date! The format is yyyyMMdd"
    exit 1
fi
ETL_DATE=`date -d "$1" +%Y%m%d`

# get start time and end time of logs
START_TIME=`ls -ltr ${k_folder}*${ETL_DATE}*log | head -1 | awk '{print $8}'`
END_TIME=`ls -lt  ${s_folder}*${ETL_DATE}*log | head -1 |  awk '{print $8}'`

# get hdfs cluster volume
HDFS_VOLUME=`hdfs dfs -du -s -h  "${hdfs_dir}" | awk '{print $3 $4}'`

# get the size of logs
KETLL_LOG_VOLUME=`du -cksh ${k_folder} | head -1 | awk '{print $1}'`
SHELL_LOG_VOLUME=`du -cksh ${s_folder} | head -1 |awk '{print $1}'`

# output results to file using csv format
printf "%s,%s,%s,%s\n" ${START_TIME} ${END_TIME} ${HDFS_VOLUME} "${KETLL_LOG_VOLUME} + ${SHELL_LOG_VOLUME}" > "${output_file}"
