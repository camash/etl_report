#!/bin/bash

# enable when using crontab
cd /home/hadoop/shenf/02_etl_report 

# get dir from config file
work_dir=`grep "work" connection.cfg | cut -d= -f2`

exe_date=`date -d "$1" +%Y%m%d`
exe_date_dash=`date -d "$1" +%Y-%m-%d`

output_dir=`grep "output" connection.cfg | cut -d= -f2`

# get ETL time and disk usage
bash "${work_dir}get_execute_info.sh" "${exe_date}"

# get database name from config file
db_names=`grep "databases" connection.cfg | cut -d= -f2`

# get python bin path from config file
python_dir=`grep "python_bin" connection.cfg | cut -d= -f2`

echo $db_names | tr ',' '\n' | while read i
do
    "${python_dir}python" "${work_dir}etl_data_count_checker.py" ${i} ${exe_date}
    "${python_dir}python" "${work_dir}etl_data_partition_checker.py" ${i}
done

# Generate html report
"${python_dir}python" "${work_dir}report_generator.py" ${exe_date}

# SCP the html file to web container
web_user=`grep "web_user" connection.cfg | cut -d= -f2`
web_server=`grep "web_server" connection.cfg | cut -d= -f2`
web_dir=`grep "web_dir" connection.cfg | cut -d= -f2`

scp "${output_dir}${exe_date_dash}.html" "${web_user}@${web_server}:${web_dir}"

# backup today's report
mkdir -p "${output_dir}${exe_date}"

find "${output_dir}" -maxdepth 1 -type f -newermt "${exe_date_dash}" -exec mv {} "${output_dir}${exe_date}" \;
