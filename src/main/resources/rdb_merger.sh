#!/bin/bash

echo "loading....."
redise_pwd="/data/redis_temp/merger/"
master_rdb="${redise_pwd}$1"
slave_rdb="${redise_pwd}$2"
#redis-cli  shutdown

tail -n 1 ${master_rdb} >${redise_pwd}master.txt
head -n 1 ${slave_rdb} >${redise_pwd}slave.txt

python ./binary_change.py ${redise_pwd}master.txt ${redise_pwd}slave.txt
echo >>${redise_pwd}master.txt

sed -i "\$d" ${master_rdb} 
cat ${redise_pwd}master.txt >> ${master_rdb}
#rm -rf ${redise_pwd}master.txt

sed -i "1d" ${slave_rdb} && cat ${slave_rdb}>>${redise_pwd}slave.txt
cat ${redise_pwd}slave.txt>>${master_rdb}

#rm -rf ${slave_rdb}
#rm -rf ${redise_pwd}slave.txt
echo "finsh!!"
