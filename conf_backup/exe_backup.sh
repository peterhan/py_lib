#!/bin/bash
source ~/.bash_profile
dt=`date +%Y%m%d`
echo $dt
#exit
python idx_backup.py  > /dev/null

cd ./file &&
for fname in $( ls *small*.sh )
do
  echo $fname 
  sh $fname >  /dev/null
done 


hadoop fs -put *.tar /ps/ubs/hanshu/oppd_ta_2-10/small_file/  &&
rm *.tar &&
rm *.sh
