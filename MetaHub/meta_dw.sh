#!/bin/sh

LOG_BASIC_PATH=/var/lib/hadoop-hdfs/huangxuanfeng/my_warehouse/metahub/metahub-logs

if [ -d "$LOG_BASIC_PATH" ];then
  echo "日志输出文件夹存在"
else
  echo "日志输出文件夹被删除，新创建一下"
  mkdir -p $LOG_BASIC_PATH
fi

echo "online环境数据开始..."
echo "keep_ods库开始..."
python meta_dw.py online keep_ods $LOG_BASIC_PATH
echo "keep_dw库开始..."
python meta_dw.py online keep_dw $LOG_BASIC_PATH
echo "keep_dm_ad库开始..."
python meta_dw.py online keep_dm_ad $LOG_BASIC_PATH
echo "keep_dm_fd库开始..."
python meta_dw.py online keep_dm_fd $LOG_BASIC_PATH
echo "keep_dm_gl库开始..."
python meta_dw.py online keep_dm_gl $LOG_BASIC_PATH
echo "keep_dm_kit库开始..."
python meta_dw.py online keep_dm_kit $LOG_BASIC_PATH
echo "keep_dm_kl库开始..."
python meta_dw.py online keep_dm_kl $LOG_BASIC_PATH
echo "keep_dm_mo库开始..."
python meta_dw.py online keep_dm_mo $LOG_BASIC_PATH
echo "keep_dm_prime库开始..."
python meta_dw.py online keep_dm_prime $LOG_BASIC_PATH
echo "keep_dm_rt库开始..."
python meta_dw.py online keep_dm_rt $LOG_BASIC_PATH
echo "keep_dm_su库开始..."
python meta_dw.py online keep_dm_su $LOG_BASIC_PATH
echo "keep_dm_tc库开始..."
python meta_dw.py online keep_dm_tc $LOG_BASIC_PATH
echo "keep_dm_ug库开始..."
python meta_dw.py online keep_dm_ug $LOG_BASIC_PATH
echo "keep_dm_up库开始..."
python meta_dw.py online keep_dm_up $LOG_BASIC_PATH
echo "keep_dm_user库开始..."
python meta_dw.py online keep_dm_user $LOG_BASIC_PATH


echo "pre环境数据开始"



