#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
DATABASE=$2
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db

echo "sh VolumeAnalyseHDFS2db.sh ${DIR}/volte_gt_user_ana_baseday/dt=${ANALY_DATE}  40 2"
sh VolumeAnalyseHDFS2db.sh ${DIR}/volte_gt_user_ana_baseday/dt=${ANALY_DATE}  40 2 &

echo "sh VolumeAnalyseHDFS2db.sh ${DIR}/volte_gt_cell_ana_baseday/dt=${ANALY_DATE} 41 2"
sh VolumeAnalyseHDFS2db.sh ${DIR}/volte_gt_cell_ana_baseday/dt=${ANALY_DATE} 41 2 &

echo "sh VolumeAnalyseHDFS2db.sh ${DIR}/mr_gt_user_ana_baseday/dt=${ANALY_DATE} 42 2"
sh VolumeAnalyseHDFS2db.sh ${DIR}/mr_gt_user_ana_baseday/dt=${ANALY_DATE} 42 2 &

echo "sh VolumeAnalyseHDFS2db.sh ${DIR}/mr_gt_cell_ana_baseday/dt=${ANALY_DATE}  43 2"
sh VolumeAnalyseHDFS2db.sh ${DIR}/mr_gt_cell_ana_baseday/dt=${ANALY_DATE}  43 2 &

sh VolumeAnalyseHDFS2db.sh ${DIR}/tac_day_http/dt=${ANALY_DATE} 13 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/cell_day_http/dt=${ANALY_DATE} 15 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/sp_day_http/dt=${ANALY_DATE} 17 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/ue_day_http/dt=${ANALY_DATE} 19 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/imsi_cell_day_http/dt=${ANALY_DATE} 21 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/sgw_day_http/dt=${ANALY_DATE} 23 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/mr_gt_grid_ana_baseday/dt=${ANALY_DATE} 49 2
