LOAD DATA
characterset UTF8
APPEND INTO TABLE grid_ltemrkpi60
FIELDS TERMINATED BY ','
trailing nullcols
(
BEGINTIME date "yyyy-mm-dd hh24:mi:ss",
ENDTIME date "yyyy-mm-dd hh24:mi:ss",
TIMESEQ,
ENODEBID,
CELLID,
GRIDCENTERLONGITUDE,
GRIDCENTERLATITUDE,
OID,
KPI1049,
KPI1239,
KPI1011,
KPI1012,
KPI1241,
KPI1243,
KPI1245,
KPI1247
)