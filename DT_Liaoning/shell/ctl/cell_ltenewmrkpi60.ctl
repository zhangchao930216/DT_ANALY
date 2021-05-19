LOAD DATA
characterset UTF8
APPEND INTO TABLE cell_ltenewmrkpi60
FIELDS TERMINATED BY ','
trailing nullcols
(
STARTTIME date "yyyy-mm-dd hh24:mi:ss",
ENDTIME date "yyyy-mm-dd hh24:mi:ss",
TIMESEQ,
MMEGROUPID,
MMEID,
ENODEBID,
CELLID,
MROVERLAYCOUNT,
MROVERCOVERCOUNT,
MRLOSENEIBCOUNT,
MREDGEWEAKCOVERCOUNT
)