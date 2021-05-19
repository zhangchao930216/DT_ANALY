LOAD DATA
characterset UTF8
APPEND INTO TABLE lte_mro_overlap_b_ana60
FIELDS TERMINATED BY ','
trailing nullcols
(
STARTTIME date "yyyy-mm-dd hh24:mi:ss",
ENDTIME date "yyyy-mm-dd hh24:mi:ss",
TIMESEQ,
ENODEBID,
CELLID,
USERCOUNT,
RSRPSUM,
RSRPCOUNT,
RSRPAVG,
OVERLAPBESTROWCELLCOUNT,
ADJACENTAREAINTERFERENCEINTENS,
ADJACENTAREAINTERFERENCEINDEX,
CELLOVERLAPBESTROWRATIO,
CELLOVERLAPBESTROWMRCOUNT,
RSRQSUM,
RSRQCOUNT,
RSRQAVG
)