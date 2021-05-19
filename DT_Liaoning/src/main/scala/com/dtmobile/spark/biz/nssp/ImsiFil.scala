package com.dtmobile.spark.biz.nssp

import org.apache.spark.sql.{SaveMode}


/**
  * NsspAnaly
  *x
  *
  * @author heyongjin
  * @ create 2017/03/02 10:36
  *
  **/
class ImsiFil(MONTH:String,DAY:String,CITY:String) {
  /*def analyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    //原始表初始化



    sparkSession.udf.register("calcdis", (fLon:Float,fLat:Float,tLon:Float,tLat:Float)=>(2 * scala.math.asin(scala.math.sqrt(scala.math.pow(scala.math.sin((fLat*3.14159265/180 - tLat*3.14159265/180)/2),2) + scala.math.cos(fLat*3.14159265/180)*scala.math.cos(tLat*3.14159265/180)*scala.math.pow(scala.math.sin((fLon*3.14159265/180 - tLon*3.14159265/180)/2),2))))*6378.137)
    sql(
    """
      | use  result
    """.stripMargin)
    //rc_hive_db
    sql(
      s"""
         |insert overwrite table adjacent_area
         | select
         | abc.cellid,
         | abc.freq1,
         | abc.pci,
         | abc.dis,
         | abc.tcellid
         | from
         |(
         |select b.cellid,
         |       b.freq1,
         |       b.pci,
         |       b.tcellid,
         |       b.dis,
         |       ROW_NUMBER() OVER(partition by b.cellid,b.freq1,b.pci order by b.dis asc ) as ascdis
         |  from (select t.cellid as cellid,
         |               a.freq1,
         |               a.pci,
         |               calcdis(a.longitude, a.latitude, t.longitude, t.latitude) as dis,
         |               a.cellid as tcellid
         |          from ltecell t, ltecell a
         |         where t.cellid != a.cellid) b
         |         )abc
         |         where abc.ascdis = 1
         |
        """.stripMargin)
*/

  /*  sql(
      s"""
        | set hive.mapred.supports.subdirectories=true;
        | set mapreduce.input.fileinputformat.input.dir.recursive=true;
        | set mapred.max.split.size=256000000;
        | set mapred.min.split.size.per.node=128000000;
        | set mapred.min.split.size.per.rack=128000000;
        | set hive.hadoop.supports.splittable.combineinputformat=true;
        | set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
        | set hive.merge.mapfiles=true;
        | set hive.merge.mapredfiles=true;
        | set hive.merge.size.per.task=256000000;
        | set hive.merge.smallfiles.avgsize=256000000;
        | set hive.groupby.skewindata=true;
        | set hive.exec.dynamic.partition.mode=nonstrict;
        | set hive.exec.parallel=true;
        | set hive.exec.parallel.thread.number=32;
        | SET hive.exec.compress.output=true;
        | SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
        | SET mapred.output.compression.type=BLOCK;
        |
        |
        |alter table d_enl_mr_new_h add if not exists partition(p_hour=$MONTH,p_city=$DAY) location '/Datang/MOR/$MONTH/$DAY' ;
        |
        |
        |alter table d_enl_mr_new_h_imsi drop  if  exists  partition(p_hour=$DAY,p_city=$DAY) ;
        |
        |alter table d_ens_http_4g_h_city add  if not exists  partition(p_hour=$DAY,p_city=$CITY) location '/jc_rc/rc_hive_db/d_ens_http_4g_h_city/p_hour=$DAY/p_city=$CITY' ;
        |alter table d_ens_s1_mme_h_city  add  if not exists  partition(p_hour=$DAY,p_city=$CITY) location '/jc_rc/rc_hive_db/d_ens_s1_mme_h_city/p_hour=$DAY/p_city=$CITY' ;
        |alter table d_enl_mr_new_h_imsi set serdeproperties('serialization.null.format' = '') ;
        |
        |insert  into table d_enl_mr_new_h_imsi partition(p_hour=$DAY,p_city=$CITY)
        |SELECT
        |       concat('460-00-',cast(ltemro.enb_id as int),'-',cast(ltemro.object_id as int)) as a_cgi,
        |       concat('460-00-',cast(AA.tcellid/256 as int),'-',cast(AA.tcellid%256 as int)) as z_cgi,
        |       '' as distance,
        |       '' as adjacent,
        |       ltemro.mmecode,
        |       ltemro.mmegroupid,
        |       ltemro.mmeues1apid,
        |       ltemro.time_stamp,
        |       ltemro.object_id as id,
        |       ltemro.ltescrsrp - 141 as ltescrsrp,
        |       ltemro.ltencrsrp - 141 as ltencrsrp,
        |       ltemro.ltescrsrq * 0.5 - 20 as ltescrsrq,
        |       ltemro.ltencrsrq * 0.5 - 20 as ltencrsrq,
        |       ltemro.ltescearfcn,
        |       ltemro.ltescpci,
        |       ltemro.ltencearfcn,
        |       ltemro.ltencpci,
        |       ltemro.ltesctadv,
        |       ltemro.ltescphr - 23 as ltescphr,
        |       ltemro.ltescrip,
        |       ltemro.ltescaoa * 0.5 as ltescaoa,
        |       ltemro.ltescplrulqci1,
        |       ltemro.ltescplrulqci2,
        |       ltemro.ltescplrulqci3,
        |       ltemro.ltescplrulqci4,
        |       ltemro.ltescplrulqci5,
        |       ltemro.ltescplrulqci6,
        |       ltemro.ltescplrulqci7,
        |       ltemro.ltescplrulqci8,
        |       ltemro.ltescplrulqci9,
        |       ltemro.ltescplrdlqci1,
        |       ltemro.ltescplrdlqci2,
        |       ltemro.ltescplrdlqci3,
        |       ltemro.ltescplrdlqci4,
        |       ltemro.ltescplrdlqci5,
        |       ltemro.ltescplrdlqci6,
        |       ltemro.ltescplrdlqci7,
        |       ltemro.ltescplrdlqci8,
        |       ltemro.ltescplrdlqci9,
        |       ltemro.ltescsinrul - 11 as ltescsinrul,
        |       ltemro.ltescri1,
        |       ltemro.ltescri2,
        |       ltemro.ltescri4,
        |       ltemro.ltescri8,
        |       ltemro.ltescpuschprbnum,
        |       ltemro.ltescpdschprbnum,
        |       ltemro.ltescbsr,
        |       ltemro.ltescenbrxtxtimediff,
        |       ltemro.gsmncellbcch,
        |       ltemro.gsmncellcarrierrssi,
        |       ltemro.gsmncellncc,
        |       ltemro.gsmncellbcc,
        |       ltemro.tdspccpchrscp,
        |       ltemro.tdsncelluarfcn,
        |       ltemro.tdscellparameterid,
        |       ltemro.imsi,
        |       ltemro.msisdn,
        |       ltemro.objectid,
        |       AA.tcellid as nobjectid,
        |       ltemro.mrtime
        | FROM
        |  (
        |    SELECT
        |       lte.*,
        |       s1.imsi,
        |       s1.msisdn,
        |      (cast(enb_id as int )*256+cast(object_id as int)) as objectid,
        |      (cast ((unix_timestamp(time_stamp,"yyyy-DD-mm HH:mm:ss.S")*1000) as bigint)) + if (cast ((split(time_stamp,'\\.')[1]) as bigint)>0,(cast ((split(time_stamp,'\\.')[1]) as bigint)),0) as mrtime
        |  FROM d_enl_mr_new_h lte
        |  left outer join (SELECT mmeues1apid,
        |                          mmegroupid,
        |                          mmecode,
        |                          imsi,
        |                          IMEI,
        |                          MSISDN,
        |                          ROW_NUMBER() OVER(PARTITION BY mmeues1apid, mmegroupid, mmecode ORDER BY abs(unix_timestamp(p.procedure_start_time, "yyyy-DD-mm HH:mm:ss.S") - unix_timestamp(p.procedure_start_time, "yyyy-DD-mm HH:mm:ss.S")) asc) rum
        |                     from (select imsi,
        |                                  IMEI,
        |                                  MSISDN,
        |                                  lte.mmeues1apid,
        |                                  lte.mmegroupid,
        |                                  lte.mmecode,
        |                                  lte.procedure_start_time
        |                             FROM (select imsi,
        |                                          IMEI,
        |                                          MSISDN,
        |                                          mme_ue_s1ap_id       mmeues1apid,
        |                                          mme_group_id         mmegroupid,
        |                                          mme_code             mmecode,
        |                                          procedure_start_time,
        |                                          p_hour
        |                                     from d_ens_s1_mme_h_city
        |                                    where p_hour = $DAY
        |                                      and p_city = $CITY
        |                                      and imsi is not null) s1
        |                             left outer join (select distinct mmeues1apid,
        |                                                             mmegroupid,
        |                                                             mmecode,
        |                                                             time_stamp procedure_start_time,
        |                                                             p_hour
        |                                               from d_enl_mr_new_h
        |                                              where p_hour = $MONTH
        |                                                and p_city = $DAY) lte
        |                               on lte.mmeues1apid = s1.mmeues1apid
        |                              and lte.mmegroupid = s1.mmegroupid
        |                              and lte.mmecode = s1.mmecode
        |                              and lte.p_hour = s1.p_hour
        |                            where unix_timestamp(s1.procedure_start_time,
        |                                                 "yyyy-DD-mm HH:mm:ss.S") >=
        |                                  unix_timestamp(lte.procedure_start_time,
        |                                                 "yyyy-DD-mm HH:mm:ss.S") - 600
        |                              and unix_timestamp(s1.procedure_start_time,
        |                                                 "yyyy-DD-mm HH:mm:ss.S") <=
        |                                  unix_timestamp(lte.procedure_start_time,
        |                                                 "yyyy-DD-mm HH:mm:ss.S") + 600) p) s1
        |    on lte.mmeues1apid = s1.mmeues1apid
        |   and lte.mmegroupid = s1.mmegroupid
        |   and lte.mmecode = s1.mmecode
        | where p_hour = $MONTH
        |   and p_city = $DAY
        |   and rum = 1 ) ltemro
        |LEFT JOIN
        |     adjacent_area AA
        |     ON  ltemro.objectid = AA.cellid
        |     AND ltemro.ltencpci = AA.pci
        |     AND ltemro.ltencearfcn = AA.freq1
        |;
        |
      """.stripMargin)


  }*/
}

