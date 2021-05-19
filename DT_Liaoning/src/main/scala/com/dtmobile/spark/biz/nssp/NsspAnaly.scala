package com.dtmobile.spark.biz.nssp

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel


/**
  * NsspAnaly
  *x
  *
  * @author heyongjin
  * @ create 2017/03/02 10:36
  *
  **/
class NsspAnaly(ANALY_DATE: String, ANALY_HOUR: String,SDB: String,DDB: String,localStr:String,warhouseDir:String,ORCAL: String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    //原始表初始化
    sql(s"use $SDB")
    sql(
      s"""
         |alter table lte_mro_source add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table tb_xdr_ifc_uu add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table tb_xdr_ifc_x2 add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)

    sql(
      s"""
         |alter table tb_xdr_ifc_gxrx_all add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/volte_rx/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table tb_xdr_ifc_gxrx add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/tb_xdr_ifc_gxrx/dt=${ANALY_DATE}/h=${ANALY_HOUR}"
       """.stripMargin)

    sql(
      s"""
         |alter table tb_xdr_ifc_http add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/s1u_http_orgn/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table tb_xdr_ifc_sv add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/volte_sv/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)
    sql(
      s"""
         |alter table tb_xdr_ifc_s1mme add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/s1mme_orgn/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)

    sql(
      s"""
         |alter table tb_xdr_ifc_mw add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/volte_orgn/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)

    sparkSession.catalog.clearCache()
    sql(s"""select * from $SDB.tb_xdr_ifc_s1mme where dt='$ANALY_DATE' and h='$ANALY_HOUR'""").createOrReplaceTempView("tb_xdr_ifc_s1mme_cache")
    sparkSession.catalog.cacheTable("tb_xdr_ifc_s1mme_cache")




    sql(s"use $DDB")
    sql(s"alter table tb_xdr_ifc_uu add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(s"alter table tb_xdr_ifc_x2 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(s"alter table cell_mr add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(s"alter table lte_mro_source add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")

    //添加异常事件分区 TOP小区分析需要用
    sql(
      s"""
         |alter table exception_analysis add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "$warhouseDir/exception_analysis/dt=${ANALY_DATE}/h=${ANALY_HOUR}"
       """.stripMargin)

    sql(
      s"""
         |alter table tb_xdr_ifc_http add if not exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
         |location "/$localStr/s1u_http_orgn/${ANALY_DATE}/${ANALY_HOUR}"
       """.stripMargin)



    sql(
      s"""
         |select
         |x2.LENGTH,
         |x2.CITY,
         |x2.INTERFACE,
         |x2.XDRID,
         |x2.RAT,
         |S1.IMSI,
         |S1.IMEI,
         |S1.MSISDN,
         |x2.PROCEDURETYPE,
         |x2.PROCEDURESTARTTIME,
         |x2.PROCEDUREENDTIME,
         |x2.PROCEDURESTATUS,
         |x2.CELLID,
         |x2.TARGETCELLID,
         |x2.ENBID,
         |x2.TARGETENBID,
         |x2.MMEUES1APID,
         |x2.MMEGROUPID,
         |x2.MMECODE,
         |x2.REQUESTCAUSE,
         |x2.FAILURECAUSE,
         |x2.EPSBEARERNUMBER,
         |x2.BEARER0ID,
         |x2.BEARER0STATUS,
         |x2.BEARER1ID,
         |x2.BEARER1STATUS,
         |x2.BEARER2ID,
         |x2.BEARER2STATUS,
         |x2.BEARER3ID,
         |x2.BEARER3STATUS,
         |x2.BEARER4ID,
         |x2.BEARER4STATUS,
         |x2.BEARER5ID,
         |x2.BEARER5STATUS,
         |x2.BEARER6ID,
         |x2.BEARER6STATUS,
         |x2.BEARER7ID,
         |x2.BEARER7STATUS,
         |x2.BEARER8ID,
         |x2.BEARER8STATUS,
         |x2.BEARER9ID,
         |x2.BEARER9STATUS,
         |x2.BEARER10ID,
         |x2.BEARER10STATUS,
         |x2.BEARER11ID,
         |x2.BEARER11STATUS,
         |x2.BEARER12ID,
         |x2.BEARER12STATUS,
         |x2.BEARER13ID,
         |x2.BEARER13STATUS,
         |x2.BEARER14ID,
         |x2.BEARER14STATUS,
         |x2.BEARER15ID,
         |x2.BEARER15STATUS,
         |x2.RANGETIME
         |from $SDB.tb_Xdr_ifc_x2 x2
         |left outer join
         |(
         |SELECT
         |mmeues1apid
         |,mmegroupid
         |,mmecode
         |,imsi
         |,IMEI
         |,MSISDN
         |,ROW_NUMBER() OVER(PARTITION BY mmeues1apid,mmegroupid,mmecode ORDER BY PROCEDURESTARTTIME DESC) rum
         |from
         |(
         |select imsi,IMEI,MSISDN,x2.mmeues1apid,x2.mmegroupid,x2.mmecode,x2.PROCEDURESTARTTIME
         |FROM
         |(select imsi,IMEI,MSISDN,mmeues1apid,mmegroupid,mmecode,PROCEDURESTARTTIME,dt,h from tb_xdr_ifc_s1mme_cache where imsi is not null and imsi !='') s1
         |left outer join
         |(select distinct mmeues1apid,mmegroupid,mmecode,PROCEDURESTARTTIME,dt,h from $SDB.tb_xdr_ifc_x2 where dt='$ANALY_DATE' and h='$ANALY_HOUR') x2
         |on x2.mmeues1apid=s1.mmeues1apid and x2.mmegroupid=s1.mmegroupid and x2.mmecode=s1.mmecode
         |and x2.dt=s1.dt and x2.h=s1.h
         |where s1.PROCEDURESTARTTIME>=x2.PROCEDURESTARTTIME-120000
         |and s1.PROCEDURESTARTTIME<=x2.PROCEDURESTARTTIME+120000
         |)
         |)s1
         |on x2.mmeues1apid=s1.mmeues1apid and x2.mmegroupid=s1.mmegroupid and x2.mmecode=s1.mmecode
         |where dt='$ANALY_DATE' and h='$ANALY_HOUR' and rum=1
       """.stripMargin).repartition(300).write.mode(SaveMode.Overwrite)
      .csv(s"$warhouseDir/tb_xdr_ifc_x2/dt=$ANALY_DATE/h=$ANALY_HOUR")

    sql(
      s"""
         |SELECT
         |UU.LENGTH,
         |UU.CITY,
         |UU.INTERFACE,
         |UU.XDRID,
         |UU.RAT,
         |S1.IMSI,
         |S1.IMEI,
         |S1.MSISDN,
         |UU.PROCEDURETYPE,
         |UU.PROCEDURESTARTTIME,
         |UU.PROCEDUREENDTIME,
         |UU.KEYWORD1,
         |UU.KEYWORD2,
         |UU.PROCEDURESTATUS,
         |UU.PLMNID,
         |UU.ENBID,
         |UU.CELLID,
         |UU.CRNTI,
         |UU.TARGETENBID,
         |UU.TARGETCELLID,
         |UU.TARGETCRNTI,
         |UU.MMEUES1APID,
         |UU.MMEGROUPID,
         |UU.MMECODE,
         |UU.MTMSI,
         |UU.CSFBINDICATION,
         |UU.REDIRECTEDNETWORK,
         |UU.EPSBEARERNUMBER,
         |UU.BEARER0ID,
         |UU.BEARER0STATUS,
         |UU.BEARER1ID,
         |UU.BEARER1STATUS,
         |UU.BEARER2ID,
         |UU.BEARER2STATUS,
         |UU.BEARER3ID,
         |UU.BEARER3STATUS,
         |UU.BEARER4ID,
         |UU.BEARER4STATUS,
         |UU.BEARER5ID,
         |UU.BEARER5STATUS,
         |UU.BEARER6ID,
         |UU.BEARER6STATUS,
         |UU.BEARER7ID,
         |UU.BEARER7STATUS,
         |UU.BEARER8ID,
         |UU.BEARER8STATUS,
         |UU.BEARER9ID,
         |UU.BEARER9STATUS,
         |UU.BEARER10ID,
         |UU.BEARER10STATUS,
         |UU.BEARER11ID,
         |UU.BEARER11STATUS,
         |UU.BEARER12ID,
         |UU.BEARER12STATUS,
         |UU.BEARER13ID,
         |UU.BEARER13STATUS,
         |UU.BEARER14ID,
         |UU.BEARER14STATUS,
         |UU.BEARER15ID,
         |UU.BEARER15STATUS,
         |UU.RANGETIME
         |FROM $SDB.TB_XDR_IFC_UU UU
         |left outer join
         |(
         |SELECT
         |mmeues1apid
         |,mmegroupid
         |,mmecode
         |,imsi
         |,IMEI
         |,MSISDN
         |,ROW_NUMBER() OVER(PARTITION BY mmeues1apid,mmegroupid,mmecode ORDER BY PROCEDURESTARTTIME DESC) rum
         |from
         |(
         |select imsi,IMEI,MSISDN,uu.mmeues1apid,uu.mmegroupid,uu.mmecode,uu.PROCEDURESTARTTIME
         |FROM
         |(select imsi,IMEI,MSISDN,mmeues1apid,mmegroupid,mmecode,PROCEDURESTARTTIME,dt,h from tb_xdr_ifc_s1mme_cache where imsi is not null and imsi !='') s1
         |left outer join
         |(select distinct mmeues1apid,mmegroupid,mmecode,PROCEDURESTARTTIME,dt,h from $SDB.tb_xdr_ifc_uu where dt='$ANALY_DATE' and h='$ANALY_HOUR') uu
         |on uu.mmeues1apid=s1.mmeues1apid and uu.mmegroupid=s1.mmegroupid and uu.mmecode=s1.mmecode
         |and uu.dt=s1.dt and uu.h=s1.h
         |where s1.PROCEDURESTARTTIME>=uu.PROCEDURESTARTTIME-120000
         |and s1.PROCEDURESTARTTIME<=uu.PROCEDURESTARTTIME+120000
         |)
         |)s1
         |on uu.mmeues1apid=s1.mmeues1apid and uu.mmegroupid=s1.mmegroupid and uu.mmecode=s1.mmecode
         |where dt='$ANALY_DATE' and h='$ANALY_HOUR' and rum=1
       """.stripMargin).repartition(300).write.mode(SaveMode.Overwrite)
      .csv(s"$warhouseDir/tb_xdr_ifc_uu/dt=$ANALY_DATE/h=$ANALY_HOUR")
/*
    sparkSession.read.format("jdbc").option("url", s"jdbc:oracle:thin:@$ORCAL")
      .option("dbtable", "ltecell")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().registerTempTable("ltecell")*/

    /*    sparkSession.read.format("jdbc").option("url", s"jdbc:oracle:thin:@$ORCAL")
          .option("dbtable","lte2lteadj")
          .option("user", "scott")
          .option("password", "tiger")
          .option("driver", "oracle.jdbc.driver.OracleDriver")
          .load().registerTempTable("lte2lteadj")*/

    sql(s"use $DDB")
/*
    sparkSession.udf.register("calcdis", (fLon:Float,fLat:Float,tLon:Float,tLat:Float)=>(2 * scala.math.asin(scala.math.sqrt(scala.math.pow(scala.math.sin((fLat*3.14159265/180 - tLat*3.14159265/180)/2),2) + scala.math.cos(fLat*3.14159265/180)*scala.math.cos(tLat*3.14159265/180)*scala.math.pow(scala.math.sin((fLon*3.14159265/180 - tLon*3.14159265/180)/2),2))))*6378.137)

    sql(
      s"""
      select
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
        """.stripMargin).repartition(20).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "false").save(s"$warhouseDir/tb_cell_distance/") //dt=$ANALY_DATE/h=$ANALY_HOUR")
*/

    sql(
      s"""
         |SELECT
         |lte.objectid,
         |lte.vid,
         |lte.fileformatversion,
         |lte.starttime,
         |lte.endtime,
         |lte.period,
         |lte.enbid,
         |lte.userlabel,
         |lte.mrname,
         |lte.cellid,
         |lte.earfcn,
         |lte.subframenbr,
         |lte.prbnbr,
         |lte.mmeues1apid,
         |lte.mmegroupid,
         |lte.mmecode,
         |lte.meatime,
         |lte.eventtype,
         |lte.gridcenterlongitude,
         |lte.gridcenterlatitude,
         |lte.kpi1,
         |lte.kpi2,
         |lte.kpi3,
         |lte.kpi4,
         |lte.kpi5,
         |lte.kpi6,
         |lte.kpi7,
         |lte.kpi8,
         |lte.kpi9,
         |lte.kpi10,
         |lte.kpi11,
         |lte.kpi12,
         |lte.kpi13,
         |lte.kpi14,
         |lte.kpi15,
         |lte.kpi16,
         |lte.kpi17,
         |lte.kpi18,
         |lte.kpi19,
         |lte.kpi20,
         |lte.kpi21,
         |lte.kpi22,
         |lte.kpi23,
         |lte.kpi24,
         |AA.tcellid,
         |lte.kpi26,
         |lte.kpi27,
         |lte.kpi28,
         |lte.kpi29,
         |lte.kpi30,
         |lte.kpi31,
         |lte.kpi32,
         |lte.kpi33,
         |lte.kpi34,
         |lte.kpi35,
         |lte.kpi36,
         |lte.kpi37,
         |lte.kpi38,
         |lte.kpi39,
         |lte.kpi40,
         |lte.kpi41,
         |lte.kpi42,
         |lte.kpi43,
         |lte.kpi44,
         |lte.kpi45,
         |lte.kpi46,
         |lte.kpi47,
         |lte.kpi48,
         |lte.kpi49,
         |lte.kpi50,
         |lte.kpi51,
         |lte.kpi52,
         |lte.kpi53,
         |lte.kpi54,
         |lte.kpi55,
         |lte.kpi56,
         |lte.kpi57,
         |lte.kpi58,
         |lte.kpi59,
         |lte.kpi60,
         |lte.kpi61,
         |lte.kpi62,
         |lte.kpi63,
         |lte.kpi64,
         |lte.kpi65,
         |lte.kpi66,
         |lte.kpi67,
         |lte.kpi68,
         |lte.kpi69,
         |lte.kpi70,
         |lte.kpi71,
         |lte.length,
         |lte.city,
         |lte.xdrtype,
         |lte.interface,
         |lte.xdrid,
         |lte.rat,
         |S1.imsi,
         |S1.imei,
         |S1.msisdn,
         |lte.mrtype,
         |lte.neighborcellnumber,
         |lte.gsmneighborcellnumber,
         |lte.tdsneighborcellnumber,
         |lte.v_enb,
         |lte.mrtime
         |FROM
         |$SDB.lte_mro_source lte
          LEFT JOIN  adjacent_area AA
         | ON  lte.cellid = AA.cellid
         | AND lte.kpi12 = AA.pci
         | AND lte.kpi11 = AA.freq1
         |left outer join
         |(
         |SELECT
         |mmeues1apid
         |,mmegroupid
         |,mmecode
         |,imsi
         |,IMEI
         |,MSISDN
         |,ROW_NUMBER() OVER(PARTITION BY mmeues1apid,mmegroupid,mmecode ORDER BY PROCEDURESTARTTIME DESC) rum
         |from
         |(
         |select imsi,IMEI,MSISDN,lte.mmeues1apid,lte.mmegroupid,lte.mmecode,lte.PROCEDURESTARTTIME
         |FROM
         |(select imsi,IMEI,MSISDN,mmeues1apid,mmegroupid,mmecode,PROCEDURESTARTTIME,dt,h from tb_xdr_ifc_s1mme_cache where imsi is not null and imsi !='') s1
         |left outer join
         |(select distinct mmeues1apid,mmegroupid,mmecode,mrtime PROCEDURESTARTTIME,dt,h from $SDB.lte_mro_source where dt='$ANALY_DATE' and h='$ANALY_HOUR' and mrname!='MR.LteScRIP0') lte
         |on lte.mmeues1apid=s1.mmeues1apid and lte.mmegroupid=s1.mmegroupid and lte.mmecode=s1.mmecode
         |and lte.dt=s1.dt and lte.h=s1.h
         |where s1.PROCEDURESTARTTIME>=lte.PROCEDURESTARTTIME-120000
         |and s1.PROCEDURESTARTTIME<=lte.PROCEDURESTARTTIME+120000
         |)
         |)s1
         |on lte.mmeues1apid=s1.mmeues1apid and lte.mmegroupid=s1.mmegroupid and lte.mmecode=s1.mmecode
         |where dt='$ANALY_DATE' and h='$ANALY_HOUR' and rum=1 and ( lte.mrtime !='' or lte.mrtime !="" or lte.mrtime  is not null or lte.mrtime != null)
         |and lte.mrname!='MR.LteScRIP0'
       """.stripMargin).repartition(300).write.mode(SaveMode.Overwrite)
      .csv(s"$warhouseDir/lte_mro_source/dt=$ANALY_DATE/h=$ANALY_HOUR")

    sql(
      s"""
         |select
         |OBJECTID,
         |VID,
         |FILEFORMATVERSION,
         |STARTTIME,
         |ENDTIME,
         |PERIOD,
         |ENBID,
         |USERLABEL,
         |MRNAME,
         |CELLID,
         |EARFCN,
         |SUBFRAMENBR,
         |PRBNBR,
         |MMEUES1APID,
         |MMEGROUPID,
         |MMECODE,
         |MEATIME,
         |EVENTTYPE,
         |GRIDCENTERLONGITUDE,
         |GRIDCENTERLATITUDE,
         |KPI1,
         |KPI2,
         |KPI3,
         |KPI4,
         |KPI5,
         |KPI6,
         |KPI7,
         |KPI8,
         |KPI9,
         |KPI10,
         |KPI11,
         |KPI12,
         |KPI13,
         |KPI14,
         |KPI15,
         |KPI16,
         |KPI17,
         |KPI18,
         |KPI19,
         |KPI20,
         |KPI21,
         |KPI22,
         |KPI23,
         |KPI24,
         |KPI25,
         |KPI26,
         |KPI27,
         |KPI28,
         |KPI29,
         |KPI30,
         |KPI31,
         |KPI32,
         |KPI33,
         |KPI34,
         |KPI35,
         |KPI36,
         |KPI37,
         |KPI38,
         |KPI39,
         |KPI40,
         |KPI41,
         |KPI42,
         |KPI43,
         |KPI44,
         |KPI45,
         |KPI46,
         |KPI47,
         |KPI48,
         |KPI49,
         |KPI50,
         |KPI51,
         |KPI52,
         |KPI53,
         |KPI54,
         |KPI55,
         |KPI56,
         |KPI57,
         |KPI58,
         |KPI59,
         |KPI60,
         |KPI61,
         |KPI62,
         |KPI63,
         |KPI64,
         |KPI65,
         |KPI66,
         |KPI67,
         |KPI68,
         |KPI69,
         |KPI70,
         |KPI71,
         |LENGTH,
         |CITY,
         |XDRTYPE,
         |INTERFACE,
         |XDRID,
         |RAT,
         |IMSI,
         |IMEI,
         |MSISDN,
         |MRTYPE,
         |NEIGHBORCELLNUMBER,
         |GSMNEIGHBORCELLNUMBER,
         |TDSNEIGHBORCELLNUMBER,
         |V_ENB,
         |MRTIME
         |FROM
         |$SDB.lte_mro_source
         |where dt='$ANALY_DATE' and h='$ANALY_HOUR' and mrname='MR.LteScRIP0' and MRTIME is not null
       """.stripMargin).repartition(300).write.mode(SaveMode.Overwrite)
      .csv(s"$warhouseDir/cell_mr/dt=$ANALY_DATE/h=$ANALY_HOUR")


    sql(
      s"""
         |insert overwrite table ${SDB}.tb_xdr_ifc_gxrx partition(dt=${ANALY_DATE},h=${ANALY_HOUR})
         |  select t.length,
         |         t.city,
         |         t.interface,
         |         t.xdrid,
         |         t.rat,
         |         t.imsi,
         |         t.imei,
         |         t.msisdn,
         |         t.proceduretype,
         |         t.procedurestarttime,
         |         t.procedureendtime,
         |         t.icid,
         |         t.originrealm,
         |         t.destinationrealm,
         |         t.originhost,
         |         t.destinationhost,
         |         t.sgsnsgwsigip,
         |         t.afappid,
         |         t.ccrequesttype,
         |         t.rxrequesttype,
         |         t.mediatype,
         |         t.abortcause,
         |         t.resultcode,
         |         t.experimentalresultcode,
         |         t.sessionreleasecause,
         |         t.rulefailurecode,
         |         t.sessionid,
         |         t.calledstationid,
         |         t.framedipv6prefix,
         |         t.framedipaddress,
         |         t.cellid,
         |         t.sourcetac,
         |         t.sourceneip,
         |         t.sourceneport,
         |         t.destinationneip,
         |         t.destinationneport,
         |         t.qci
         |    from (select length,
         |                 city,
         |                 interface,
         |                 xdrid,
         |                 rat,
         |                 imsi,
         |                 imei,
         |                 msisdn,
         |                 proceduretype,
         |                 procedurestarttime,
         |                 procedureendtime,
         |                 icid,
         |                 originrealm,
         |                 destinationrealm,
         |                 originhost,
         |                 destinationhost,
         |                 sgsnsgwsigip,
         |                 afappid,
         |                 ccrequesttype,
         |                 rxrequesttype,
         |                 mediatype,
         |                 abortcause,
         |                 resultcode,
         |                 experimentalresultcode,
         |                 sessionreleasecause,
         |                 rulefailurecode,
         |                 sessionid,
         |                 calledstationid,
         |                 framedipv6prefix,
         |                 framedipaddress,
         |                 cellid,
         |                 sourcetac,
         |                 sourceneip,
         |                 sourceneport,
         |                 destinationneip,
         |                 destinationneport,
         |                 qci,
         |                 row_number() over(partition by imsi, sessionid,proceduretype,interface order by procedurestarttime asc) rownum
         |            from ${SDB}.tb_xdr_ifc_gxrx_all where dt=${ANALY_DATE} and h=${ANALY_HOUR} ) t
         |   where t.rownum = 1
         |
       """.stripMargin)
  }
}

