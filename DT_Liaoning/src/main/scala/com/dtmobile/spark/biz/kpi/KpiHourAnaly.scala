package com.dtmobile.spark.biz.kpi

import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * KpiHourAnaly
  *
  * @author heyongjin
  * create 2017/03/02 10:36
  *
  **/
class KpiHourAnaly(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  var onoff=0

  var procedurestatussuccess=1
  var procedurestatusfaile=2
  var ServiceTypeaudio=1
  var ServiceTypevideo=2
  var callsidecalling=1
  var callsedediacalled=2
  var timetr=1


  def analyse(implicit sparkSession: SparkSession): Unit = {
      imsiCellHourAnalyse(sparkSession)
      cellHourAnalyse(sparkSession)
      mrCellHourAnalyse(sparkSession)
      mrImsiHourAnalyse(sparkSession)

  }

  def imsiCellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    if(onoff==1){
      procedurestatussuccess = 0
      ServiceTypeaudio=0
      ServiceTypevideo=1
      callsidecalling=0
      callsedediacalled=1
      timetr=100
    }

    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table volte_gt_user_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")

//    sql(s"use $DDB")
//    sparkSession.catalog.clearCache()
//    sql(s"""select * from $SDB.tb_xdr_ifc_s1mme where dt='$ANALY_DATE' and h='$ANALY_HOUR'""").createOrReplaceTempView("tb_xdr_ifc_s1mme_cache")
//    sparkSession.catalog.cacheTable("tb_xdr_ifc_s1mme_cache")

    val uu = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	cellid,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	sum(
         |		CASE
         |		WHEN (
         |			proceduretype = 7
         |			OR proceduretype = 8
         |		)
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) lteswsucc,
         |	sum(
         |		CASE
         |		WHEN (
         |			proceduretype = 7
         |			OR proceduretype = 8
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 4 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcrebuild,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 8
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbswx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 8 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbswy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbinx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbiny,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         |  0 AS srvccsuccS1,
         |  0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         | 0 as srvccsucc_s1,
         | 0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	 $DDB.TB_XDR_IFC_UU
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin)

    val x2 = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) enbx2swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND (
         |			ProcedureStatus = 0
         |			OR (
         |				(
         |					ProcedureStatus BETWEEN 1
         |					AND 255
         |				)
         |				AND (
         |					failurecause != 1000
         |					OR failurecause IS NULL
         |				)
         |			)
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swx,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND (
         |			ProcedureStatus = 0
         |			OR (
         |				(
         |					ProcedureStatus BETWEEN 1
         |					AND 255
         |				)
         |				AND (
         |					failurecause != 1000
         |					OR failurecause IS NULL
         |				)
         |			)
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$DDB.TB_XDR_IFC_X2
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin)

    val x2_enbx2 = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(targetcellid)CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         | 0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         0 AS remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |SUM(CASE WHEN proceduretype=1 and procedurestatus=0 then 1 else 0 end)enbx2insucc,
         |SUM(CASE WHEN proceduretype=1 then 1 else 0 end)enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$DDB.TB_XDR_IFC_X2
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	targetcellid
       """.stripMargin)

    val sv = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(SOURCEECI) cellid,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srvccsucc,
         |	0 as srvccatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			SVDELAY/$timetr
         |		END
         |	) srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         |  0 AS srvccsuccS1,
         |  0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_SV
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	SOURCEECI
       """.stripMargin)

    val voltesip = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(sourceeci) cellid,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |  and
         |t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = $procedurestatussuccess
         |  and
         |t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_MW t1
         | left join $DDB.MW_IP t2 on t1.sourceneip=t2.MWIP
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	sourceeci
       """.stripMargin)

    val voltesip0 = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(sourceeci) cellid,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND alertingtime <> 4294967295 AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio AND t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo
         |		AND alertingtime <> 4294967295 AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND alertingtime <> 4294967295 THEN
         |			alertingtime/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltetime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND callduration <> 4294967295 THEN
         |			callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltemctime,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND callduration <> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltemctimey,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltevdtime,
         | sum(
         |		CASE
         |		WHEN interface = 14
         |		AND ProcedureType = 5
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltevdtimey,
         |	0 AS voltemchandover,
         |	sum(
         |		CASE
         |		WHEN interface = 14
         |		AND ProcedureType = 5
         |		AND ServiceType = $ServiceTypeaudio
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) volteanswer,
         |	0 AS voltevdhandover,
         |	sum(
         |		CASE
         |		WHEN interface = 14
         |		AND ProcedureType = 5
         |		AND ServiceType = $ServiceTypevideo
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |  and t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = $procedurestatussuccess
         |  and t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	sum(
         |		CASE
         |		WHEN interface = 14
         |		AND ProcedureType = 5
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND alertingtime <> 4294967295 AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltecallingmcsucc,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo
         |		AND alertingtime <> 4294967295 AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltecallingvdsucc,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio and alertingtime is not null
         |		AND (alertingtime <> 4294967295 or responsecode in (403,404,405,413,414,415,416,422,423,480,486,487,488,600,603,604,606)) AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltemcnetsucc,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltemcnetatt,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and alertingtime is not null
         |		AND (alertingtime <> 4294967295 or responsecode in (403,404,405,413,414,415,416,422,423,480,486,487,488,600,603,604,606)) AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltevdnetsucc,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltevdnetatt,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio and alertingtime is not null
         |		AND alertingtime <> 4294967295 AND
         |   t2.MWIP is not null THEN
         |			alertingtime
         |		ELSE
         |			0
         |		END
         |	)voltecallingmctime,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and alertingtime is not null
         |		AND alertingtime <> 4294967295 AND
         |   t2.MWIP is not null THEN
         |			alertingtime
         |		ELSE
         |			0
         |		END
         |	)voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_MW t1
         | left join $DDB.MW_IP t2 on t1.sourceneip=t2.MWIP
         |WHERE
         |	callside = $callsidecalling
         |AND dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	sourceeci
       """.stripMargin)

    voltesip0.createOrReplaceTempView("voltesip0table")

    val voltesip1 = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(desteci) cellid,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND alertingtime <> 4294967295 AND t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio AND t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo
         |		AND alertingtime <> 4294967295 AND t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo AND t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND alertingtime <> 4294967295 THEN
         |			alertingtime/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltetime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND callduration <> 4294967295 THEN
         |			callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltemctime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND callduration <> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltemctimey,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltevdtime,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltevdtimey,
         |	0 AS voltemchandover,
         |	sum(
         |		CASE
         |		WHEN interface = 14
         |		AND ProcedureType = 5
         |		AND ServiceType = $ServiceTypeaudio
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) volteanswer,
         |	0 AS voltevdhandover,
         |	sum(
         |		CASE
         |		WHEN interface = 14
         |		AND ProcedureType = 5
         |		AND ServiceType = $ServiceTypevideo
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14 and t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = $procedurestatussuccess
         |  and t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	sum(
         |		CASE
         |		WHEN interface = 14
         |		AND ProcedureType = 5
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND alertingtime <> 4294967295 AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltecalledmcsucc,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo
         |		AND alertingtime <> 4294967295 AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltecalledvdsucc,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo AND
         |   t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_mw t1
         | left join $DDB.MW_IP t2 on t1.destneip=t2.MWIP
         |WHERE
         |	callside = $callsedediacalled
         |AND dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	desteci
       """.stripMargin)

    voltesip1.createOrReplaceTempView("voltesip1table")

    sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	sum(CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16
         |    AND keyword1=3 THEN
         |			1
         |		ELSE
         |			0
         |		END)srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 2 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srqatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 2
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)srqsucc,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 5 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) tauatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 5
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 20
         |		AND Keyword1 = 0
         |		AND RequestCause <> 65535
         |		AND RequestCause NOT IN (2, 20, 23, 24, 28, 512, 514) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) wirelessdrop,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 18
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) wireless,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 21
         |		AND BEARER0REQUESTCAUSE <> 65535
         |		AND BEARER1REQUESTCAUSE <> 65535
         |		AND BEARER2REQUESTCAUSE <> 65535
         |		AND BEARER3REQUESTCAUSE <> 65535
         |		AND BEARER4REQUESTCAUSE <> 65535
         |		AND BEARER5REQUESTCAUSE <> 65535
         |		AND BEARER6REQUESTCAUSE <> 65535
         |		AND BEARER7REQUESTCAUSE <> 65535
         |		AND BEARER8REQUESTCAUSE <> 65535
         |		AND BEARER9REQUESTCAUSE <> 65535
         |		AND BEARER10REQUESTCAUSE <> 65535
         |		AND BEARER11REQUESTCAUSE <> 65535
         |		AND BEARER12REQUESTCAUSE <> 65535
         |		AND BEARER13REQUESTCAUSE <> 65535
         |		AND BEARER14REQUESTCAUSE <> 65535
         |		AND BEARER15REQUESTCAUSE <> 65535
         |		AND BEARER0REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER1REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER2REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER3REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER4REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER5REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER6REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER7REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER8REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER9REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER10REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER11REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER12REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER13REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER14REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER15REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16
         |		AND keyword1 = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) eabs1swy,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 14
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) s1tox2swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16
         |  AND keyword1 = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 1
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) attachx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END)attachy,
         |0 AS voltesucc,
         |sum(CASE WHEN INTERFACE = 5
         |AND proceduretype = 16
         |AND keyword1=3 and PROCEDURESTATUS=0 THEN
         |1
         |ELSE
         |0
         |END)srvccsuccS1,
         |sum(case when interface=5 and ProcedureType=18 then 1 else 0 end)s1contextbuild,
         |sum(case when interface=5 and ProcedureType=20 and Keyword1=0 then 1 else 0 end)enbrelese,
         |sum(case when interface=5 and proceduretype=20 and Keyword1=0 and requestcause in (20,23,24,28,128) then 1 else 0 end)nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |sum(case when Interface=5 and ProcedureType=16 and keyword1=3 then 1 else 0 end)srvccatt_s1,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer0status in (1,2)) then 1 else 0 end)erabbuildreq1,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer1status in (1,2)) then 1 else 0 end)erabbuildreq2,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer2status in (1,2)) then 1 else 0 end)erabbuildreq3,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer3status in (1,2)) then 1 else 0 end)erabbuildreq4,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer4status in (1,2)) then 1 else 0 end)erabbuildreq5,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer5status in (1,2)) then 1 else 0 end)erabbuildreq6,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer6status in (1,2)) then 1 else 0 end)erabbuildreq7,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer7status in (1,2)) then 1 else 0 end)erabbuildreq8,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer0status=1) then 1 else 0 end)erabbuildsucc1,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer1status=1) then 1 else 0 end)erabbuildsucc2,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer2status=1) then 1 else 0 end)erabbuildsucc3,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer3status=1) then 1 else 0 end)erabbuildsucc4,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer4status=1) then 1 else 0 end)erabbuildsucc5,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer5status=1) then 1 else 0 end)erabbuildsucc6,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer6status=1) then 1 else 0 end)erabbuildsucc7,
         |sum(case when (ProcedureType in (2,3,5,7,9,10,13) and bearer7status=1) then 1 else 0 end)erabbuildsucc8,
         |sum(case when interface=5 and procedureType=18 and ProcedureStatus=0 then 1 else 0 end)s1contextbuildsucc,
         |sum(case when interface=5 and procedureType=13 and bearer0qci=1 and  bearer0status=1 then 1 else 0 end)sm_adebsucc_qci1_1,
         |sum(case when interface=5 and procedureType=13 and bearer1qci=1 and  bearer1status=1 then 1 else 0 end)sm_adebsucc_qci1_2,
         |sum(case when interface=5 and procedureType=13 and bearer2qci=1 and  bearer2status=1 then 1 else 0 end)sm_adebsucc_qci1_3,
         |sum(case when interface=5 and procedureType=13 and bearer3qci=1 and  bearer3status=1 then 1 else 0 end)sm_adebsucc_qci1_4,
         |sum(case when interface=5 and procedureType=13 and bearer4qci=1 and  bearer4status=1 then 1 else 0 end)sm_adebsucc_qci1_5,
         |sum(case when interface=5 and procedureType=13 and bearer5qci=1 and  bearer5status=1 then 1 else 0 end)sm_adebsucc_qci1_6,
         |sum(case when interface=5 and procedureType=13 and bearer6qci=1 and  bearer6status=1 then 1 else 0 end)sm_adebsucc_qci1_7,
         |sum(case when interface=5 and procedureType=13 and bearer7qci=1 and  bearer7status=1 then 1 else 0 end)sm_adebsucc_qci1_8,
         |sum(case when interface=5 and procedureType=13 and bearer0qci=1 and  bearer0status in (1,2) then 1 else 0 end)sm_adebreq_qci1_1,
         |sum(case when interface=5 and procedureType=13 and bearer1qci=1 and  bearer1status in (1,2) then 1 else 0 end)sm_adebreq_qci1_2,
         |sum(case when interface=5 and procedureType=13 and bearer2qci=1 and  bearer2status in (1,2) then 1 else 0 end)sm_adebreq_qci1_3,
         |sum(case when interface=5 and procedureType=13 and bearer3qci=1 and  bearer3status in (1,2) then 1 else 0 end)sm_adebreq_qci1_4,
         |sum(case when interface=5 and procedureType=13 and bearer4qci=1 and  bearer4status in (1,2) then 1 else 0 end)sm_adebreq_qci1_5,
         |sum(case when interface=5 and procedureType=13 and bearer5qci=1 and  bearer5status in (1,2) then 1 else 0 end)sm_adebreq_qci1_6,
         |sum(case when interface=5 and procedureType=13 and bearer6qci=1 and  bearer6status in (1,2) then 1 else 0 end)sm_adebreq_qci1_7,
         |sum(case when interface=5 and procedureType=13 and bearer7qci=1 and  bearer7status in (1,2) then 1 else 0 end)sm_adebreq_qci1_8,
         |sum(case when interface=5 and procedureType=13 and bearer0qci=2 and  bearer0status=1 then 1 else 0 end)sm_adebsucc_qci2_1,
         |sum(case when interface=5 and procedureType=13 and bearer1qci=2 and  bearer1status=1 then 1 else 0 end)sm_adebsucc_qci2_2,
         |sum(case when interface=5 and procedureType=13 and bearer2qci=2 and  bearer2status=1 then 1 else 0 end)sm_adebsucc_qci2_3,
         |sum(case when interface=5 and procedureType=13 and bearer3qci=2 and  bearer3status=1 then 1 else 0 end)sm_adebsucc_qci2_4,
         |sum(case when interface=5 and procedureType=13 and bearer4qci=2 and  bearer4status=1 then 1 else 0 end)sm_adebsucc_qci2_5,
         |sum(case when interface=5 and procedureType=13 and bearer5qci=2 and  bearer5status=1 then 1 else 0 end)sm_adebsucc_qci2_6,
         |sum(case when interface=5 and procedureType=13 and bearer6qci=2 and  bearer6status=1 then 1 else 0 end)sm_adebsucc_qci2_7,
         |sum(case when interface=5 and procedureType=13 and bearer7qci=2 and  bearer7status=1 then 1 else 0 end)sm_adebsucc_qci2_8,
         |sum(case when interface=5 and procedureType=13 and bearer0qci=2 and  bearer0status in (1,2) then 1 else 0 end)sm_adebreq_qci2_1,
         |sum(case when interface=5 and procedureType=13 and bearer1qci=2 and  bearer1status in (1,2) then 1 else 0 end)sm_adebreq_qci2_2,
         |sum(case when interface=5 and procedureType=13 and bearer2qci=2 and  bearer2status in (1,2) then 1 else 0 end)sm_adebreq_qci2_3,
         |sum(case when interface=5 and procedureType=13 and bearer3qci=2 and  bearer3status in (1,2) then 1 else 0 end)sm_adebreq_qci2_4,
         |sum(case when interface=5 and procedureType=13 and bearer4qci=2 and  bearer4status in (1,2) then 1 else 0 end)sm_adebreq_qci2_5,
         |sum(case when interface=5 and procedureType=13 and bearer5qci=2 and  bearer5status in (1,2) then 1 else 0 end)sm_adebreq_qci2_6,
         |sum(case when interface=5 and procedureType=13 and bearer6qci=2 and  bearer6status in (1,2) then 1 else 0 end)sm_adebreq_qci2_7,
         |sum(case when interface=5 and procedureType=13 and bearer7qci=2 and  bearer7status in (1,2) then 1 else 0 end)sm_adebreq_qci2_8,
         |sum(case when Interface=5 and ProcedureType=21 and keyword1=0 and ((bearer0qci =1 and (bearer0status=5 or bearer0status=6)) or
         |(bearer1qci =1 and (bearer1status=5 or bearer1status=6)) or (bearer2qci =1 and (bearer2status=5 or bearer2status=6)) or
         |(bearer3qci =1 and (bearer3status=5 or bearer3status=6)) or (bearer4qci =1 and (bearer4status=5 or bearer4status=6)) or
         |(bearer5qci =1 and (bearer5status=5 or bearer5status=6)) or (bearer6qci =1 and (bearer6status=5 or bearer6status=6)) or
         |(bearer7qci =1 and (bearer7status=5 or bearer7status=6)) or (bearer8qci =1 and (bearer8status=5 or bearer8status=6)))then 1 else 0 end)nbrreqrelenb_qci1_erab,
         |sum(case when Interface=5 and ProcedureType=21 and keyword1=0 and ((bearer0qci =1 and (bearer0status=5 or bearer0status=6)) or
         |(bearer1qci =1 and (bearer1status=5 or bearer1status=6)) or (bearer2qci =1 and (bearer2status=5 or bearer2status=6)) or
         |(bearer3qci =1 and (bearer3status=5 or bearer3status=6)) or (bearer4qci =1 and (bearer4status=5 or bearer4status=6)) or
         |(bearer5qci =1 and (bearer5status=5 or bearer5status=6)) or (bearer6qci =1 and (bearer6status=5 or bearer6status=6)) or
         |(bearer7qci =1 and (bearer7status=5 or bearer7status=6)) or (bearer8qci =1 and (bearer8status=5 or bearer8status=6)))then 1 else 0 end)nbrreqrelenb_qci1,
         |sum(case when ProcedureType=16 and keyword1=1 and ProcedureStatus=0 then 1 else 0 end)s1hooutsucc,
         |sum(case when ProcedureType=16 and keyword1=1 then 1 else 0 end)s1hoout,
         |sum(case when ProcedureType=15 and keyword1=1 and ProcedureStatus=0 then 1 else 0 end)s1hoinsucc,
         |sum(case when ProcedureType=15 and keyword1=1 then 1 else 0 end)s1hoin,
         |sum(case when interface=5 and procedureType=16 and bearer0qci=1 and  bearer0status=2 then 1 else 0 end)ERAB_NbrS1HoFail_qci1_1,
         |sum(case when interface=5 and procedureType=16 and bearer1qci=1 and  bearer1status=2 then 1 else 0 end)ERAB_NbrS1HoFail_qci1_2,
         |sum(case when interface=5 and procedureType=16 and bearer2qci=1 and  bearer2status=2 then 1 else 0 end)ERAB_NbrS1HoFail_qci1_3,
         |sum(case when interface=5 and procedureType=16 and bearer3qci=1 and  bearer3status=2 then 1 else 0 end)ERAB_NbrS1HoFail_qci1_4,
         |sum(case when interface=5 and procedureType=16 and bearer4qci=1 and  bearer4status=2 then 1 else 0 end)ERAB_NbrS1HoFail_qci1_5,
         |sum(case when interface=5 and procedureType=16 and bearer5qci=1 and  bearer5status=2 then 1 else 0 end)ERAB_NbrS1HoFail_qci1_6,
         |sum(case when interface=5 and procedureType=16 and bearer6qci=1 and  bearer6status=2 then 1 else 0 end)ERAB_NbrS1HoFail_qci1_7,
         |sum(case when interface=5 and procedureType=16 and bearer7qci=1 and  bearer7status=2 then 1 else 0 end)ERAB_NbrS1HoFail_qci1_8,
         |sum(case when interface=5 and procedureType=14 and bearer0qci=1 and  bearer0status=2 then 1 else 0 end)ERAB_NbrX2HoFail_qci1_1,
         |sum(case when interface=5 and procedureType=14 and bearer1qci=1 and  bearer1status=2 then 1 else 0 end)ERAB_NbrX2HoFail_qci1_2,
         |sum(case when interface=5 and procedureType=14 and bearer2qci=1 and  bearer2status=2 then 1 else 0 end)ERAB_NbrX2HoFail_qci1_3,
         |sum(case when interface=5 and procedureType=14 and bearer3qci=1 and  bearer3status=2 then 1 else 0 end)ERAB_NbrX2HoFail_qci1_4,
         |sum(case when interface=5 and procedureType=14 and bearer4qci=1 and  bearer4status=2 then 1 else 0 end)ERAB_NbrX2HoFail_qci1_5,
         |sum(case when interface=5 and procedureType=14 and bearer5qci=1 and  bearer5status=2 then 1 else 0 end)ERAB_NbrX2HoFail_qci1_6,
         |sum(case when interface=5 and procedureType=14 and bearer6qci=1 and  bearer6status=2 then 1 else 0 end)ERAB_NbrX2HoFail_qci1_7,
         |sum(case when interface=5 and procedureType=14 and bearer7qci=1 and  bearer7status=2 then 1 else 0 end)ERAB_NbrX2HoFail_qci1_8,
         |sum(case when interface=5 and procedureType=20 and procedurestatus=0 then 1 else 0 end)enbreleseSucc
         |FROM
         |	tb_xdr_ifc_s1mme_cache T
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin).registerTempTable("s1mme_tmp")

    val s1mme = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	srqatt,
         |	srqsucc,
         |	tauatt,
         |	tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	wirelessdrop,
         |	wireless,
         |	eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	eabs1swy,
         |	s1tox2swx,
         |	s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	swy,
         |	attachx,
         |	attachy,
         |0 AS voltesucc,
         |srvccsuccS1,
         |s1contextbuild,
         |enbrelese,
         |nenbrelese,
         |remaincontext,
         |0 as srvccsucc_Sv,
         |srvccatt_s1,
         |(erabbuildreq1+erabbuildreq2+erabbuildreq3+erabbuildreq4+erabbuildreq5+erabbuildreq6+erabbuildreq7+erabbuildreq8)erabbuildreq,
         |(erabbuildsucc1+erabbuildsucc2+erabbuildsucc3+erabbuildsucc4+erabbuildsucc5+erabbuildsucc6+erabbuildsucc7+erabbuildsucc8)erabbuildsucc,
         |s1contextbuildsucc,
         |(sm_adebreq_qci1_1+sm_adebreq_qci1_2+sm_adebreq_qci1_3+sm_adebreq_qci1_4+sm_adebreq_qci1_5+sm_adebreq_qci1_6+sm_adebreq_qci1_7+sm_adebreq_qci1_8)sm_adebreq_qci,
         |(sm_adebsucc_qci1_1+sm_adebsucc_qci1_2+sm_adebsucc_qci1_3+sm_adebsucc_qci1_4+sm_adebsucc_qci1_5+sm_adebsucc_qci1_6+sm_adebsucc_qci1_7+sm_adebsucc_qci1_8)sm_adebsucc_qci1,
         |(sm_adebreq_qci2_1+sm_adebreq_qci2_2+sm_adebreq_qci2_3+sm_adebreq_qci2_4+sm_adebreq_qci2_5+sm_adebreq_qci2_6+sm_adebreq_qci2_7+sm_adebreq_qci2_8)sm_adebreq_qci2,
         |(sm_adebsucc_qci2_1+sm_adebsucc_qci2_2+sm_adebsucc_qci2_3+sm_adebsucc_qci2_4+sm_adebsucc_qci2_5+sm_adebsucc_qci2_6+sm_adebsucc_qci2_7+sm_adebsucc_qci2_8)sm_adebsucc_qci2,
         |nbrreqrelenb_qci1_erab,
         |nbrreqrelenb_qci1,
         |s1hooutsucc,
         |s1hoout,
         |s1hoinsucc,
         |s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |(ERAB_NbrS1HoFail_qci1_1+ERAB_NbrS1HoFail_qci1_2+ERAB_NbrS1HoFail_qci1_3+ERAB_NbrS1HoFail_qci1_4+
         |ERAB_NbrS1HoFail_qci1_5+ERAB_NbrS1HoFail_qci1_6+ERAB_NbrS1HoFail_qci1_7+ERAB_NbrS1HoFail_qci1_8)ERAB_NbrS1HoFail_qci1,
         |(ERAB_NbrX2HoFail_qci1_1+ERAB_NbrX2HoFail_qci1_2+ERAB_NbrX2HoFail_qci1_3+ERAB_NbrX2HoFail_qci1_4+
         |ERAB_NbrX2HoFail_qci1_6+ERAB_NbrX2HoFail_qci1_7+ERAB_NbrX2HoFail_qci1_8)ERAB_NbrX2HoFail_qci1,
         |enbreleseSucc
         |FROM
         |s1mme_tmp
       """.stripMargin)

    val s1mmeHandOver = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	count(1) AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	count(1) AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	(
         |		SELECT DISTINCT
         |			S1MME_1.*
         |		FROM
         |			(
         |				SELECT
         |					*
         |				FROM
         |					tb_xdr_ifc_s1mme_cache
         |				WHERE
         |			 PROCEDURETYPE = 16
         |				AND keyword1 = 1
         |				AND PROCEDURESTATUS = 0
         |				AND IMSI IS NOT NULL
         |			) S1MME_1
         |		LEFT JOIN (
         |			SELECT
         |				*
         |			FROM
         |				tb_xdr_ifc_s1mme_cache
         |			WHERE
         |			PROCEDURETYPE = 20
         |			AND requestcause = 2
         |			AND IMSI IS NOT NULL
         |		) S1MME_2 ON S1MME_1.IMSI = S1MME_2.IMSI
         |		AND S1MME_1.CELLID = S1MME_2.CELLID
         |		WHERE
         |			S1MME_2.PROCEDURESTARTTIME BETWEEN S1MME_1.PROCEDURESTARTTIME
         |		AND S1MME_1.PROCEDURESTARTTIME + 8 * 1000
         |	) a
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin)
    s1mmeHandOver.createOrReplaceTempView("s1mmeHandOvertable")

    val s1mmeHandOver_srvccsucc = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 as swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |count(1) as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	(
         |		SELECT DISTINCT
         |			S1MME_1.*
         |		FROM
         |			(
         |				SELECT
         |					*
         |				FROM
         |    tb_xdr_ifc_s1mme_cache
         |				WHERE
         |				 PROCEDURETYPE = 16
         |				AND keyword1 = 3
         |				AND PROCEDURESTATUS = 0
         |				AND IMSI IS NOT NULL
         |			) S1MME_1
         |		LEFT JOIN (
         |			SELECT
         |				*
         |			FROM
         |				tb_xdr_ifc_s1mme_cache
         |			WHERE
         |				PROCEDURETYPE = 20
         |			AND requestcause = 2
         |			AND IMSI IS NOT NULL
         |		) S1MME_2 ON S1MME_1.IMSI = S1MME_2.IMSI
         |		AND S1MME_1.CELLID = S1MME_2.CELLID
         |		WHERE
         |			S1MME_2.PROCEDURESTARTTIME BETWEEN S1MME_1.PROCEDURESTARTTIME
         |		AND S1MME_1.PROCEDURESTARTTIME + 8 * 1000
         |	) a
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin)

    s1mmeHandOver_srvccsucc.createOrReplaceTempView("s1mmeHandOver_srvccsucctable")

    val rx = sql(
      s"""
         |SELECT
         |t1.imsi,
         |t1.msisdn,
         |t1.CELLID,
         |0 AS voltemcsucc,
         |0 AS voltemcatt,
         |0 AS voltevdsucc,
         |0 AS voltevdatt,
         |0 AS voltetime,
         |0 AS voltemctime,
         |0 AS voltemctimey,
         |0 AS voltevdtime,
         |0 AS voltevdtimey,
         |count(distinct(case when t1.Interface = 26 and t1.ProcedureType = 4 and t1.MEDIATYPE = 0
         |and t1.AbortCause in (0, 1, 2, 4)  and t2.procedurestarttime is not null then t1.xdrid end))voltemchandover,
         |0 AS volteanswer,
         |count(distinct(case when t1.Interface = 26 and t1.ProcedureType = 4 and t1.MEDIATYPE = 1
         |and t1.AbortCause in (0, 1, 2, 4) and t2.procedurestarttime is not null then t1.xdrid end))voltevdhandover,
         |0 AS voltevdanswer,
         |0 AS srvccsucc,
         |0 AS srvccatt,
         |0 as srvcctime,
         |0 AS lteswsucc,
         |0 AS lteswatt,
         |0 AS srqatt,
         |0 AS srqsucc,
         |0 AS tauatt,
         |0 AS tausucc,
         |0 AS rrcrebuild,
         |0 AS rrcsucc,
         |0 AS rrcreq,
         |0 AS imsiregatt,
         |0 AS imsiregsucc,
         |0 AS wirelessdrop,
         |0 AS wireless,
         |0 AS eabdrop,
         |0 AS eab,
         |0 AS eabs1swx,
         |0 AS eabs1swy,
         |0 AS s1tox2swx,
         |0 AS s1tox2swy,
         |0 AS enbx2swx,
         |0 AS enbx2swy,
         |0 AS uuenbswx,
         |0 AS uuenbswy,
         |0 AS uuenbinx,
         |0 AS uuenbiny,
         |0 AS swx,
         |0 AS swy,
         |0 AS attachx,
         |0 AS attachy,
         |0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |$SDB.tb_xdr_ifc_gxrx t1
         |left join (select t11.* FROM
         |	$SDB.TB_XDR_IFC_mw t11
         |WHERE
         |	t11.dt = $ANALY_DATE AND t11.h = $ANALY_HOUR and t11.ProcedureType = 5
         |AND t11.interface = 14
         |AND t11.ServiceType = $ServiceTypeaudio
         |AND t11.alertingtime <> 4294967295 and (t11.sourceneip in (select * from $DDB.mw_ip) or
         |t11.destneip in (select * from $DDB.mw_ip))) t2
         |on t1.imsi=t2.imsi
         |WHERE
         |t1.dt = $ANALY_DATE
         |AND t1.h = $ANALY_HOUR and t1.procedurestarttime>=t2.procedurestarttime
         |and t1.procedurestarttime<=t2.procedureendtime
         |GROUP BY
         |t1.cellid,
         |t1.imsi,
         |t1.msisdn
       """.stripMargin)
    rx.createOrReplaceTempView("rx_temp")
//    val tmp=uu.union(x2).union(sv).union(voltesip).union(voltesip0).union(voltesip1).union(s1mme).union(s1mmeHandOver)
//    tmp.createOrReplaceTempView("tmp")
//    sql("select * from tmp limit 1").show(1)
//    uu.union(x2).union(voltesip).union(voltesip0).union(voltesip1).union(s1mme).union(s1mmeHandOver).createOrReplaceTempView("temp_kpi")
    uu.union(x2).union(sv).union(voltesip).union(voltesip0).union(voltesip1).union(s1mme).union(s1mmeHandOver).union(rx).union(x2_enbx2).union(s1mmeHandOver_srvccsucc).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |	imsi,
         |  '' as imei,
         |	msisdn,
         |	CELLID,
         | '$cal_date' as ttime,
         |	sum(voltemcsucc) as voltemcsucc,
         |	sum(voltemcatt) as voltemcatt,
         |	sum(voltevdsucc) as voltevdsucc,
         |	sum(voltevdatt) as voltevdatt,
         |	sum(voltetime) as voltetime,
         |	sum(voltemctime) as voltemctime,
         |	sum(voltemctimey) as voltemctimey,
         |  sum(voltevdtime) as voltevdtime,
         |  sum(voltevdtimey) as voltevdtimey,
         |	sum(voltemchandover) as voltemchandover,
         |	sum(volteanswer) as volteanswer,
         |	sum(voltevdhandover) as voltevdhandover,
         |	sum(voltevdanswer) as voltevdanswer,
         |	sum(srvccsucc) as srvccsucc,
         |	sum(srvccatt) as srvccatt,
         |	sum(srvcctime) as srvcctime,
         |	sum(lteswsucc) as lteswsucc,
         |	sum(lteswatt) as lteswatt,
         |	sum(srqatt) as srqatt,
         |	sum(srqsucc) as srqsucc,
         |	sum(tauatt) as tauatt,
         |	sum(tausucc) as tausucc,
         |	sum(rrcrebuild) as rrcrebuild,
         |	sum(rrcsucc) as rrcsucc,
         |	sum(rrcreq) as rrcreq,
         |	sum(imsiregatt) as imsiregatt,
         |	sum(imsiregsucc) as imsiregsucc,
         |	sum(wirelessdrop) as wirelessdrop,
         |	sum(wireless) as wireless,
         |	sum(eabdrop) as eabdrop,
         |	sum(eab) as eab,
         |	sum(eabs1swx) as eabs1swx,
         |	sum(eabs1swy) as eabs1swy,
         |	sum(s1tox2swx) as s1tox2swx,
         |	sum(s1tox2swy) as s1tox2swy,
         |	sum(enbx2swx) as enbx2swx,
         |	sum(enbx2swy) as enbx2swy,
         |	sum(uuenbswx) as uuenbswx,
         |	sum(uuenbswy) as uuenbswy,
         |	sum(uuenbinx) as uuenbinx,
         |	sum(uuenbiny) as uuenbiny,
         |	sum(swx) as swx,
         |	sum(swy) as swy,
         |	sum(attachx) as attachx,
         |	sum(attachy) as attachy,
         |	sum(voltesucc) as voltesucc,
         |  sum(srvccsuccS1) as srvccsuccS1,
         |  sum(s1contextbuild) as s1contextbuild,
         |sum(enbrelese) as enbrelese,
         |sum(nenbrelese) as nenbrelese,
         |sum(s1ContextBuildSucc+s1hoinsucc+enbx2insucc-enbreleseSucc-s1hooutsucc-enbx2swx) as remaincontext,
         |sum(srvccsucc_Sv) as srvccsucc_Sv,
         |sum(srvccatt_s1) as srvccatt_s1,
         |sum(erabbuildreq),
         |sum(erabbuildsucc),
         |sum(s1contextbuildsucc),
         |sum(sm_adebreq_qci),
         |sum(sm_adebsucc_qci1),
         |sum(sm_adebreq_qci2),
         |sum(sm_adebsucc_qci2),
         |sum(nbrreqrelenb_qci1_erab),
         |sum(nbrreqrelenb_qci1),
         |sum(s1hooutsucc),
         |sum(s1hoout),
         |sum(s1hoinsucc),
         |sum(s1hoin),
         |sum(voltecallingmcsucc),
         |sum(voltecallingmcatt),
         |sum(voltecalledmcsucc),
         |sum(voltecalledmcatt),
         |sum(voltecallingvdsucc),
         |sum(voltecallingvdatt),
         |sum(voltecalledvdsucc),
         |sum(voltecalledvdatt),
         |sum(voltemcnetsucc),
         |sum(voltemcnetatt),
         |sum(voltevdnetsucc),
         |sum(voltevdnetatt),
         |sum(voltecallingmctime),
         |sum(voltecallingvdtime),
         |sum(srvccsucc_s1),
         |sum(enbx2insucc),
         |sum(enbx2inatt),
         |sum(ERAB_NbrS1HoFail_qci1),
         |sum(ERAB_NbrX2HoFail_qci1),
         |sum(enbreleseSucc)
         |from temp_kpi
         |group by imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin).createOrReplaceTempView("volte_gt_user_ana_base60_tmp")
    sql(s"""select * from volte_gt_user_ana_base60_tmp""").write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/volte_gt_user_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
//    sql(
//      s"""
//         |select t11.* from volte_gt_user_ana_base60_tmp t11 inner join
//         |(select cellid from $DDB.ltecell group by cellid) t12 on t11.cellid=t12.cellid
//       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/volte_gt_user_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def cellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table volte_gt_cell_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    val uu = sql(
      s"""
         |SELECT
         |	cellid AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	sum(
         |		CASE
         |		WHEN (
         |			proceduretype = 7
         |			OR proceduretype = 8
         |		)
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) lteswsucc,
         |	sum(
         |		CASE
         |		WHEN (
         |			proceduretype = 7
         |			OR proceduretype = 8
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 4 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcrebuild,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 8
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbswx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 8 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbswy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbinx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbiny,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_UU
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	CELLID
       """.stripMargin)
    val x2 = sql(
      s"""
         |SELECT
         |	CELLID  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) enbx2swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND (
         |			ProcedureStatus = 0
         |			OR (
         |				(
         |					ProcedureStatus BETWEEN 1
         |					AND 255
         |				)
         |				AND (
         |					failurecause != 1000
         |					OR failurecause IS NULL
         |				)
         |			)
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swx,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND (
         |			ProcedureStatus = 0
         |			OR (
         |				(
         |					ProcedureStatus BETWEEN 1
         |					AND 255
         |				)
         |				AND (
         |					failurecause != 1000
         |					OR failurecause IS NULL
         |				)
         |			)
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_X2
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	CELLID
       """.stripMargin)

    val x2_enbx2 = sql(
      s"""
         |SELECT
         |	(targetcellid)CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         | 0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         0 AS remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |SUM(CASE WHEN proceduretype=1 and procedurestatus=0 then 1 else 0 end)enbx2insucc,
         |SUM(CASE WHEN proceduretype=1 then 1 else 0 end)enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_X2
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	targetcellid
       """.stripMargin)

    val sv = sql(
      s"""
         |SELECT
         |	(SOURCEECI)  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srvccsucc,
         |	0 AS srvccatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			SVDELAY/$timetr
         |		END
         |	) srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	)srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_SV
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	SOURCEECI
       """.stripMargin)
    val voltesip = sql(
      s"""
         |SELECT
         |	(sourceeci)  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14 and t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = 1 and t2.MWIP is not null THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	$SDB.TB_XDR_IFC_MW t1
         | left join $DDB.MW_IP t2 on t1.sourceneip=t2.MWIP
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	sourceeci
       """.stripMargin)
    val voltesip0 = sql(
      s"""
         |SELECT
         |CELLID,
         |sum(voltemcsucc) voltemcsucc,
         |sum(voltemcatt) voltemcatt,
         |	sum(voltevdsucc) voltevdsucc,
         |	sum(voltevdatt) voltevdatt,
         |	sum(voltetime) voltetime,
         |	sum(voltemctime) voltemctime,
         | sum(voltemctimey) voltemctimey,
         |	sum(voltevdtime) voltevdtime,
         | sum(voltevdtimey) voltevdtimey,
         |	0 AS voltemchandover,
         |	sum(volteanswer) volteanswer,
         |	0 AS voltevdhandover,
         |	sum(voltevdanswer) voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	sum(voltesucc) voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |sum(voltecallingmcsucc)voltecallingmcsucc,
         |sum(voltecallingmcatt)voltecallingmcatt,
         |sum(voltecalledmcsucc)voltecalledmcsucc,
         |sum(voltecalledmcatt)voltecalledmcatt,
         |sum(voltecallingvdsucc)voltecallingvdsucc,
         |sum(voltecallingvdatt)voltecallingvdatt,
         |sum(voltecalledvdsucc)voltecalledvdsucc,
         |sum(voltecalledvdatt)voltecalledvdatt,
         |sum(voltemcnetsucc)voltemcnetsucc,
         |sum(voltemcnetatt)voltemcnetatt,
         |sum(voltevdnetsucc)voltevdnetsucc,
         |sum(voltevdnetatt)voltevdnetatt,
         |sum(voltecallingmctime)voltecallingmctime,
         |sum(voltecallingvdtime)voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |voltesip0table
         |GROUP BY
         |	CELLID
       """.stripMargin)

    val voltesip1 = sql(
      s"""
         |SELECT
         |	CELLID,
         |sum(voltemcsucc) voltemcsucc,
         |sum(voltemcatt) voltemcatt,
         |	sum(voltevdsucc) voltevdsucc,
         |	sum(voltevdatt) voltevdatt,
         |	sum(voltetime) voltetime,
         |	sum(voltemctime) voltemctime,
         | sum(voltemctimey) voltemctimey,
         |	sum(voltevdtime) voltevdtime,
         | sum(voltevdtimey) voltevdtimey,
         |	0 AS voltemchandover,
         |	sum(volteanswer) volteanswer,
         |	0 AS voltevdhandover,
         |	sum(voltevdanswer) voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	sum(voltesucc) voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |sum(voltecallingmcsucc)voltecallingmcsucc,
         |sum(voltecallingmcatt)voltecallingmcatt,
         |sum(voltecalledmcsucc)voltecalledmcsucc,
         |sum(voltecalledmcatt)voltecalledmcatt,
         |sum(voltecallingvdsucc)voltecallingvdsucc,
         |sum(voltecallingvdatt)voltecallingvdatt,
         |sum(voltecalledvdsucc)voltecalledvdsucc,
         |sum(voltecalledvdatt)voltecalledvdatt,
         |sum(voltemcnetsucc)voltemcnetsucc,
         |sum(voltemcnetatt)voltemcnetatt,
         |sum(voltevdnetsucc)voltevdnetsucc,
         |sum(voltevdnetatt)voltevdnetatt,
         |sum(voltecallingmctime)voltecallingmctime,
         |sum(voltecallingvdtime)voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |voltesip1table
         |GROUP BY
         |	CELLID
       """.stripMargin)

    val s1mme = sql(
      s"""
         |SELECT
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	sum(srvccatt),
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	sum(srqatt),
         |	sum(srqsucc),
         |	sum(tauatt),
         |	sum(tausucc),
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	sum(wirelessdrop),
         |	sum(wireless),
         |	sum(eabdrop),
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	sum(eabs1swy),
         |	sum(s1tox2swx),
         |	sum(s1tox2swy),
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	sum(swy),
         |	sum(attachx),
         |	sum(attachy),
         |0 AS voltesucc,
         |sum(srvccsuccS1),
         |sum(s1contextbuild),
         |sum(enbrelese),
         |sum(nenbrelese),
         |sum(remaincontext),
         |0 as srvccsucc_Sv,
         |sum(srvccatt_s1),
         |sum((erabbuildreq1+erabbuildreq2+erabbuildreq3+erabbuildreq4+erabbuildreq5+erabbuildreq6+erabbuildreq7+erabbuildreq8))erabbuildreq,
         |sum((erabbuildsucc1+erabbuildsucc2+erabbuildsucc3+erabbuildsucc4+erabbuildsucc5+erabbuildsucc6+erabbuildsucc7+erabbuildsucc8))erabbuildsucc,
         |sum(s1contextbuildsucc),
         |sum((sm_adebreq_qci1_1+sm_adebreq_qci1_2+sm_adebreq_qci1_3+sm_adebreq_qci1_4+sm_adebreq_qci1_5+sm_adebreq_qci1_6+sm_adebreq_qci1_7+sm_adebreq_qci1_8))sm_adebreq_qci,
         |sum((sm_adebsucc_qci1_1+sm_adebsucc_qci1_2+sm_adebsucc_qci1_3+sm_adebsucc_qci1_4+sm_adebsucc_qci1_5+sm_adebsucc_qci1_6+sm_adebsucc_qci1_7+sm_adebsucc_qci1_8))sm_adebsucc_qci1,
         |sum((sm_adebreq_qci2_1+sm_adebreq_qci2_2+sm_adebreq_qci2_3+sm_adebreq_qci2_4+sm_adebreq_qci2_5+sm_adebreq_qci2_6+sm_adebreq_qci2_7+sm_adebreq_qci2_8))sm_adebreq_qci2,
         |sum((sm_adebsucc_qci2_1+sm_adebsucc_qci2_2+sm_adebsucc_qci2_3+sm_adebsucc_qci2_4+sm_adebsucc_qci2_5+sm_adebsucc_qci2_6+sm_adebsucc_qci2_7+sm_adebsucc_qci2_8))sm_adebsucc_qci2,
         |sum(nbrreqrelenb_qci1_erab),
         |sum(nbrreqrelenb_qci1),
         |sum(s1hooutsucc),
         |sum(s1hoout),
         |sum(s1hoinsucc),
         |sum(s1hoin),
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |sum(ERAB_NbrS1HoFail_qci1_1+ERAB_NbrS1HoFail_qci1_2+ERAB_NbrS1HoFail_qci1_3+ERAB_NbrS1HoFail_qci1_4+
         |ERAB_NbrS1HoFail_qci1_5+ERAB_NbrS1HoFail_qci1_6+ERAB_NbrS1HoFail_qci1_7+ERAB_NbrS1HoFail_qci1_8)ERAB_NbrS1HoFail_qci1,
         |sum(ERAB_NbrX2HoFail_qci1_1+ERAB_NbrX2HoFail_qci1_2+ERAB_NbrX2HoFail_qci1_3+ERAB_NbrX2HoFail_qci1_4+
         |ERAB_NbrX2HoFail_qci1_6+ERAB_NbrX2HoFail_qci1_7+ERAB_NbrX2HoFail_qci1_8)ERAB_NbrX2HoFail_qci1,
         |sum(enbreleseSucc)
         |FROM
         |s1mme_tmp
         |group by cellid
       """.stripMargin)

    val s1mmeHandOver = sql(
      s"""
         |SELECT
         |	CELLID  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	sum(eabs1swx) AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	sum(swx) AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	s1mmeHandOvertable a
         |GROUP BY
         |	CELLID
       """.stripMargin)

    val s1mmeHandOver_srvccsucc = sql(
      s"""
         |SELECT
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 as swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |sum(srvccsucc_s1) as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |	s1mmeHandOver_srvccsucctable a
         |GROUP BY
         |	CELLID
       """.stripMargin)


    val rx = sql(
      s"""
         |SELECT
         |CELLID,
         |0 AS voltemcsucc,
         |0 AS voltemcatt,
         |0 AS voltevdsucc,
         |0 AS voltevdatt,
         |0 AS voltetime,
         |0 AS voltemctime,
         |0 AS voltemctimey,
         |0 AS voltevdtime,
         |0 AS voltevdtimey,
         |sum(voltemchandover)voltemchandover,
         |0 AS volteanswer,
         |sum(voltevdhandover)voltevdhandover,
         |0 AS voltevdanswer,
         |0 AS srvccsucc,
         |0 AS srvccatt,
         |0 as srvcctime,
         |0 AS lteswsucc,
         |0 AS lteswatt,
         |0 AS srqatt,
         |0 AS srqsucc,
         |0 AS tauatt,
         |0 AS tausucc,
         |0 AS rrcrebuild,
         |0 AS rrcsucc,
         |0 AS rrcreq,
         |0 AS imsiregatt,
         |0 AS imsiregsucc,
         |0 AS wirelessdrop,
         |0 AS wireless,
         |0 AS eabdrop,
         |0 AS eab,
         |0 AS eabs1swx,
         |0 AS eabs1swy,
         |0 AS s1tox2swx,
         |0 AS s1tox2swy,
         |0 AS enbx2swx,
         |0 AS enbx2swy,
         |0 AS uuenbswx,
         |0 AS uuenbswy,
         |0 AS uuenbinx,
         |0 AS uuenbiny,
         |0 AS swx,
         |0 AS swy,
         |0 AS attachx,
         |0 AS attachy,
         |0 AS voltesucc,
         | 0 AS srvccsuccS1,
         | 0 as s1contextbuild,
         |0 as enbrelese,
         |0 as nenbrelese,
         |0 as remaincontext,
         |0 as srvccsucc_Sv,
         |0 as srvccatt_s1,
         |0 as erabbuildreq,
         |0 as erabbuildsucc,
         |0 as s1contextbuildsucc,
         |0 as sm_adebreq_qci,
         |0 as sm_adebsucc_qci1,
         |0 as sm_adebreq_qci2,
         |0 as sm_adebsucc_qci2,
         |0 as nbrreqrelenb_qci1_erab,
         |0 as nbrreqrelenb_qci1,
         |0 as s1hooutsucc,
         |0 as s1hoout,
         |0 as s1hoinsucc,
         |0 as s1hoin,
         |0 as voltecallingmcsucc,
         |0 as voltecallingmcatt,
         |0 as voltecalledmcsucc,
         |0 as voltecalledmcatt,
         |0 as voltecallingvdsucc,
         |0 as voltecallingvdatt,
         |0 as voltecalledvdsucc,
         |0 as voltecalledvdatt,
         |0 as voltemcnetsucc,
         |0 as voltemcnetatt,
         |0 as voltevdnetsucc,
         |0 as voltevdnetatt,
         |0 as voltecallingmctime,
         |0 as voltecallingvdtime,
         |0 as srvccsucc_s1,
         |0 as enbx2insucc,
         |0 as enbx2inatt,
         |0 as ERAB_NbrS1HoFail_qci1,
         |0 as ERAB_NbrX2HoFail_qci1,
         |0 as enbreleseSucc
         |FROM
         |rx_temp
         |GROUP BY
         |cellid
       """.stripMargin)
//    uu.union(x2).union(voltesip).union(voltesip0).union(voltesip1).union(s1mme).union(s1mmeHandOver).createOrReplaceTempView("temp_kpi")
    uu.union(x2).union(sv).union(voltesip).union(voltesip0).union(voltesip1).union(s1mme).union(s1mmeHandOver).union(s1mmeHandOver_srvccsucc).union(rx).union(x2_enbx2).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |'$cal_date' as ttime,
         |	CELLID,
         |	sum(voltemcsucc) as voltemcsucc,
         |	sum(voltemcatt) as voltemcatt,
         |	sum(voltevdsucc) as voltevdsucc,
         |	sum(voltevdatt) as voltevdatt,
         |	sum(voltetime) as voltetime,
         |	sum(voltemctime) as voltemctime,
         |	sum(voltemctimey) as voltemctimey,
         |  sum(voltevdtime) as voltevdtime,
         |  sum(voltevdtimey) as voltevdtimey,
         |	sum(voltemchandover) as voltemchandover,
         |	sum(volteanswer) as volteanswer,
         |	sum(voltevdhandover) as voltevdhandover,
         |	sum(voltevdanswer) as voltevdanswer,
         |	sum(srvccsucc) as srvccsucc,
         |	sum(srvccatt) as srvccatt,
         |	sum(srvcctime) as srvcctime,
         |	sum(lteswsucc) as lteswsucc,
         |	sum(lteswatt) as lteswatt,
         |	sum(srqatt) as srqatt,
         |	sum(srqsucc) as srqsucc,
         |	sum(tauatt) as tauatt,
         |	sum(tausucc) as tausucc,
         |	sum(rrcrebuild) as rrcrebuild,
         |	sum(rrcsucc) as rrcsucc,
         |	sum(rrcreq) as rrcreq,
         |	sum(imsiregatt) as imsiregatt,
         |	sum(imsiregsucc) as imsiregsucc,
         |	sum(wirelessdrop) as wirelessdrop,
         |	sum(wireless) as wireless,
         |	sum(eabdrop) as eabdrop,
         |	sum(eab) as eab,
         |	sum(eabs1swx) as eabs1swx,
         |	sum(eabs1swy) as eabs1swy,
         |	sum(s1tox2swx) as s1tox2swx,
         |	sum(s1tox2swy) as s1tox2swy,
         |	sum(enbx2swx) as enbx2swx,
         |	sum(enbx2swy) as enbx2swy,
         |	sum(uuenbswx) as uuenbswx,
         |	sum(uuenbswy) as uuenbswy,
         |	sum(uuenbinx) as uuenbinx,
         |	sum(uuenbiny) as uuenbiny,
         |	sum(swx) as swx,
         |	sum(swy) as swy,
         |	sum(attachx) as attachx,
         |	sum(attachy) as attachy,
         |	sum(voltesucc) as voltesucc,
         | sum(srvccsuccS1) as srvccsuccS1,
         | sum(s1contextbuild) as s1contextbuild,
         |sum(enbrelese) as enbrelese,
         |sum(nenbrelese) as nenbrelese,
         |sum(s1ContextBuildSucc+s1hoinsucc+enbx2insucc-enbreleseSucc-s1hooutsucc-enbx2swx) as remaincontext,
         |sum(srvccsucc_Sv) as srvccsucc_Sv,
         |sum(srvccatt_s1) as srvccatt_s1,
         |sum(erabbuildreq),
         |sum(erabbuildsucc),
         |sum(s1contextbuildsucc),
         |sum(sm_adebreq_qci),
         |sum(sm_adebsucc_qci1),
         |sum(sm_adebreq_qci2),
         |sum(sm_adebsucc_qci2),
         |sum(nbrreqrelenb_qci1_erab),
         |sum(nbrreqrelenb_qci1),
         |sum(s1hooutsucc),
         |sum(s1hoout),
         |sum(s1hoinsucc),
         |sum(s1hoin),
         |sum(voltecallingmcsucc),
         |sum(voltecallingmcatt),
         |sum(voltecalledmcsucc),
         |sum(voltecalledmcatt),
         |sum(voltecallingvdsucc),
         |sum(voltecallingvdatt),
         |sum(voltecalledvdsucc),
         |sum(voltecalledvdatt),
         |sum(voltemcnetsucc),
         |sum(voltemcnetatt),
         |sum(voltevdnetsucc),
         |sum(voltevdnetatt),
         |sum(voltecallingmctime),
         |sum(voltecallingvdtime),
         |sum(srvccsucc_s1),
         |sum(enbx2insucc),
         |sum(enbx2inatt),
         |sum(ERAB_NbrS1HoFail_qci1),
         |sum(ERAB_NbrX2HoFail_qci1),
         |sum(enbreleseSucc)
         |FROM
         |	temp_kpi
         |GROUP BY
         |	cellid
       """.stripMargin).createOrReplaceTempView("volte_gt_cell_ana_base60_tmp")
    sql(s"""select * from volte_gt_cell_ana_base60_tmp""").write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/volte_gt_cell_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
//      sql(
//        s"""
//           |select t11.* from volte_gt_cell_ana_base60_tmp t11 inner join
//           |(select distinct(cellid) from $DDB.ltecell) t12 on t11.cellid=t12.cellid
//       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/volte_gt_cell_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def mrImsiHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table mr_gt_user_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	xdrid,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			KPI1 - 141
         |		ELSE
         |			0
         |		END
         |	) avgrsrpx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) commy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI3 IS NOT NULL THEN
         |			KPI3 * 0.5 - 20
         |		ELSE
         |			NULL
         |		END
         |	) avgrsrqx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP' AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) >- 110 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) ltecoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) <=- 110 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) weakcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (KPI1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratey,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI1 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI2 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI3 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI4 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI5 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI6 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI7 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI8 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI9 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI10 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststroy,
         |	MAX(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			NULL
         |		END
         |	) upsigrateavgmax,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			46 - KPI6
         |		ELSE
         |			0
         |		END
         |	) uebootx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uebooty,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6
         |		AND KPI10 = KPI12 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststrox,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststroy
         |FROM
         |	LTE_MRO_SOURCE
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	xdrid
       """.stripMargin).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |	imsi,
         | '',
         |	msisdn,
         | '' cellid,
         | '' rruid,
         | '' gridid,
         | '$cal_date',
         | '' dir_state,
         | '' elong,
         | '' elat,
         | sum(avgrsrpx),
         |	sum(commy),
         |	sum(avgrsrqx),
         |	sum(ltecoverratex),
         |	sum(weakcoverratex),
         |	sum(
         |		CASE
         |		WHEN overlapcoverratey > 3 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(overlapcoverratey),
         |	sum(upsigrateavgx),
         |	sum(upsigrateavgy),
         |sum(
         |		updiststroxvalue1 + updiststroxvalue2 + updiststroxvalue3 + updiststroxvalue4 + updiststroxvalue5 + updiststroxvalue6 + updiststroxvalue7 + updiststroxvalue8 + updiststroxvalue9 + updiststroxvalue10
         |	) updiststrox,
         |	sum(
         |  case when (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) is not null then
         |  (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) else 0 end
         | ) updiststroy,
         |	sum(model3diststrox) model3diststrox,
         |	sum(model3diststroy) model3diststroy,
         |  sum(uebootx),
         |	sum(uebooty),
         | max(
         |		greatest(
         |			updiststroxvalue1,
         |			updiststroxvalue2,
         |			updiststroxvalue3,
         |			updiststroxvalue4,
         |			updiststroxvalue5,
         |			updiststroxvalue6,
         |			updiststroxvalue7,
         |			updiststroxvalue8,
         |			updiststroxvalue9,
         |			updiststroxvalue10
         |		)
         |	) updiststromax,
         |	MAX(upsigrateavgmax)upsigrateavgmax
         |FROM
         |	temp_kpi
         |GROUP BY
         |	imsi,
         |	msisdn
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/mr_gt_user_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def mrCellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    val CAL_DATE = ANALY_DATE + " " + ANALY_HOUR + "00:00"
    sql(s"use $DDB")
    sql(s"alter table mr_gt_cell_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |SELECT
         |	cellid,
         |	xdrid,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			KPI1 - 141
         |		ELSE
         |			0
         |		END
         |	) avgrsrpx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) commy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI3 IS NOT NULL THEN
         |			KPI3 * 0.5 - 20
         |		ELSE
         |			0
         |		END
         |	) avgrsrqx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP' AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) ltecoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) <=- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) weakcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratey,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI1 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI2 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI3 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI4 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI5 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI6 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI7 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI8 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI9 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI10 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststroy,
         |	MAX(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			NULL
         |		END
         |	) upsigrateavgmax,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			46 - KPI6
         |		ELSE
         |			0
         |		END
         |	) uebootx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uebooty,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6
         |		AND KPI10 = KPI12 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststrox,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststroy
         |FROM
         |	$SDB.LTE_MRO_SOURCE
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	CELLID,
         |	xdrid
       """.stripMargin).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |	cellid,
         | '$cal_date',
         | '' dir_state,
         |	sum(avgrsrpx),
         |	sum(commy),
         |	sum(avgrsrqx),
         |	sum(ltecoverratex),
         |	sum(weakcoverratex),
         |	sum(
         |		CASE
         |		WHEN overlapcoverratey > 3 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(overlapcoverratey),
         |	sum(upsigrateavgx),
         |	sum(upsigrateavgy),
         |sum(
         |		updiststroxvalue1 + updiststroxvalue2 + updiststroxvalue3 + updiststroxvalue4 + updiststroxvalue5 + updiststroxvalue6 + updiststroxvalue7 + updiststroxvalue8 + updiststroxvalue9 + updiststroxvalue10
         |	) updiststrox,
         |	sum(
         |  case when (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) is not null then
         |  (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) else 0 end
         | ) updiststroy,
         |	sum(model3diststrox) model3diststrox,
         |	sum(model3diststroy) model3diststroy,
         |  sum(uebootx),
         |	sum(uebooty),
         | max(
         |		greatest(
         |			updiststroxvalue1,
         |			updiststroxvalue2,
         |			updiststroxvalue3,
         |			updiststroxvalue4,
         |			updiststroxvalue5,
         |			updiststroxvalue6,
         |			updiststroxvalue7,
         |			updiststroxvalue8,
         |			updiststroxvalue9,
         |			updiststroxvalue10
         |		)
         |	) updiststromax,
         |	MAX(upsigrateavgmax)upsigrateavgmax
         |FROM
         |	temp_kpi
         |GROUP BY
         |	cellid
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/mr_gt_cell_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

}

