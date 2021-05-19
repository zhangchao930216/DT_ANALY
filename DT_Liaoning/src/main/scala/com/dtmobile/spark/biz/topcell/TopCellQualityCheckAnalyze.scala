package com.dtmobile.spark.biz.topcell

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * TOP/质检小区分析
  *
  * @param analyDate 分析日期
  * @param dcl hive数据库
  * @param warhouseDir 数据结果保存路径
  * @param orcl 门限Oracle数据库
  */
class TopCellQualityCheckAnalyze(analyDate:String, dcl:String, warhouseDir:String, orcl:String) {
  val hourCond = "(08,09,10,11,12,13,14,15,16,17,18,19,20,21,22)"
  val wirelesscode = "(1,2,3,4,11,15,17,18,19,23,24,25,26,27,28)"

  //获取时间SQL
  def GetDateSQL(analyDate:String, dayNum:Int):String ={
    var date=""
    var dayTmp=""
    for(i <- 1 to dayNum){
      if(dayTmp == ""){
        dayTmp = analyDate
      }else{
        dayTmp  = getYesterday(dayTmp)
      }
      date += " vgu.dt = "+getYesterday(dayTmp) +" or"
    }
    date = date.substring(0,date.length-2)
    date
  }

  //Top小区SQL
  def GetExceptionTmpSQL(dcl:String,date:String,exDate:String,hourNum:Integer,dayNum:Integer,strConSQL:String, eType:Integer,
                         topTip:String, qualityTip:String, isDispatch:Integer):String ={
//    println(s"date: $date\n")
//    println(s"exDate: $exDate\n")
    var strSQL:String = ""
    if ( isDispatch == 0){
      strSQL =s"""
             |select exx.cellid, $eType eType, exx.falurecause, exx.cellregion, exx.prointerface, exx.exceptioncode,
             |sum(1)over(partition by exx.cellid) as cellcount,
             |mapf.meaning descf,mapf.suggestion suggestionf,mapw.meaning descw,mapw.suggestion suggestionw,
             |'$topTip' topTip, '$qualityTip' qualityTip from (
             |select * from $dcl.exception_analysis ex where ex.etype = $eType and ($exDate) and ex.h in $hourCond
             |) exx inner join (
             |select cellid from (
             |select cellid, count(1), sum(case when hourcount >= $hourNum then 1 else 0 end) as daycount from (
             |select cellid, dt, count(1) as hourcount from $dcl.volte_gt_cell_ana_base60 vgu
             |where ($date) and vgu.h in $hourCond and
             |$strConSQL
             |group by cellid, dt) t1
             |group by cellid) t2
             |where daycount >= $dayNum
             |) kpi
             |on exx.cellid = kpi.cellid
             |left join $dcl.EXCEPTIONMAP mapf on exx.prointerface=mapf.interface and exx.falurecause=mapf.code
             |left join $dcl.EXCEPTIONMAP mapw on "UU"=mapw.interface and exx.exceptioncode=mapw.code
             |where (exx.exceptioncode is not null and exx.exceptioncode <> '')
             |or (exx.falurecause is not null and exx.falurecause <> '')
             |or (exx.cellregion is not null and exx.cellregion <> '')
             |""".stripMargin
    }else{ //这个是一个查询为空的SQL，为了后面的union all
      strSQL =s"""
             |select 0 cellid, $eType eType, '' falurecause, '' cellregion, '' prointerface, '' exceptioncode,
             |0 cellcount,
             |'' descf,'' suggestionf,'' descw,'' suggestionw,
             |'$topTip' topTip, '$qualityTip' qualityTip from $dcl.ltecell where cellid=-1
             |""".stripMargin
    }

    strSQL
  }

  //质检SQL
  def GetExceptionTmpSQL_z(dcl:String,date:String,exDate:String,hourNum:Integer,dayNum:Integer,strConSQL:String,
                           eType:Integer, isDispatch:Integer):String ={
//    println(s"date: $date\n")
//    println(s"exDate: $exDate\n")
    var strSQL:String = ""
    if ( isDispatch == 0){
      strSQL =s"""
             |select cellid, $eType eType from (
             |select cellid, count(1), sum(case when hourcount >= $hourNum then 1 else 0 end) as daycount from (
             |select cellid, dt, count(1) as hourcount from $dcl.volte_gt_cell_ana_base60 vgu
             |where ($date) and vgu.h in $hourCond and
             |$strConSQL
             |group by cellid, dt) t1
             |group by cellid) t2
             |where daycount >= $dayNum
             |""".stripMargin
    }else{ //这个是一个查询为空的SQL，为了后面的union all
      strSQL =s"""
             |select 0 cellid, $eType eType from $dcl.ltecell where cellid=-1
             |""".stripMargin
    }

    strSQL
  }

  //S1初始上下文建立失败TOP小区
  val s1Context = 3
  var s1ContextDispatch:Integer = 1 //派单开关
  var s1ContextSuccRatio:Integer = 90 //小时级S1初始上下文建立成功率	90	%	(0，100]，整型，默认<
  var s1ContextFailCnt:Integer = 50 //小时级S2初始上下文建立失败次数	50	次	(0，任意]，整型，默认>
  var s1ContextHour:Integer = 5 //质差小时数	5	个	(0，15]，整形，默认>=
  var s1ContextDay:Integer = 3 //连续质差天数	3	天	(0，任意]，整型，默认=
  var s1ContextDate = ""
  var s1ContextExDate = ""
  var s1ContextTip = ""
  var s1ContextSQL = ""
  //S1初始上下文建立失败质检小区
  var z_s1ContextDispatch:Integer = 1 //派单开关
  var z_s1ContextSuccRatio:Integer = 92 //小时级S1初始上下文建立成功率	92	%	(0，100]，整型，默认>=
  var z_s1ContextHour:Integer = 13 //达标小时数	13	个	(0，15]，整形，默认>=
  var z_s1ContextDay:Integer = 3 //连续达标天数	3	天	(0，任意]，整型，默认=
  var z_s1ContextDate = ""
  var z_s1ContextExDate = ""
  var z_s1ContextTip = ""
  var z_s1ContextSQL = ""
  def GetS1ContextSQLInfo(analyDate:String):Unit ={
    s1ContextTip = s"告警门限：小时级S1初始上下文建立成功率<$s1ContextSuccRatio%，小时级S2初始上下文建立失败次数>${s1ContextFailCnt}次，质差小时数>=${s1ContextHour}小时，连续质差天数>=${s1ContextDay}天；~"
    z_s1ContextTip = s"；~质检门限：小时级S1初始上下文建立成功率>=$z_s1ContextSuccRatio%，达标小时数>=${z_s1ContextHour}小时，连续达标天数>=${z_s1ContextDay}天；"
    s1ContextSQL = s"""
                  |(s1contextbuild-wireless) > ${s1ContextFailCnt} and
                  |wireless > 0 and ((s1contextbuild-wireless)/wireless)*100 < ${s1ContextSuccRatio}""".stripMargin
    z_s1ContextSQL = s"""
                  |(wireless > 0 and ((s1contextbuild-wireless)/wireless)*100 >= ${z_s1ContextSuccRatio})""".stripMargin
    s1ContextDate = GetDateSQL(analyDate, s1ContextDay)
    s1ContextExDate = s1ContextDate.replace("vgu","ex")
    z_s1ContextDate = GetDateSQL(analyDate, z_s1ContextDay)
    z_s1ContextExDate = z_s1ContextDate.replace("vgu","ex")
  }

  //TAU失败TOP小区
  val tau = 8
  var tauDispatch:Integer = 1 //派单开关
  var tauSuccRatio:Integer = 80 //小时级TAU成功率	80	%	(0，100]，整型，默认<
  var tauFailCnt:Integer = 10 //小时级TAU失败次数	10	次	(0，任意]，整型，默认>
  var tauHour:Integer = 5 //质差小时数	5	个	(0，15]，整形，默认>
  var tauDay:Integer = 3 //连续质差天数	3	天	(0，任意]，整型，默认=
  var tauDate:String = ""
  var tauExDate:String = ""
  var tauTip = ""
  var tauSQL = ""
  //TAU失败质检小区
  var z_tauDispatch:Integer = 1 //派单开关
  var z_tauSuccRatio:Integer = 85 //小时级TAU成功率	85	%	(0，100]，整型，默认>=
  var z_tauHour:Integer = 13 //达标小时数	13	个	(0，15]，整形，默认>=
  var z_tauDay:Integer = 3 //连续达标天数	3	天	(0，任意]，整型，默认=
  var z_tauDate:String = ""
  var z_tauExDate:String = ""
  var z_tauTip = ""
  var z_tauSQL = ""
  def GetTauSQLInfo(analyDate:String):Unit ={
    tauTip = s"告警门限：小时级TAU成功率<${tauSuccRatio}%，小时级TAU失败次数>${tauFailCnt}次，质差小时数>=${tauHour}小时，连续质差天数>=${tauDay}天；~"
    z_tauTip = s"；~质检门限：小时级TAU成功率>=${z_tauSuccRatio}%，达标小时数>=${z_tauHour}小时，连续达标天数>=${z_tauDay}天；"
    tauSQL = s"""
             |(tauatt-tausucc) > ${tauFailCnt} and
             |tauatt > 0 and (tausucc/tauatt)*100 < ${tauSuccRatio}""".stripMargin
    z_tauSQL = s"""
             |(tauatt > 0 and (tausucc/tauatt)*100 >= ${z_tauSuccRatio})""".stripMargin
    tauDate = GetDateSQL(analyDate, tauDay)
    tauExDate = tauDate.replace("vgu","ex")
    z_tauDate = GetDateSQL(analyDate, z_tauDay)
    z_tauExDate = z_tauDate.replace("vgu","ex")
  }

  //UE上下文异常释放TOP小区
  val ueContext = 9
  var ueContextDispatch:Integer = 1 //派单开关
  var ueContextAbnormalRatio:Integer = 2 //小时级UE上下文异常释放率	2	%	(0，100]，整型，默认>
  var ueContextAbnormalCnt:Integer = 50 //小时级UE上下文异常释放次数	50	次	(0，任意]，整型，默认>
  var ueContextHour:Integer = 5 //质差小时数	5	个	(0，15]，整形，默认>=
  var ueContextDay:Integer = 3 //连续质差天数	3	天	(0，任意]，整型，默认=
  var ueContextDate:String = ""
  var ueContextExDate:String = ""
  var ueContextTip = ""
  var ueContextSQL = ""
  //UE上下文异常释放质检小区
  var z_ueContextDispatch:Integer = 1 //派单开关
  var z_ueContextAbnormalRatio:Integer = 1 //小时级UE上下文异常释放率	1	%	(0，100]，整型，默认<=
  var z_ueContextHour:Integer = 13 //达标小时数	13	个	(0，15]，整形，默认>=
  var z_ueContextDay:Integer = 3 //连续达标天数	3	天	(0，任意]，整型，默认=
  var z_ueContextDate:String = ""
  var z_ueContextExDate:String = ""
  var z_ueContextTip = ""
  var z_ueContextSQL = ""
  def GetUeContextSQLInfo(analyDate:String):Unit ={
    ueContextTip = s"告警门限：小时级UE上下文异常释放率>${ueContextAbnormalRatio}%，小时级UE上下文异常释放次数>${ueContextAbnormalCnt}次，质差小时数>=${ueContextHour}小时，连续质差天数>=${ueContextDay}天；~"
    z_ueContextTip = s"；~质检门限：小时级UE上下文异常释放率<=${z_ueContextAbnormalRatio}%，达标小时数>=${z_ueContextHour}小时，连续达标天数>=${z_ueContextDay}天；"
    ueContextSQL = s"""
                  |(enbrelese-nenbrelese) > ${ueContextAbnormalCnt} and
                  |(wireless) > 0 and ((enbrelese-nenbrelese)/(wireless))*100 > $ueContextAbnormalRatio""".stripMargin
    z_ueContextSQL = s"""
                  |((wireless) > 0 and ((enbrelese-nenbrelese)/(wireless))*100 <= $z_ueContextAbnormalRatio)""".stripMargin
    ueContextDate = GetDateSQL(analyDate, ueContextDay)
    ueContextExDate = ueContextDate.replace("vgu","ex")
    z_ueContextDate = GetDateSQL(analyDate, z_ueContextDay)
    z_ueContextExDate = z_ueContextDate.replace("vgu","ex")
  }

  //切换失败TOP小区
  val handover = 10
  var handoverDispatch:Integer = 1 //派单开关
  var handoverSuccRatio:Integer = 95 //小时级切换成功率	95	%	(0，100]，整型，默认<
  var handoverFailCnt:Integer = 100 //小时级切换失败次数	100	次	(0，任意]，整型，默认>
  var handoverHour:Integer = 5 //小时质差数	5	个	(0，15]，整形，默认>=
  var handoverDay:Integer = 3 //连续质差天数	3	天	(0，任意]，整型，默认=
  var handoverDate:String = ""
  var handoverExDate:String = ""
  var handoverTip = ""
  var handoverSQL = ""
  //切换失败质检小区
  var z_handoverDispatch:Integer = 1 //派单开关
  var z_handoverSuccRatio:Integer = 98 //小时级切换成功率	98	%	(0，100]，整型，默认>=
  var z_handoverHour:Integer = 13 //达标小时数	13	个	(0，15]，整形，默认>=
  var z_handoverDay:Integer = 3 //连续达标天数	3	天	(0，任意]，整型，默认=
  var z_handoverDate:String = ""
  var z_handoverExDate:String = ""
  var z_handoverTip = ""
  var z_handoverSQL = ""
  def GetHandoverSQLInfo(analyDate:String):Unit ={
    handoverTip = s"告警门限：小时级切换成功率<${handoverSuccRatio}%，小时级切换失败次数>${handoverFailCnt}次，质差小时数>=${handoverHour}小时，连续质差天数>=${handoverDay}天；~"
    z_handoverTip = s"；~质检门限：小时级切换成功率>=${z_handoverSuccRatio}%，达标小时数>=${z_handoverHour}小时，连续达标天数>=${z_handoverDay}天；"
    handoverSQL = s"""
                 |(lteswatt+enbx2swy+eabs1swy-lteswsucc-enbx2swx-eabs1swx) > ${handoverFailCnt} and
                 |(lteswatt+enbx2swy+eabs1swy) > 0
                 |and (lteswsucc+enbx2swx+eabs1swx)/(lteswatt+enbx2swy+eabs1swy)*100 < ${handoverSuccRatio}""".stripMargin
    z_handoverSQL = s"""
                 |((lteswatt+enbx2swy+eabs1swy) > 0
                 |and (lteswsucc+enbx2swx+eabs1swx)/(lteswatt+enbx2swy+eabs1swy)*100 >= ${z_handoverSuccRatio})""".stripMargin
    handoverDate = GetDateSQL(analyDate, handoverDay)
    handoverExDate = handoverDate.replace("vgu","ex")
    z_handoverDate = GetDateSQL(analyDate, z_handoverDay)
    z_handoverExDate = z_handoverDate.replace("vgu","ex")
  }

  //IMS注册失败TOP小区
  val imsReg = 1
  var imsRegDispatch:Integer = 1 //派单开关
  var imsRegSuccRatio:Integer = 95 //小时级IMS注册成功率	95	%	(0，100]，整型，默认<
  var imsRegCnt:Integer = 20 //小时级IMS注册次数	20	次	(0，任意]，整型，默认>
  var imsRegHour:Integer = 5 //质差小时数	5	个	(0，15]，整形，默认>
  var imsRegDay:Integer = 3 //连续质差天数	3	天	(0，任意]，整型，默认=
  var imsRegDate:String = ""
  var imsRegExDate:String = ""
  var imsRegTip = ""
  var imsRegSQL = ""
  //IMS注册失败质检小区
  var z_imsRegDispatch:Integer = 1 //派单开关
  var z_imsRegSuccRatio:Integer = 98 //小时级IMS注册成功率	98	%	(0，100]，整型，默认>=
  var z_imsRegHour:Integer = 13 //达标小时数	13	个	(0，15]，整形，默认>=
  var z_imsRegDay:Integer = 3 //连续达标天数	3	天	(0，任意]，整型，默认=
  var z_imsRegDate:String = ""
  var z_imsRegExDate:String = ""
  var z_imsRegTip = ""
  var z_imsRegSQL = ""
  def GetImsRegSQLInfo(analyDate:String):Unit ={
    imsRegTip = s"告警门限：小时级IMS注册成功率<${imsRegSuccRatio}%，小时级IMS注册次数>${imsRegCnt}次，质差小时数>=${imsRegHour}小时，连续质差天数>=${imsRegDay}天；~"
    z_imsRegTip = s"；~质检门限：小时级IMS注册成功率>=${z_imsRegSuccRatio}%，达标小时数>=${z_imsRegHour}小时，连续达标天数>=${z_imsRegDay}天；"
    imsRegSQL = s"""
               |imsiregatt > ${imsRegCnt} and
               |imsiregatt > 0 and (imsiregsucc/imsiregatt)*100 < ${imsRegSuccRatio}""".stripMargin
    z_imsRegSQL = s"""
               |(imsiregatt > 0 and (imsiregsucc/imsiregatt)*100 >= ${z_imsRegSuccRatio})""".stripMargin
    imsRegDate = GetDateSQL(analyDate, imsRegDay)
    imsRegExDate = imsRegDate.replace("vgu","ex")
    z_imsRegDate = GetDateSQL(analyDate, z_imsRegDay)
    z_imsRegExDate = z_imsRegDate.replace("vgu","ex")
  }

  //VoLTE语音未接通TOP小区
  val voLteVoice = 4
  var voLteVoiceDispatch:Integer = 1 //派单开关
  var voLteVoiceSuccRatio:Integer = 90 //小时级VoLTE语音接通率	90	%	(0，100]，整型，默认<
  var voLteVoiceCnt:Integer = 100 //小时级VoLTE语音呼叫总次数	100	次	(0，任意]，整型，默认>
  var voLteVoiceHour:Integer = 5 //质差小时数	5	个	(0，15]，整形，默认>=
  var voLteVoiceDay:Integer = 3 //连续质差天数	3	天	(0，任意]，整型，默认=
  var voLteVoiceDate:String = ""
  var voLteVoiceExDate:String = ""
  var voLteVoiceTip = ""
  var voLteVoiceSQL = ""
  //VoLTE语音未接通质检小区
  var z_voLteVoiceDispatch:Integer = 1 //派单开关
  var z_voLteVoiceSuccRatio:Integer = 95 //小时级VoLTE语音接通率	95	%	(0，100]，整型，默认>
  var z_voLteVoiceHour:Integer = 13 //达标小时数	13	个	(0，15]，整形，默认>=
  var z_voLteVoiceDay:Integer = 3 //连续达标天数	3	天	(0，任意]，整型，默认=
  var z_voLteVoiceDate:String = ""
  var z_voLteVoiceExDate:String = ""
  var z_voLteVoiceTip = ""
  var z_voLteVoiceSQL = ""
  def GetVolteVoiceSQLInfo(analyDate:String):Unit ={
    voLteVoiceTip = s"告警门限：小时级VoLTE语音接通率<${voLteVoiceSuccRatio}%，小时级VoLTE语音呼叫总次数>${voLteVoiceCnt}次，质差小时数>=${voLteVoiceHour}小时，连续质差天数>=${voLteVoiceDay}天；~"
    z_voLteVoiceTip = s"；~质检门限：小时级VoLTE语音接通率>=${z_voLteVoiceSuccRatio}%，达标小时数>=${z_voLteVoiceHour}小时，连续达标天数>=${z_voLteVoiceDay}天；"
    voLteVoiceSQL = s"""
                   |voltemcatt > ${voLteVoiceCnt} and
                   |voltemcatt > 0 and (voltemcsucc/voltemcatt)*100 < ${voLteVoiceSuccRatio}""".stripMargin
    z_voLteVoiceSQL = s"""
                   |(voltemcatt > 0 and (voltemcsucc/voltemcatt)*100 >= ${z_voLteVoiceSuccRatio})""".stripMargin
    voLteVoiceDate = GetDateSQL(analyDate, voLteVoiceDay)
    voLteVoiceExDate = voLteVoiceDate.replace("vgu","ex")
    z_voLteVoiceDate = GetDateSQL(analyDate, z_voLteVoiceDay)
    z_voLteVoiceExDate = z_voLteVoiceDate.replace("vgu","ex")
  }

  //VoLTE语音掉话TOP小区
  val voLteVoiceDrop = 5
  var voLteVoiceDropDispatch:Integer = 1 //派单开关
  var voLteVoiceDropRatio:Integer = 2 //小时级VoLTE语音掉话率	2	%	(0，100]，整型，默认>
  var voLteVoiceDropCnt:Integer = 20 //小时级VoLTE语音掉话次数	20	次	(0，任意]，整型，默认>
  var voLteVoiceDropHour:Integer = 5 //质差小时数	5	个	(0，15]，整形，默认>=
  var voLteVoiceDropDay:Integer = 3 //连续质差天数	3	天	(0，任意]，整型，默认=
  var voLteVoiceDropDate:String = ""
  var voLteVoiceDropExDate:String = ""
  var voLteVoiceDropTip = ""
  var voLteVoiceDropSQL = ""
  //VoLTE语音掉话质检小区
  var z_voLteVoiceDropDispatch:Integer = 1 //派单开关
  var z_voLteVoiceDropRatio:Integer = 1 //小时级VoLTE语音掉话率	1	%	(0，100]，整型，默认<=
  var z_voLteVoiceDropHour:Integer = 13 //达标小时数	13	个	(0，15]，整形，默认>=
  var z_voLteVoiceDropDay:Integer = 3 //连续达标天数	3	天	(0，任意]，整型，默认=
  var z_voLteVoiceDropDate:String = ""
  var z_voLteVoiceDropExDate:String = ""
  var z_voLteVoiceDropTip = ""
  var z_voLteVoiceDropSQL = ""
  def GetVolteVoiceDropSQLInfo(analyDate:String):Unit ={
    voLteVoiceDropTip = s"告警门限：小时级VoLTE语音掉话率>${voLteVoiceDropRatio}%，小时级VoLTE语音掉话次数>${voLteVoiceDropCnt}次，质差小时数>=${voLteVoiceDropHour}小时，连续质差天数>=${voLteVoiceDropDay}天；~"
    z_voLteVoiceDropTip = s"；~质检门限：小时级VoLTE语音掉话率<=${z_voLteVoiceDropRatio}%，达标小时数>=${z_voLteVoiceDropHour}小时，连续达标天数>=${z_voLteVoiceDropDay}天；"
    voLteVoiceDropSQL = s"""
                      |voltemchandover > ${voLteVoiceDropCnt} and
                      |(voltemcsucc) > 0 and (voltemchandover / (voltemcsucc))*100 > ${voLteVoiceDropRatio}""".stripMargin
    z_voLteVoiceDropSQL = s"""
                      |((voltemcsucc) > 0 and (voltemchandover / (voltemcsucc))*100 <= ${z_voLteVoiceDropRatio})""".stripMargin
    voLteVoiceDropDate = GetDateSQL(analyDate, voLteVoiceDropDay)
    voLteVoiceDropExDate = voLteVoiceDropDate.replace("vgu","ex")
    z_voLteVoiceDropDate = GetDateSQL(analyDate, z_voLteVoiceDropDay)
    z_voLteVoiceDropExDate = z_voLteVoiceDropDate.replace("vgu","ex")
  }

  //eSRVCC切换失败TOP小区
  val eSrvcc = 13
  var eSrvccDispatch:Integer = 1 //派单开关
  var eSrvccSuccRatio:Integer = 90 //小时级eSRVCC成功率	90	%	(0，100]，整型，默认<
  var eSrvccCnt:Integer = 50 //小时级eSRVCC请求次数	50	次	(0，任意]，整型，默认>
  var eSrvccHour:Integer = 5 //质差小时数	5	个	(0，15]，整形，默认>=
  var eSrvccDay:Integer = 3 //连续质差天数	3	天	(0，任意]，整型，默认=
  var eSrvccDate:String = ""
  var eSrvccExDate:String = ""
  var eSrvccTip = ""
  var eSrvccSQL = ""
  //eSRVCC切换失败质检小区
  var z_eSrvccDispatch:Integer = 1 //派单开关
  var z_eSrvccSuccRatio:Integer = 95 //小时级eSRVCC成功率	95	%	(0，100]，整型，默认>=
  var z_eSrvccHour:Integer = 13 //达标小时数	13	个	(0，15]，整形，默认>=
  var z_eSrvccDay:Integer = 3 //连续达标天数	3	天	(0，任意]，整型，默认=
  var z_eSrvccDate:String = ""
  var z_eSrvccExDate:String = ""
  var z_eSrvccTip = ""
  var z_eSrvccSQL = ""
  def GeteSrvccSQLInfo(analyDate:String):Unit ={
    eSrvccTip = s"告警门限：小时级eSRVCC成功率<${eSrvccSuccRatio}%，小时级eSRVCC请求次数>${eSrvccCnt}次，质差小时数>=${eSrvccHour}小时，连续质差天数>=${eSrvccDay}天；~"
    z_eSrvccTip = s"；~质检门限：小时级eSRVCC成功率>=${z_eSrvccSuccRatio}%，达标小时数>=${z_eSrvccHour}小时，连续达标天数>=${z_eSrvccDay}天；"
    eSrvccSQL = s"""
               |srvccatt_s1 > ${eSrvccCnt} and
               |srvccatt_s1 > 0 and (srvccsucc_Sv/srvccatt_s1)*100 < ${eSrvccSuccRatio}""".stripMargin
    z_eSrvccSQL = s"""
               |(srvccatt_s1 = 0 or (srvccsucc_Sv/srvccatt_s1)*100 >= ${z_eSrvccSuccRatio})""".stripMargin
    eSrvccDate = GetDateSQL(analyDate, eSrvccDay)
    eSrvccExDate = eSrvccDate.replace("vgu","ex")
    z_eSrvccDate = GetDateSQL(analyDate, z_eSrvccDay)
    z_eSrvccExDate = z_eSrvccDate.replace("vgu","ex")
  }

  def getCondition(sparkSession: SparkSession): Unit = {
    val oracle:String = "jdbc:oracle:thin:@"+orcl
    sparkSession.read.format("jdbc").option("url", s"$oracle")
      .option("dbtable","TABLE_PARAMSET")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("PARAMSET")

    val t = sparkSession.sql("select FIELD,cast(VALUE as int) VALUE from PARAMSET").collectAsList()
    var i = 0
    var field = ""
    val size = t.size()-1
    if ( !t.isEmpty ) {
      for ( i <- 0 to size ) {
        field = t.get(i).getString(0)
        if (field.equals("S1_single_switch_1")) s1ContextDispatch = t.get(i).getInt(1)
        else if (field.equals("S1_ratio")) s1ContextSuccRatio = t.get(i).getInt(1)
        else if (field.equals("S1_count")) s1ContextFailCnt = t.get(i).getInt(1)
        else if (field.equals("S1_quality_difference_hour")) s1ContextHour = t.get(i).getInt(1)
        else if (field.equals("S1_continuous_quality_dayCount")) s1ContextDay = t.get(i).getInt(1)
        else if (field.equals("S1_single_switch_2")) z_s1ContextDispatch = t.get(i).getInt(1)
        else if (field.equals("S1_ratio_2")) z_s1ContextSuccRatio = t.get(i).getInt(1)
        else if (field.equals("S1_reach_standard_hour_2")) z_s1ContextHour = t.get(i).getInt(1)
        else if (field.equals("S1_continuous_reach_dayCount_2")) z_s1ContextDay = t.get(i).getInt(1)
        else if (field.equals("TAU_single_switch_1")) tauDispatch = t.get(i).getInt(1)
        else if (field.equals("TAU_ratio")) tauSuccRatio = t.get(i).getInt(1)
        else if (field.equals("TAU_count")) tauFailCnt = t.get(i).getInt(1)
        else if (field.equals("TAU_quality_difference_hour")) tauHour = t.get(i).getInt(1)
        else if (field.equals("TAU_continuous_quality_dayCount")) tauDay = t.get(i).getInt(1)
        else if (field.equals("TAU_single_switch_2")) z_tauDispatch = t.get(i).getInt(1)
        else if (field.equals("TAU_ratio_2")) z_tauSuccRatio = t.get(i).getInt(1)
        else if (field.equals("TAU_reach_standard_hour_2")) z_tauHour = t.get(i).getInt(1)
        else if (field.equals("TAU_continuous_reach_dayCount_2")) z_tauDay = t.get(i).getInt(1)
        else if (field.equals("UE_single_switch_1")) ueContextDispatch = t.get(i).getInt(1)
        else if (field.equals("UE_ratio")) ueContextAbnormalRatio = t.get(i).getInt(1)
        else if (field.equals("UE_count")) ueContextAbnormalCnt = t.get(i).getInt(1)
        else if (field.equals("UE_quality_difference_hour")) ueContextHour = t.get(i).getInt(1)
        else if (field.equals("UE_continuous_quality_dayCount")) ueContextDay = t.get(i).getInt(1)
        else if (field.equals("UE_single_switch_2")) z_ueContextDispatch = t.get(i).getInt(1)
        else if (field.equals("UE_ratio_2")) z_ueContextAbnormalRatio = t.get(i).getInt(1)
        else if (field.equals("UE_reach_standard_hour_2")) z_ueContextHour = t.get(i).getInt(1)
        else if (field.equals("UE_continuous_reach_dayCount_2")) z_ueContextDay = t.get(i).getInt(1)
        else if (field.equals("SWITCH_single_switch_1")) handoverDispatch = t.get(i).getInt(1)
        else if (field.equals("SWITCH_ratio")) handoverSuccRatio = t.get(i).getInt(1)
        else if (field.equals("SWITCH_count")) handoverFailCnt = t.get(i).getInt(1)
        else if (field.equals("SWITCH_quality_difference_hour")) handoverHour = t.get(i).getInt(1)
        else if (field.equals("SWITCH_continuous_quality_dayCount")) handoverDay = t.get(i).getInt(1)
        else if (field.equals("SWITCH_single_switch_2")) z_handoverDispatch = t.get(i).getInt(1)
        else if (field.equals("SWITCH_ratio_2")) z_handoverSuccRatio = t.get(i).getInt(1)
        else if (field.equals("SWITCH_reach_standard_hour_2")) z_handoverHour = t.get(i).getInt(1)
        else if (field.equals("SWITCH_continuous_reach_dayCount_2")) z_handoverDay = t.get(i).getInt(1)
        else if (field.equals("IMS_single_switch_1")) imsRegDispatch = t.get(i).getInt(1)
        else if (field.equals("IMS_ratio")) imsRegSuccRatio = t.get(i).getInt(1)
        else if (field.equals("IMS_count")) imsRegCnt = t.get(i).getInt(1)
        else if (field.equals("IMS_quality_difference_hour")) imsRegHour = t.get(i).getInt(1)
        else if (field.equals("IMS_continuous_quality_dayCount")) imsRegDay = t.get(i).getInt(1)
        else if (field.equals("IMS_single_switch_2")) z_imsRegDispatch = t.get(i).getInt(1)
        else if (field.equals("IMS_ratio_2")) z_imsRegSuccRatio = t.get(i).getInt(1)
        else if (field.equals("IMS_reach_standard_hour_2")) z_imsRegHour = t.get(i).getInt(1)
        else if (field.equals("IMS_continuous_reach_dayCount_2")) z_imsRegDay = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_single_switch_1")) voLteVoiceDispatch = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_ratio")) voLteVoiceSuccRatio = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_count")) voLteVoiceCnt = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_quality_difference_hour")) voLteVoiceHour = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_continuous_quality_dayCount")) voLteVoiceDay = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_single_switch_2")) z_voLteVoiceDispatch = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_ratio_2")) z_voLteVoiceSuccRatio = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_reach_standard_hour_2")) z_voLteVoiceHour = t.get(i).getInt(1)
        else if (field.equals("UNCONNECTED_continuous_reach_dayCount_2")) z_voLteVoiceDay = t.get(i).getInt(1)
        else if (field.equals("DROP_single_switch_1")) voLteVoiceDropDispatch = t.get(i).getInt(1)
        else if (field.equals("DROP_ratio")) voLteVoiceDropRatio = t.get(i).getInt(1)
        else if (field.equals("DROP_count")) voLteVoiceDropCnt = t.get(i).getInt(1)
        else if (field.equals("DROP_quality_difference_hour")) voLteVoiceDropHour = t.get(i).getInt(1)
        else if (field.equals("DROP_continuous_quality_dayCount")) voLteVoiceDropDay = t.get(i).getInt(1)
        else if (field.equals("DROP_single_switch_2")) z_voLteVoiceDropDispatch = t.get(i).getInt(1)
        else if (field.equals("DROP_ratio_2")) z_voLteVoiceDropRatio = t.get(i).getInt(1)
        else if (field.equals("DROP_reach_standard_hour_2")) z_voLteVoiceDropHour = t.get(i).getInt(1)
        else if (field.equals("DROP_continuous_reach_dayCount_2")) z_voLteVoiceDropDay = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_single_switch_1")) eSrvccDispatch = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_ratio")) eSrvccSuccRatio = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_count")) eSrvccCnt = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_quality_difference_hour")) eSrvccHour = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_continuous_quality_dayCount")) eSrvccDay = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_single_switch_2")) z_eSrvccDispatch = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_ratio_2")) z_eSrvccSuccRatio = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_reach_standard_hour_2")) z_eSrvccHour = t.get(i).getInt(1)
        else if (field.equals("ESRVCC_continuous_reach_dayCount_2")) z_eSrvccDay = t.get(i).getInt(1)
      }
    }
  }

  def analyse(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql

    //注册方法getNote，用于格式化Top小区的描述字段
    sparkSession.udf.register("getNote", (strTxt:String)=>{
      var strNote = strTxt
      var i = 1
      for ( i <- 1 to 7 ){
        var strTrim:String = s"$i# "
        strNote = strNote.replace(strTrim, "")
      }
      strNote
    })

    //获取数据库里面指标门限设置
    getCondition(sparkSession)

    //获取 S1初始上下文建立失败 相关SQL等
    GetS1ContextSQLInfo(analyDate)
    val strS1ContextSQL = GetExceptionTmpSQL(dcl, s1ContextDate, s1ContextExDate,s1ContextHour,s1ContextDay,s1ContextSQL,
      s1Context, s1ContextTip, z_s1ContextTip, s1ContextDispatch) //3
    val z_strS1ContextSQL = GetExceptionTmpSQL_z(dcl, z_s1ContextDate, z_s1ContextExDate,z_s1ContextHour,z_s1ContextDay,
        z_s1ContextSQL, s1Context, z_s1ContextDispatch) //3

    //获取 TAU失败 相关SQL等
    GetTauSQLInfo(analyDate)
    val strTauSQL = GetExceptionTmpSQL(dcl, tauDate, tauExDate, tauHour, tauDay, tauSQL, tau, tauTip, z_tauTip,tauDispatch) //8
    val z_strTauSQL = GetExceptionTmpSQL_z(dcl, z_tauDate, z_tauExDate, z_tauHour, z_tauDay, z_tauSQL, tau,z_tauDispatch) //8

    //获取 UE上下文异常释放 相关SQL等
    GetUeContextSQLInfo(analyDate)
    val strUeContextSQL = GetExceptionTmpSQL(dcl, ueContextDate, ueContextExDate, ueContextHour, ueContextDay,
      ueContextSQL, ueContext, ueContextTip, z_ueContextTip,ueContextDispatch) //9
    val z_strUeContextSQL = GetExceptionTmpSQL_z(dcl, z_ueContextDate, z_ueContextExDate, z_ueContextHour, z_ueContextDay,
        z_ueContextSQL, ueContext,z_ueContextDispatch) //9

    //获取 切换失败 相关SQL等
    GetHandoverSQLInfo(analyDate)
    val strHandoverSQL = GetExceptionTmpSQL(dcl, handoverDate, handoverExDate, handoverHour, handoverDay, handoverSQL,
      handover, handoverTip, z_handoverTip,handoverDispatch) //10
    val z_strHandoverSQL = GetExceptionTmpSQL_z(dcl, z_handoverDate, z_handoverExDate, z_handoverHour, z_handoverDay,
        z_handoverSQL, handover,z_handoverDispatch) //10

    //获取 IMS注册失败 相关SQL等
    GetImsRegSQLInfo(analyDate)
    val strImsRegSQL = GetExceptionTmpSQL(dcl, imsRegDate, imsRegExDate, imsRegHour, imsRegDay, imsRegSQL, imsReg,
      imsRegTip, z_imsRegTip,imsRegDispatch) //1
    val z_strImsRegSQL = GetExceptionTmpSQL_z(dcl, z_imsRegDate, z_imsRegExDate, z_imsRegHour, z_imsRegDay, z_imsRegSQL,
        imsReg,z_imsRegDispatch) //1

    //获取 VoLTE语音未接通 相关SQL等
    GetVolteVoiceSQLInfo(analyDate)
    val strVoLteVoiceSQL = GetExceptionTmpSQL(dcl, voLteVoiceDate, voLteVoiceExDate, voLteVoiceHour, voLteVoiceDay,
      voLteVoiceSQL, voLteVoice, voLteVoiceTip, z_voLteVoiceTip,voLteVoiceDispatch) //4
    val z_strVoLteVoiceSQL = GetExceptionTmpSQL_z(dcl, z_voLteVoiceDate, z_voLteVoiceExDate, z_voLteVoiceHour, z_voLteVoiceDay,
        z_voLteVoiceSQL, voLteVoice,z_voLteVoiceDispatch) //4

    //获取 VoLTE语音掉话 相关SQL等
    GetVolteVoiceDropSQLInfo(analyDate)
    val strVoLteVoiceDropSQL = GetExceptionTmpSQL(dcl, voLteVoiceDropDate, voLteVoiceDropExDate, voLteVoiceDropHour,
      voLteVoiceDropDay, voLteVoiceDropSQL, voLteVoiceDrop, voLteVoiceDropTip, z_voLteVoiceDropTip,voLteVoiceDropDispatch) //5
    val z_strVoLteVoiceDropSQL = GetExceptionTmpSQL_z(dcl, z_voLteVoiceDropDate, z_voLteVoiceDropExDate, z_voLteVoiceDropHour,
        z_voLteVoiceDropDay, z_voLteVoiceDropSQL, voLteVoiceDrop,z_voLteVoiceDropDispatch) //5

    //获取 eSRVCC切换失败 相关SQL等
    GeteSrvccSQLInfo(analyDate)
    val streSrvccSQL = GetExceptionTmpSQL(dcl, eSrvccDate, eSrvccExDate, eSrvccHour, eSrvccDay, eSrvccSQL, eSrvcc,
      eSrvccTip, z_eSrvccTip,eSrvccDispatch) //13
    val z_streSrvccSQL = GetExceptionTmpSQL_z(dcl, z_eSrvccDate, z_eSrvccExDate, z_eSrvccHour, z_eSrvccDay, z_eSrvccSQL,
        eSrvcc,z_eSrvccDispatch) //13

    //etype 与 问题 映射SQL
    var strEType:String = ""
    strEType =
      s"""
         |case when etype=${s1Context} then 'S1初始上下文建立失败'
         |     when  etype=${tau} then 'TAU失败'
         |     when  etype=${ueContext} then 'UE上下文异常释放'
         |     when  etype=${handover} then '切换失败'
         |     when  etype=${imsReg} then 'IMS注册失败'
         |     when  etype=${voLteVoice} then 'VoLTE语音未接通'
         |     when  etype=${voLteVoiceDrop} then 'VoLTE语音掉话'
         |     when  etype=${eSrvcc} then 'eSRVCC切换失败'
         |     else null end
       """.stripMargin

    //查出TopCell相对应的异常事件
    sql(s"""drop table IF EXISTS $dcl.context_tmp""")
    sql(
      s"""
         |create table $dcl.context_tmp(
         |cellid int,
         |eType int,
         |falurecause string,
         |cellregion string,
         |prointerface string,
         |exceptioncode string,
         |cellcount int,
         |descf string,
         |suggestionf string,
         |descw string,
         |suggestionw string,
         |topTip string,
         |qualityTip string)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
         """.stripMargin)

    //查出TopCell相对应的异常事件
      sql(
      s"""
         |insert into $dcl.context_tmp select * from(
         |${strS1ContextSQL}
         |union all
         |${strTauSQL}
         |union all
         |${strUeContextSQL}
         |union all
         |${strHandoverSQL}
         |union all
         |${strImsRegSQL}
         |union all
         |${strVoLteVoiceSQL}
         |union all
         |${strVoLteVoiceDropSQL}
         |union all
         |${streSrvccSQL}
         |)
       """.stripMargin) //.createOrReplaceTempView("context_tmp")

    //统计并生成Top小区表
    sql(
      s"""
         |select "辽宁" province,
         |lte.city city,
         |'EUTRAN' rat,
         |lte.cellname cellname,
         |lte.cellname objectname,
         |${strEType} Title,
         |from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') time,
         |lte.company company,
         |from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') foundtime,
         |'TOP小区' type,
         |top
         |from (
         |select cellid,eType,
         |regexp_replace(concat(topTip,getNote(concat_ws('；~', sort_array(collect_list(top)))),qualityTip,'~告警来源：软硬采关联端到端信令分析平台'),'~','\n') top from(
         |select * from (
         |select t3.cellid,t3.eType,topTip,qualityTip,
         |(case when code in $wirelesscode then
         |concat(cast(r1 as String),'# 无线原因top', cast(r1 as String),
         |'：无线名称：', desc,
         |'，原因次数：', cast(codecnt as String),
         |'，原因占比：', cast(round(coderatio,2) as String),
         |'%，优化建议：', suggestion)
         |else
         |concat(cast(r1 as String),'# 无线原因top', cast(r1 as String),
         |'：无线名称：未发现无线问题',
         |'，原因次数：', cast(codecnt as String),
         |'，原因占比：', cast(round(coderatio,2) as String),
         |'%，优化建议：', suggestion)
         |end) top
         |from (
         |select t2.cellid,t2.eType,
         |t2.exceptioncode code,t2.topTip,t2.qualityTip,
         |min(t2.cellcount) sumcode,
         |count(t2.exceptioncode) as codecnt,
         |case when min(t2.cellcount)=0 then 0 else round(count(t2.exceptioncode)/min(t2.cellcount),4)*100 end coderatio,
         |min(descw) desc, min(suggestionw) suggestion,
         |row_number() over(partition by t2.cellid,t2.eType order by count(t2.exceptioncode) desc) r1
         |from $dcl.context_tmp t2 where t2.exceptioncode is not null and t2.exceptioncode <> ''
         |--from context_tmp t2
         |group by t2.cellid, t2.eType, t2.exceptioncode,t2.topTip,t2.qualityTip
         |) t3
         |where t3.r1 <= 3
         |union
         |select t3.cellid, t3.eType,topTip,qualityTip,
         |concat(cast(r1+3 as String),'# 信令错误码top', cast(r1 as String),
         |'：信令原因：', desc,
         |'：信令错误码：', code,
         |'，原因次数：', cast(codecnt as String),
         |'，原因占比：', cast(round(coderatio,2) as String),
         |'%，优化建议：', suggestion) top
         |from (
         |select t2.cellid,t2.eType,
         |t2.falurecause code,t2.topTip,t2.qualityTip,
         |min(t2.cellcount) sumcode,
         |count(t2.falurecause) as codecnt,
         |case when min(t2.cellcount)=0 then 0 else round(count(t2.falurecause)/min(t2.cellcount),4)*100 end coderatio,
         |min(descf) desc, min(suggestionf) suggestion,
         |row_number() over(partition by t2.cellid,t2.eType order by count(t2.falurecause) desc) r1
         |from $dcl.context_tmp t2 where t2.falurecause is not null and t2.falurecause <> ''
         |--from context_tmp t2
         |group by t2.cellid, t2.eType,t2.falurecause,t2.topTip,t2.qualityTip
         |) t3
         |where t3.r1 <= 3
         |union
         |select t3.cellid,t3.eType,topTip,qualityTip,
         |concat(cast(r1+6 as String),'# 问题归属域分析：top归属域：', code,
         |'，错误域占比：', cast(round(coderatio,2) as String),'%') top
         |from (
         |select t2.cellid,t2.eType,
         |t2.cellregion code,t2.topTip,t2.qualityTip,
         |min(t2.cellcount) sumcode,
         |count(t2.cellregion) as codecnt,
         |case when min(t2.cellcount)=0 then 0 else round(count(t2.cellregion)/min(t2.cellcount),4)*100 end coderatio,
         |'' desc, '' suggestion,
         |row_number() over(partition by t2.cellid,t2.eType order by count(t2.cellregion) desc) r1
         |from $dcl.context_tmp t2 where t2.cellregion is not null and t2.cellregion <> ''
         |--from context_tmp t2
         |group by t2.cellid, t2.eType, t2.cellregion,t2.topTip,t2.qualityTip
         |) t3
         |where t3.r1 <= 1
         |) t4
         |) t5 group by cellid,eType,topTip,qualityTip
         |) t6 inner join $dcl.ltecell lte on t6.cellid = lte.cellid
      """.stripMargin).repartition(1).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/topcell/dt=${analyDate}")//.show() //createOrReplaceTempView("note_tmp") //show()

    //统计并生成质检小区表
    sql(s"""drop table IF EXISTS $dcl.z_context_tmp""")
    sql(
      s"""
         |create table $dcl.z_context_tmp(
         |cellid int,
         |eType int)
         |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
         """.stripMargin)

    sql(
      s"""
         |insert into $dcl.z_context_tmp select * from (
         |${z_strS1ContextSQL}
         |union all
         |${z_strTauSQL}
         |union all
         |${z_strUeContextSQL}
         |union all
         |${z_strHandoverSQL}
         |union all
         |${z_strImsRegSQL}
         |union all
         |${z_strVoLteVoiceSQL}
         |union all
         |${z_strVoLteVoiceDropSQL}
         |union all
         |${z_streSrvccSQL}
         |)
       """.stripMargin
    )

    sql(
      s"""
         |select "辽宁" province,
         |lte.city city,
         |'EUTRAN' rat,
         |lte.cellname cellname,
         |lte.cellname objectname,
         |${strEType} Title,
         |from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') time,
         |lte.company company,
         |from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') foundtime,
         |'质检小区' type
         |from $dcl.z_context_tmp t6 inner join $dcl.ltecell lte on t6.cellid = lte.cellid
       """.stripMargin).repartition(1).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/qualityCheck/dt=${analyDate}")//.show()

    return
  }

  /**
    * 获取指定日期前一天
    *
    * @param date
    * @return
    */
  def getYesterday(date:String):String= {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val day:Date = dateFormat.parseObject(date).asInstanceOf[Date]
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(day)
    cal.add(Calendar.DAY_OF_MONTH, -1);
    val yesterdayTime:Date = cal.getTime
    val yesterday = dateFormat.format(yesterdayTime)
    yesterday
  }
}
/*
object TopCellQualityCheck {

  /**
    * args(0) 分析日期
    * args(1) hive数据库
    * args(2) 数据结果保存路径
    * args(3) 门限Oracle数据库
  def main(args: Array[String]): Unit = {
    if (args.length < 4 ){
      println(s"Please input four parameters, Thank you!\n")
      return
    }

//    val analysis = new TopCellQualityCheckAnalyze("20170427","result","/user/hive/warehouse/liaoning.db/","172.30.4.159:1521/ahh")
    val analysis = new TopCellQualityCheckAnalyze(args(0),args(1),args(2),args(3))
    val conf = new SparkConf().setAppName("TopCell")
    val sparkSession = SparkSessionSingleton.getInstance(conf)
    analysis.analyse(sparkSession)
  }

}*/*/
