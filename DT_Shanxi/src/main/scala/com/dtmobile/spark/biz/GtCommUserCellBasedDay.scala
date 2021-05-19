package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/26/0026.
  */
class GtCommUserCellBasedDay(ANALY_DATE: String,DDB: String,warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + "00:00:00"

  var non_gtsubpulse_commusers = 30
  var non_gtsubpuse_times = 10
  var busiCellTimes=3

  def analy(implicit sparkSession: SparkSession): Unit ={
    gtCommUserCell
  }

  def gtCommUserCell(implicit sparkSession: SparkSession): Unit ={

    import  sparkSession.sql

    val t= sql("""select non_gtsubpulse_commusers,non_gtsubpuse_times,busiCellTimes from gt_capacity_config""").collectAsList()

    if(t.size()>0){
      non_gtsubpulse_commusers = t.get(0).getAs("non_gtsubpulse_commusers")
      non_gtsubpuse_times = t.get(0).getAs("non_gtsubpuse_times")
      busiCellTimes = t.get(0).getAs("busiCellTimes")
    }


    sql(s"""alter table $DDB.gt_commusermore_baseday drop if  exists partition(dt="$ANALY_DATE")""".stripMargin)
    sql(s"""alter table $DDB.gt_commusermore_baseday add  partition(dt=$ANALY_DATE)""".stripMargin)


    sql(
      s"""
         |select gt.line_name,
         |l.city,
         |'$cal_date' ttime,
         |l.CELLID cellid,
         |l.CELLNAME cellname,c.musers from
         |(
         |select cellid,max(husers) musers from(
         |select hours,cellid,max(users) husers
         |         from $DDB.gt_pulse_cell_min
         |          where dt="$ANALY_DATE" and sub_pulse_type=0 and users>=$non_gtsubpulse_commusers
         |          group by hours,cellid
         |          having count(1)>=$non_gtsubpuse_times
         |) a group by cellid having count(1)>$busiCellTimes
         |) c inner join ltecell l
         |on l.cellid=c.cellid
         |inner join gt_publicandprofess_new_cell gt
         |on c.cellid=gt.cell_id
         |
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/gt_commusermore_baseday/dt=$ANALY_DATE""")



  }


}
