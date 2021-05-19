package com.dtmobile.spark.job

import com.dtmobile.spark.Analyse
import com.dtmobile.spark.biz.nssp.ImsiFil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

/**
  * AnalyJob
  *
  * @author heyongjin
  * create 2017/03/02 11:10
  *  $ANALY_DATE $ANALY_HOUR liaoning lndcl $MASTER ifdayanaly
  **/
class ImsiFillJob(args: Array[String]) extends Analyse {
  override val appName: String = this.getClass.getName
  override val master: String = args(0)
  override val sourceDir: String = ""
  val warhouseDir: String = ""
  val month = args(1)
  val city = args(2)
  val day = args(3)
  override def analyse(implicit sparkSession: SparkSession): Unit = {
  val iF  = new ImsiFil(month,city,day)
//  iF.analyse
  }
}
object ImsiFillJob {
  def main(args: Array[String]): Unit = {
    val imsiFillJob = new ImsiFillJob(args: Array[String])
    imsiFillJob.exec()
  }
}


