package com.dtmobile.spark.job

import java.util.concurrent._

import com.dtmobile.spark.{Analyse, SparkSessionSingleton}
import com.dtmobile.spark.biz.kpi.{KpiDayAnaly, KpiHourAnaly, KpibusinessDayAnaly, KpibusinessHourAnaly}
import com.dtmobile.spark.biz.nssp.{NsspAnaly, QueryPt}
import com.dtmobile.util.DateUtils
import org.apache.spark.sql.SparkSession
import com.dtmobile.spark.biz.businessexception.businessexception
import com.dtmobile.spark.biz.businesstypedetail.businesstypedetail
import com.dtmobile.spark.biz.topcell.TopCellQualityCheckAnalyze
import org.apache.spark.SparkConf


/**
  * AnalyJob
  *
  * @author heyongjin
  * create 2017/03/02 11:10
  *  $ANALY_DATE $ANALY_HOUR liaoning lndcl $MASTER ifdayanaly
  **/
class AnalyJob(args: Array[String]) extends Analyse {
  override val appName: String = this.getClass.getName
  override val master: String = args(4)
  override val sourceDir: String = args(6)
  val warhouseDir: String = "/user/hive/warehouse/" + args(3) + ".db"
//  override val warhouseDir: String = "/"+args(2)
  val onoff=args(7).toInt

  override def analyse(implicit sparkSession: SparkSession): Unit = {
//      var threadPool: ExecutorService = null
//      try {
//        threadPool = Executors.newFixedThreadPool(1)
//        val future = new FutureTask[String](new Callable[String] {
//        override def call(): String = {
          val nsspAnaly = new NsspAnaly(args(0), args(1), args(2), args(3), sourceDir, warhouseDir, args(5))
          val kpiHourAnaly = new KpiHourAnaly(args(0), args(1), args(2), args(3), warhouseDir)
          val kpibusinessHourAnaly = new KpibusinessHourAnaly(args(0), args(1), args(2), args(3), warhouseDir)
          val exception = new businessexception(args(0), args(1), args(2), args(3), warhouseDir, args(5))
          val typedetail = new businesstypedetail(args(0), args(1), args(2), args(3), warhouseDir)
          val queryPt = new QueryPt(args(0), args(1), args(2), args(3), "gelq", "/user/hive/warehouse/gelq.db")
          nsspAnaly.analyse
          kpibusinessHourAnaly.analyse
          kpiHourAnaly.analyse
          exception.analyse
          typedetail.analyse
          queryPt.analyse

          if ("03".equals(args(1))) {
            val kpiDayAnALY = new KpiDayAnaly(DateUtils.addDay(args(0), -1, "yyyyMMdd"), args(2), args(3), warhouseDir)
            val kpibusinessDayAnaly = new KpibusinessDayAnaly(DateUtils.addDay(args(0), -1, "yyyyMMdd"), args(2), args(3), warhouseDir)
            val topCellQualityCheckAnalyze = new TopCellQualityCheckAnalyze(args(0), args(3), warhouseDir, args(5))
            kpiDayAnALY.analyse
            kpibusinessDayAnaly.analyse
            topCellQualityCheckAnalyze.analyse
          }
//          "SUCCESS!!!"
        }
//      })
//      threadPool.execute(future)
//      println("future get : " + future.get(180, TimeUnit.MINUTES))
//        )  }catch{
//      case ex:TimeoutException =>
//        println("TIMEOUT!!!!!!!!!!!")
//        sparkSession.sparkContext.stop(
      /*
      val conf = new SparkConf().setAppName(appName).setMaster(master)
      val sparkSessionNew = SparkSessionSingleton.getInstance(conf)
      analyse(sparkSessionNew)*/
//      case ex:Exception => println(ex.printStackTrace());sparkSession.sparkContext.stop();
//    }finally {
//        threadPool.shutdown()
//    }
      /* val splitData = new SplitData(args(0), args(1), args(2), args(3), sourceDir, warhouseDir)

       splitData.analyse*/


      /* val init = new Init(args(0), args(1), args(2), args(3),warhouseDir,args(5),sourceDir)
       val overcover =new Overcover(args(0), args(1), args(2), args(3),warhouseDir)
       val disturbAnalysis =new  DisturbAnalysnis(args(0), args(1),"1","1",args(2),args(3),warhouseDir)
       val disturbMixAna =new DisturbMixAna(args(0), args(1),"1","1",args(2),args(3),warhouseDir)
       val disturbSecAna =new DisturbSecAna(args(0), args(1),"1","1",args(2),args(3),warhouseDir)
       val lteMroAdjCoverAna =new LteMroAdjCoverAna(args(0), args(1),"1",args(2),args(3),warhouseDir)
       val pCIOptimize = new PCIOptimize(args(0), args(1), args(2), args(3), warhouseDir)
       val weakcover = new Weakcover(args(0), args(1), args(2), args(3), warhouseDir)
       val gridCover = new GridCover(args(0), args(1), args(2), args(3), warhouseDir)


        init.analyse
        overcover.analyse
        disturbAnalysis.analyse
        disturbMixAna.analyse
        disturbSecAna.analyse
        lteMroAdjCoverAna.analyse
        pCIOptimize.analyse
        weakcover.analyse
        gridCover.analyse*/


      ////伪基站
      /*
          val finit = new FakeDataInit(args(0), args(1), args(2), args(3),warhouseDir,args(5))
          val fAnaly = new FakeDataAnaly(args(0),args(1), args(2), args(3), warhouseDir,args(5))

          if ( "1".equals(args(7)) ){
            finit.analyse
          }
          else if ( "0".equals(args(7)) ){
            fAnaly.analyse
          }
      */

//  }
}

object AnalyJob {
  def main(args: Array[String]): Unit = {
    val analyJob = new AnalyJob(args: Array[String])
    analyJob.exec()
  }
}
