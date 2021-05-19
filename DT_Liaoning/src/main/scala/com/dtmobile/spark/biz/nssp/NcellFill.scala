package com.dtmobile.spark.biz.nssp

import com.dtmobile.spark.Analyse
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhangchao15 on 2018/3/22.
  */
class NcellFill(args:Array[String]) extends Analyse {
  override val appName: String = "NcellFill"
  val ORACL = args(2)
  override val sourceDir: String = ""
  override val warhouseDir: String = "/user/hive/warehouse/" + args(0) + ".db"
  override val master: String = args(1)
  override def analyse(implicit sparkSession: SparkSession): Unit = {
    sparkSession.read.format("jdbc").option("url", s"jdbc:oracle:thin:@$ORACL")
      .option("dbtable", "ltecell")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().registerTempTable("ltecell")
    import sparkSession.sql

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

  }


}

object NcellFill{
  def main(args: Array[String]) {
    val fillBack = new NcellFill(args)
    fillBack.exec()
  }
}
