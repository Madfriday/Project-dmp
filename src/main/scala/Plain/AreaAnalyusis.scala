package Plain

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object AreaAnalyusis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //
    val sc = new SparkContext(conf)
    //
    val qc = new SQLContext(sc)
    //
    val lines = qc.read.parquet("d:/w.jar/allforyou/")
      //
    lines.createTempView("v1")
    //
    val re = qc.sql(
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode=1 and processnode >=2 then 1 else 0 end)a1,
        |sum(case when requestmode=1 and processnode>=3 then 1 else 0 end)a2,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid != 0 then 1 else 0 end)a3,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end)a4,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end)a5,
        |sum(case when  requestmode=3 and iseffective=1 then 1 else 0 end)a6,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0 * winprice/1000 else 0 end)a7,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0 * adpayment/1000 else 0 end)a8
        |from v1
        |group by provincename,cityname
        |""".stripMargin).show()

  }

  //


}
