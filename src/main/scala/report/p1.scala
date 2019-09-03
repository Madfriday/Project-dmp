package report

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object p1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //
    val sc = new SparkContext(conf)
    val qc = new SQLContext(sc)
    //
    val lines = qc.read.parquet("d:/w.jar/allforyou/")
    //
    lines.createTempView("v1")
    //
    val re = qc.sql("select provincename,cityname,count(1) from v1 group by provincename,cityname")
    //
    re.coalesce(1).write.json("d:/w.jar/all4me/")


  }

}
