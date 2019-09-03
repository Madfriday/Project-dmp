package report

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object P3RDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    //
    conf.setMaster("local[2]").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //
    val sc = new SparkContext(conf)
    //
    val lines = sc.textFile("d:/w.jar/logs.log")
    //
    val line = lines.map(_.split(",",-1)).filter(_.length >= 85).map(x =>{
      ((x(24),x(25)),1)
    })
      //
    val rec = line.reduceByKey(_+_).map(x =>{
      x._1._1+"," + x._1._2 +"," + x._2
    })
    //
  rec.saveAsTextFile("d:/w.jar/all4her")
  }

}
