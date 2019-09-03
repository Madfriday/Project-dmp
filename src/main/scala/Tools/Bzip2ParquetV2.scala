package Tools

import Utils.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Bzip2ParquetV2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //
    val sc = new SparkContext(conf)
    //
    val qc = new SQLContext(sc)
    //
    qc.setConf("spark.sql.parquet.compression.codec","snappy")
    //
    val lines = sc.textFile("d:/w.jar/logs.log")
    //
    val line: RDD[Log] = lines.map(_.split(",", -1)).filter(_.length >= 85).map(x => {
      Log.apply(x)
    })
    //
    val df = qc.createDataFrame(line)
    //
    df.write.partitionBy("provincename","cityname").parquet("d:/w.jar/all4you/")
    //
    sc.stop()
  }
}
