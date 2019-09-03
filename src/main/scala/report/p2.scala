package report

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object p2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //
    val sc = new SparkContext(conf)
    //
    val qc = new SQLContext(sc)
    //
    val df = qc.read.parquet("d:/w.jar/allforyou/")
    //
    df.createTempView("v1")
    //
    val re = qc.sql("select provincename,cityname,count(1) rn from v1 group by provincename,cityname")
    //
    val load = ConfigFactory.load()
    //
    val prop = new Properties()
    //
    prop.setProperty("user",load.getString("jdbc.user"))
    //
    prop.setProperty("password",load.getString("jdbc.password"))
    //
    re.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),
      load.getString("jdbc.tableName"),prop)

  }

}
