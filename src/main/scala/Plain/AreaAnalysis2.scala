package Plain

import java.util.Properties


import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

object AreaAnalysis2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //
    val sc = new SparkContext(conf)
    //
    val qc = new SQLContext(sc)
    //
    val lines = qc.read.parquet("d:/w.jar/allforyou/")
    //
    val rdd = lines.rdd
    //
    val re = rdd.map(x => {
      val effrequest = if (x(8) == 1 && x(35).asInstanceOf[Int] >= 2) 1 else 0
      val adrequest = if (x(8) == 1 && x(35).asInstanceOf[Int] >= 3) 1 else 0
      val bidnum = if (x(30) == "1" && x(31) == "1" && x(39) == "1" && x(2) != 0) 1 else 0
      val sucessnum = if (x(30) == "1" && x(31) == "1" && x(42) == "1") 1 else 0
      ((x(24), x(25)), (effrequest, adrequest, bidnum, sucessnum))

    })
    //
    val re1 = re.reduceByKey((x,y) =>{
      (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4)
    })
    //
    val va = re1.map(x =>{
      Row(x._1._1,x._1._2,x._2._1,x._2._2,x._2._3)
    })
    //
    val schema = StructType(List(StructField("shen",StringType),
      StructField("shi",StringType),
      StructField("eff",IntegerType),
      StructField("adre",IntegerType),
      StructField("bid",IntegerType)))
//
    val l = qc.createDataFrame(va,schema)
    //
    l.write.json("d:")
    l.write.parquet("d")
    val load = ConfigFactory.load()
    //
    val prop = new Properties()
    //
    prop.setProperty("user",load.getString("jdbc.user"))
    //
    prop.setProperty("password",load.getString("jdbc.password"))
    //
    l.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),
      load.getString("jdbc.tableName"),prop)

  }
}
