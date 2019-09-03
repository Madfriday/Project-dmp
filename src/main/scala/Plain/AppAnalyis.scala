package Plain


import org.apache.spark.{SparkConf, SparkContext}

object AppAnalyis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    val sc = new SparkContext(conf)
    //
    val relu = sc.textFile("d:/w.jar/app.txt")
    val relus = relu.map(x =>{
      val appid = x(0).toInt
      val appname = x(1)
      (appid,appname)
    }).collect().toMap
    //
    val broadcast = sc.broadcast(relus)
    //
    val lines = sc.textFile("d:/w.jar/logs.log")
    //
   val va =  lines.map(_.split(",",-1)).filter(_.length >= 85).filter(x =>{
     !x(15).isEmpty || !x(67).isEmpty
   }).map(x =>{
     var appname = x(16)
     val appid = x(15).toInt
     val log = broadcast.value
     if("".equals(x(16))){
       appname = log.getOrElse(appid,"nono").toString
     }
     val effrequest = if (x(8).toInt == 1 && x(35).toInt >= 2) 1 else 0
     val adrequest = if (x(8).toInt == 1 && x(35).toInt >= 3) 1 else 0
     val bidnum = if (x(30) == "1" && x(31) == "1" && x(39) == "1" && x(2) != 0) 1 else 0
     val sucessnum = if (x(30) == "1" && x(31) == "1" && x(42) == "1") 1 else 0
     ((appname),(effrequest,adrequest,bidnum,sucessnum))
   })
    //
    val v1 = va.reduceByKey((x,y) =>{
      (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4)
    })
    //
    val re = v1.map(x =>{
      (x._1+","+x._2)
    })
    println(re.collect().toBuffer)

  }

}
