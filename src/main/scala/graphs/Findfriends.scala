package graphs

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Findfriends {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    val sc = new SparkContext(conf)
    //
    val lines = sc.textFile("d:/w.jar/ff.txt").map(_.split("\t"))
    //
    val uv= lines.flatMap(x => {
      //
      val name = x.filter(_.indexOf(":") == -1)
      //
      val tags = x.filter(_.indexOf(":") != -1).toList
      //
      name.map(x => {
        (x.hashCode.toLong, (x, tags))
      })
    })
    //
    val ue = lines.flatMap(x => {
      val name = x.filter(_.indexOf(":") == -1)
      //
      name.map(x => {
        Edge(name(0).hashCode.toLong, x.hashCode.toLong,0)
      })

    })
    //
    val graph = Graph(uv,ue)
    //
    val result = graph.connectedComponents().vertices
    //
    result.join(uv).map{
      case (userid, (cmid,(name,tags))) => (cmid,(name,tags))
    }.reduceByKey((x,y) =>{
      (x._1+","+y._1,x._2++y._2)
    }).foreach(println)

  }

}
