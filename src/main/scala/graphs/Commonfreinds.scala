package graphs

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Commonfreinds {
  def main(args: Array[String]): Unit = {
    //
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    //
    val sc = new SparkContext(conf)
    //
    val uc: RDD[(VertexId, (String, Int))] = sc.parallelize(Seq(
      (1, ("万壑船", 23)), (2, ("王琳", 24)), (6, ("小镇子", 24)), (19, ("狗子萧", 25)), (133, ("黄兴", 30)),
      (16, ("流程请", 27)), (21, ("小南", 22)), (44, ("崔文加", 23)), (138, ("张贤", 31)), (5, ("小柯", 22)),
      (7, ("南邓", 23)), (158, ("陆玲", 21))
    ))
    //
    val ue: RDD[Edge[Int]] = sc.parallelize(Seq(Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(9, 133, 0),
      Edge(6, 133, 0),
      Edge(6, 138, 0),
      Edge(16, 138, 0),
      Edge(44, 138, 0),
      Edge(21, 138, 0),
      Edge(5, 158, 0),
      Edge(7, 158, 0)))
//
    val gr = Graph(uc,ue)
    //
    val re = gr.connectedComponents().vertices
    //
    uc.join(re).map{
      case (userId,((name,age),commonid)) =>
        //
        (commonid,List(name,age))
    }.reduceByKey(_++_).foreach(println)





  }
}
