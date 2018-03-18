package finalproject

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import scala.collection.immutable.ListMap

object Driver extends App {

//  def emergencyByZip() = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("FinalProject").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("911.csv")

    val myTuple = lines
        .map(_.split(","))
        .map(x => (x(3), x(4)))
        .persist();

    // val accidentGroupings =  myTuple.reduceByKey((x, y) => x + ", " + y)

    val accidentCounts = myTuple.countByValue()
    val sortByKey = ListMap(accidentCounts.toSeq.sortBy(x => (x._1.toString().substring(1, 6), x._2)): _*)
    sortByKey.foreach {
      println(_)
    };
//  }
}
