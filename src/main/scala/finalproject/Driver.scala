package finalproject

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection._

object Driver extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("FinalProject").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val lines = sc.textFile("911.csv").filter(x => x.split(",")(0).trim() != "lat")
  findTopSpecificEmergencies(5)
  findTopCategories()
  findTopCategoryEmergency()

  def findTopCategories():Unit = {
    val emergencyCategoryCounts = lines.map(line => (line.split(",")(4).trim().split(":")(0).trim(), 1)).reduceByKey(_+_)
    val result = emergencyCategoryCounts.sortBy(r => (r._2 * -1, r._1)).collect()

    val pw = new PrintWriter(new File("topcategories.csv"))
    pw.println("Category,Count")
    result.foreach(x => pw.println(x._1.trim() + "," + x._2))
    pw.close()
  }

  def findTopSpecificEmergencies(n: Int):Unit = {
    val specificEmergencyCounts = lines.map(line => (line.split(",")(4).trim(), 1)).reduceByKey(_+_)
    val result = specificEmergencyCounts.sortBy(r => (r._2 * -1, r._1)).collect().take(n)

    val pw = new PrintWriter(new File("topspecificemergencies.csv"))
    pw.println("Emergency,Count")
    result.foreach(x => pw.println(x._1.replace("-", "").trim() + "," + x._2))
    pw.close()
  }

  def findTopCategoryEmergency():Unit = {
    val specificEmergencyCounts = lines.map(line => (line.split(",")(4).trim(), 1)).reduceByKey(_+_)
    val categoryEmergency = specificEmergencyCounts.map(x => (x._1.split(":")(0).trim(), (x._1.split(":")(1).trim(), x._2)))
    val result = categoryEmergency.reduceByKey((x,y) => if (math.max(x._2, y._2) == x._2) (x._1, x._2) else (y._1, y._2)).collect()

    val pw = new PrintWriter(new File("topcategoryemergency.csv"))
    pw.println("Category,Emergency,Count")
    result.foreach(x => pw.println(x._1 + "," + x._2._1 + "," + x._2._2))
    pw.close()
  }
}
