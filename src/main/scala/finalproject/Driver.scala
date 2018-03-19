package finalproject

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection._
import scala.collection.immutable.ListMap

object Driver extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("FinalProject").setMaster("local[4]")
  val sc = new SparkContext(conf)

  findTopSpecificEmergencies(5)
  findTopCategories()
  findTopCategoryEmergency()
  findEmergencyByZipcode()
  findVehicleAccidentsPerMonth()
  findVehicleAccidentsPerHour()
  findFireAlarmsPerMonth()
  findFireAlarmsPerHour()
  findRespiratoryEmergenciesByMonth()
  findRespiratoryEmergenciesByHour()
  findStrokesPerHour()
  
  def findVehicleAccidentsPerMonth():Unit = {
    val file = Source.fromFile("911.csv").getLines.toList;
    val lines = sc.parallelize(file);
	val pw = new PrintWriter(new File("accidentsPerMonth.txt"));

    lines
    .map(line => line.split(","))
    .map(arr => (arr(5).split(" ")(0).split('-'), arr(4)))
    .filter(line => line._1.size == 3)
    .filter(line => line._2.contains("VEHICLE ACCIDENT"))
    .map(line => (line._1(1), 1))
    .countByKey()
    .toSeq.sortBy{ case (k, v) => -v }
    .foreach{pw.println(_)};
    pw.close();
  }

  def findVehicleAccidentsPerHour():Unit = {
    val file = Source.fromFile("911.csv").getLines.toList;
    val lines = sc.parallelize(file);
    val pw = new PrintWriter(new File("accidentsPerHour.txt"));

    lines
    .map(line => line.split(","))
    .map(arr => (arr(5).split(" "), arr(4)))
    .filter(line => line._1.size == 2)
    .filter(line => line._2.contains("VEHICLE ACCIDENT"))
    .map(line => (line._1(1).split(':')(0), line._2))
    .countByKey()
    .toSeq.sortBy{ case (k, v) => -v }
    .foreach{pw.println(_)};
    pw.close();
  }

  def findFireAlarmsPerMonth():Unit = {
    val file = Source.fromFile("911.csv").getLines.toList;
    val lines = sc.parallelize(file);
    val pw = new PrintWriter(new File("fireAlarmsPerMonth.txt"));

    lines
    .map(line => line.split(","))
    .map(arr => (arr(5).split(" ")(0).split('-'), arr(4)))
    .filter(line => line._1.size == 3)
    .filter(line => line._2.contains("FIRE ALARM"))
    .map(line => (line._1(1), line._2))
    .countByKey()
    .toSeq.sortBy{ case (k, v) => k }
    .foreach{pw.println(_)};
    pw.close();
  }

  def findFireAlarmsPerHour():Unit = {
	val file = Source.fromFile("input/911.csv").getLines.toList;
	val lines = sc.parallelize(file);
	val pw = new PrintWriter(new File("fireAlarmsPerHour.txt"));

	lines
	.map(line => line.split(","))
	.map(arr => (arr(5).split(" "), arr(4)))
	.filter(line => line._1.size == 2)
	.filter(line => line._2.contains("FIRE ALARM"))
	.map(line => (line._1(1).split(':')(0), line._2))
	.countByKey()
	.toSeq.sortBy{ case (k, v) => -v }
	.foreach{pw.println(_)};
    pw.close();
  }

  def findRespiratoryEmergenciesByMonth():Unit = {
    val file = Source.fromFile("input/911.csv").getLines.toList;
    val lines = sc.parallelize(file);
    val pw = new PrintWriter(new File("respiratoryEmergenciesByMonth.txt"));
    
    lines
    .map(line => line.split(","))
    .map(arr => (arr(5).split(" ")(0).split('-'), arr(4)))
    .filter(line => line._1.size == 3)
    .filter(line => line._2.contains("RESPIRATORY EMERGENCY"))
    .map(line => (line._1(1), line._2))
    .countByKey()
    .toSeq.sortBy{ case (k, v) => k }
    .foreach{pw.println(_)};
    pw.close();
  }

  def findRespiratoryEmergenciesByHour():Unit = {
    val file = Source.fromFile("input/911.csv").getLines.toList;
    val lines = sc.parallelize(file);
    val pw = new PrintWriter(new File("respiratoryEmergenciesByHour.txt"));
    
    lines
    .map(line => line.split(","))
    .map(arr => (arr(5).split(" "), arr(4)))
    .filter(line => line._1.size == 2)
    .filter(line => line._2.contains("RESPIRATORY EMERGENCY"))
    .map(line => (line._1(1).split(':')(0), line._2))
    .countByKey()
    .toSeq.sortBy{ case (k, v) => k }
    .foreach{pw.println(_)};
    pw.close();
  }

  def findStrokesPerHour():Unit = {
    val file = Source.fromFile("input/911.csv").getLines.toList;
    val lines = sc.parallelize(file);
    val pw = new PrintWriter(new File("strokesPerHour.txt"));

    lines
    .map(line => line.split(","))
    .map(arr => (arr(5).split(" "), arr(4)))
    .filter(line => line._1.size == 2)
    .filter(line => line._2.contains("STROKE"))
    .map(line => (line._1(1).split(':')(0), line._2))
    .countByKey()
    .toSeq.sortBy{ case (k, v) => k }
    .foreach{pw.println(_)};

    pw.close();
  }

  def findTopCategories():Unit = {
    val lines = sc.textFile("911.csv").filter(x => x.split(",")(0).trim() != "lat")
    val emergencyCategoryCounts = lines.map(line => (line.split(",")(4).trim().split(":")(0).trim(), 1)).reduceByKey(_+_)
    val result = emergencyCategoryCounts.sortBy(r => (r._2 * -1, r._1)).collect()

    val pw = new PrintWriter(new File("topcategories.csv"))
    pw.println("Category,Count")
    result.foreach(x => pw.println(x._1.trim() + "," + x._2))
    pw.close()
  }

  def findTopSpecificEmergencies(n: Int):Unit = {
    val lines = sc.textFile("911.csv").filter(x => x.split(",")(0).trim() != "lat")
    val specificEmergencyCounts = lines.map(line => (line.split(",")(4).trim(), 1)).reduceByKey(_+_)
    val result = specificEmergencyCounts.sortBy(r => (r._2 * -1, r._1)).collect().take(n)

    val pw = new PrintWriter(new File("topspecificemergencies.csv"))
    pw.println("Emergency,Count")
    result.foreach(x => pw.println(x._1.replace("-", "").trim() + "," + x._2))
    pw.close()
  }

  def findTopCategoryEmergency():Unit = {
    val lines = sc.textFile("911.csv").filter(x => x.split(",")(0).trim() != "lat")
    val specificEmergencyCounts = lines.map(line => (line.split(",")(4).trim(), 1)).reduceByKey(_+_)
    val categoryEmergency = specificEmergencyCounts.map(x => (x._1.split(":")(0).trim(), (x._1.split(":")(1).trim(), x._2)))
    val result = categoryEmergency.reduceByKey((x,y) => if (math.max(x._2, y._2) == x._2) (x._1, x._2) else (y._1, y._2)).collect()

    val pw = new PrintWriter(new File("topcategoryemergency.csv"))
    pw.println("Category,Emergency,Count")
    result.foreach(x => pw.println(x._1 + "," + x._2._1 + "," + x._2._2))
    pw.close()
  }
  
  def findEmergencyByZipcode():Unit = {
    val lines = sc.textFile("911.csv")
    val myTuple = lines
    .map(_.split(","))
    .map(x => (x(3), x(4)))
    .persist()

    val accidentCounts = myTuple.countByValue()
    
    val pw = new PrintWriter(new File("emergencyByZipcode.txt"))
    val sortByKey = ListMap(accidentCounts.toSeq.sortBy(x => (x._1.toString().substring(1, 6), x._2)): _*)
    .sortByKey.foreach {
      pw.println(_)
    } 
    pw.close()
  }
}
