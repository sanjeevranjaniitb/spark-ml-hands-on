package com.tutorial.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

/**
 * Spark Fundamentals Tutorial
 * 
 * Covers core Spark concepts including:
 * - SparkSession creation and configuration
 * - RDD operations and transformations
 * - Lazy evaluation and action triggers
 * - Basic data processing patterns
 */
object SparkFundamentals extends App {

  // SparkSession - Entry point to Spark functionality
  val spark = SparkSession.builder()
    .appName("Spark Fundamentals Tutorial")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
    .getOrCreate()

  // Suppress verbose logs
  spark.sparkContext.setLogLevel("ERROR")
  
  // Suppress Hadoop shutdown exceptions
  System.setProperty("hadoop.home.dir", "/tmp")
  System.setProperty("java.util.logging.config.file", "")

  import spark.implicits._

  println("=== SPARK FUNDAMENTALS TUTORIAL ===")
  
  // 1. RDD BASICS
  println("\n1. RDD (Resilient Distributed Dataset) Operations")
  
  val numbers = spark.sparkContext.parallelize(1 to 100)
  println(s"Created RDD with ${numbers.count()} elements")
  println(s"RDD partitions: ${numbers.getNumPartitions}")
  
  // Transformations (lazy evaluation)
  val evenNumbers = numbers.filter(_ % 2 == 0)
  val squaredNumbers = evenNumbers.map(x => x * x)
  
  println("Transformations defined (not executed yet)")
  
  // Actions (trigger execution)
  val result = squaredNumbers.take(10)
  println(s"First 10 squared even numbers: ${result.mkString(", ")}")
  
  val sum = squaredNumbers.reduce(_ + _)
  println(s"Sum of all squared even numbers: $sum")

  // 2. WORKING WITH TEXT DATA
  println("\n2. Text Data Processing")
  
  val textData = spark.sparkContext.parallelize(Seq(
    "Apache Spark is a unified analytics engine",
    "Spark provides high-level APIs in Java, Scala, Python",
    "Spark runs on Hadoop, Apache Mesos, Kubernetes",
    "Spark can access diverse data sources"
  ))
  
  val wordCounts = textData
    .flatMap(_.split(" "))
    .map(word => (word.toLowerCase, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
  
  println("Top 10 most frequent words:")
  wordCounts.take(10).foreach { case (word, count) =>
    println(s"  $word: $count")
  }

  // 3. DATAFRAME CREATION
  println("\n3. DataFrame Operations")
  
  case class Person(name: String, age: Int, city: String, salary: Double)
  
  val people = Seq(
    Person("Alice", 25, "New York", 75000),
    Person("Bob", 30, "San Francisco", 85000),
    Person("Charlie", 35, "Chicago", 70000),
    Person("Diana", 28, "Boston", 80000),
    Person("Eve", 32, "Seattle", 90000)
  )
  
  val peopleDF = people.toDF()
  
  println("DataFrame Schema:")
  peopleDF.printSchema()
  
  println("DataFrame Content:")
  peopleDF.show()
  
  // DataFrame transformations
  val highEarners = peopleDF.filter($"salary" > 75000)
  println("High earners (salary > 75000):")
  highEarners.show()
  
  val avgSalaryByCity = peopleDF
    .groupBy("city")
    .agg(avg("salary").alias("avg_salary"))
    .orderBy($"avg_salary".desc)
  
  println("Average salary by city:")
  avgSalaryByCity.show()

  println("\n=== TUTORIAL COMPLETED ===")
  
  spark.stop()
}