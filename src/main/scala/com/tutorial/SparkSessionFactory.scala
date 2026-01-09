package com.tutorial

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(appName: String): SparkSession = {
    // Set system properties to suppress Hadoop noise
    System.setProperty("jdk.security.auth.subject.doAs.useJaasSubject", "false")
    System.setProperty("hadoop.home.dir", "/tmp")
    System.setProperty("java.util.logging.config.file", "")
    
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    
    // Suppress all verbose logging
    spark.sparkContext.setLogLevel("ERROR")
    
    spark
  }
}
