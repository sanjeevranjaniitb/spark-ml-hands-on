package com.tutorial.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
 * Structured Streaming Tutorial
 * 
 * Comprehensive guide to Spark Structured Streaming:
 * - Stream processing fundamentals
 * - Various data sources and sinks
 * - Windowing and watermarking
 * - Stateful operations and aggregations
 */
object StreamingExample extends App {

  val spark = SparkSession.builder()
    .appName("Structured Streaming Tutorial")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-streaming-checkpoint")
    .getOrCreate()

  import spark.implicits._

  println("=== STRUCTURED STREAMING TUTORIAL ===")

  // 1. BASIC STREAMING CONCEPTS
  println("\n1. Basic Streaming Setup")
  
  // Define schema for streaming data
  val userActivitySchema = StructType(Array(
    StructField("timestamp", TimestampType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("action", StringType, nullable = false),
    StructField("page", StringType, nullable = true),
    StructField("duration", IntegerType, nullable = true)
  ))
  
  // Simulate streaming data source (in production, this would be Kafka, files, etc.)
  val streamingDF = spark
    .readStream
    .format("rate") // Built-in source for testing
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("timestamp", current_timestamp())
    .withColumn("user_id", concat(lit("user_"), ($"value" % 100).cast("string")))
    .withColumn("action", 
      when($"value" % 4 === 0, "login")
      .when($"value" % 4 === 1, "view_page")
      .when($"value" % 4 === 2, "click")
      .otherwise("logout"))
    .withColumn("page", 
      when($"action" === "view_page", 
        concat(lit("page_"), (($"value" % 10) + 1).cast("string")))
      .otherwise(null))
    .withColumn("duration", 
      when($"action" === "view_page", ($"value" % 300) + 10)
      .otherwise(null))
    .select("timestamp", "user_id", "action", "page", "duration")
  
  println("Streaming DataFrame schema:")
  streamingDF.printSchema()

  // 2. BASIC STREAMING QUERY
  println("\n2. Basic Streaming Query")
  
  val basicQuery = streamingDF
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", false)
    .option("numRows", 5)
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start()
  
  println("Basic streaming query started (will run for 15 seconds)")
  Thread.sleep(15000)
  basicQuery.stop()

  // 3. STREAMING AGGREGATIONS
  println("\n3. Streaming Aggregations")
  
  val aggregatedStream = streamingDF
    .groupBy("action")
    .agg(
      count("*").alias("action_count"),
      countDistinct("user_id").alias("unique_users")
    )
  
  val aggregationQuery = aggregatedStream
    .writeStream
    .outputMode("complete") // Required for aggregations without watermark
    .format("console")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()
  
  println("Aggregation query started (will run for 20 seconds)")
  Thread.sleep(20000)
  aggregationQuery.stop()

  // 4. WINDOWED AGGREGATIONS
  println("\n4. Windowed Aggregations")
  
  val windowedStream = streamingDF
    .withWatermark("timestamp", "30 seconds") // Handle late data
    .groupBy(
      window($"timestamp", "1 minute", "30 seconds"), // 1-minute windows, sliding every 30 seconds
      $"action"
    )
    .agg(
      count("*").alias("action_count"),
      countDistinct("user_id").alias("unique_users"),
      avg("duration").alias("avg_duration")
    )
    .select(
      $"window.start".alias("window_start"),
      $"window.end".alias("window_end"),
      $"action",
      $"action_count",
      $"unique_users",
      $"avg_duration"
    )
  
  val windowQuery = windowedStream
    .writeStream
    .outputMode("append") // Can use append with watermark
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("15 seconds"))
    .start()
  
  println("Windowed aggregation query started (will run for 30 seconds)")
  Thread.sleep(30000)
  windowQuery.stop()

  // 5. STATEFUL OPERATIONS
  println("\n5. Stateful Operations - Session Analysis")
  
  // Define session timeout (5 minutes of inactivity)
  val sessionTimeout = "5 minutes"
  
  val sessionStream = streamingDF
    .withWatermark("timestamp", "10 minutes")
    .groupBy($"user_id")
    .agg(
      min($"timestamp").alias("session_start"),
      max($"timestamp").alias("session_end"),
      count("*").alias("activity_count"),
      collect_list($"action").alias("actions"),
      sum(when($"duration".isNotNull, $"duration").otherwise(0)).alias("total_duration")
    )
    .withColumn("session_duration_minutes", 
      (unix_timestamp($"session_end") - unix_timestamp($"session_start")) / 60.0)
  
  val sessionQuery = sessionStream
    .writeStream
    .outputMode("update") // Only output changed results
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()
  
  println("Session analysis query started (will run for 40 seconds)")
  Thread.sleep(40000)
  sessionQuery.stop()

  // 6. STREAM-STREAM JOINS
  println("\n6. Stream-Stream Joins")
  
  // Create two streams to demonstrate joins
  val clickStream = streamingDF
    .filter($"action" === "click")
    .select($"timestamp".alias("click_time"), $"user_id", $"page")
  
  val viewStream = streamingDF
    .filter($"action" === "view_page")
    .select($"timestamp".alias("view_time"), $"user_id", $"page", $"duration")
  
  val joinedStream = clickStream
    .withWatermark("click_time", "1 minute")
    .join(
      viewStream.withWatermark("view_time", "1 minute"),
      expr("""
        user_id = user_id AND
        page = page AND
        click_time >= view_time AND
        click_time <= view_time + interval 2 minutes
      """)
    )
    .select(
      $"user_id",
      $"page",
      $"view_time",
      $"click_time",
      $"duration",
      (unix_timestamp($"click_time") - unix_timestamp($"view_time")).alias("time_to_click")
    )
  
  val joinQuery = joinedStream
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("15 seconds"))
    .start()
  
  println("Stream-stream join query started (will run for 30 seconds)")
  Thread.sleep(30000)
  joinQuery.stop()

  // 7. CUSTOM SINK EXAMPLE
  println("\n7. Custom Sink - Memory Sink for Testing")
  
  val memorySinkQuery = streamingDF
    .groupBy($"action")
    .agg(count("*").alias("count"))
    .writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("action_counts")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start()
  
  println("Memory sink query started, will query results after 15 seconds")
  Thread.sleep(15000)
  
  // Query the in-memory table
  spark.sql("SELECT * FROM action_counts ORDER BY count DESC").show()
  
  memorySinkQuery.stop()

  // 8. ERROR HANDLING AND MONITORING
  println("\n8. Error Handling and Monitoring")
  
  val monitoredQuery = streamingDF
    .writeStream
    .outputMode("append")
    .format("console")
    .option("numRows", 3)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()
  
  // Monitor query progress
  println("Query monitoring information:")
  Thread.sleep(5000)
  
  val progress = monitoredQuery.lastProgress
  if (progress != null) {
    println(s"Batch ID: ${progress.batchId}")
    println(s"Input rows: ${progress.inputRowsPerSecond}")
    println(s"Processing time: ${progress.durationMs.get("triggerExecution")} ms")
  }
  
  monitoredQuery.stop()

  // 9. ADVANCED PATTERNS
  println("\n9. Advanced Streaming Patterns")
  
  // Deduplication
  val deduplicatedStream = streamingDF
    .withWatermark("timestamp", "1 hour")
    .dropDuplicates("user_id", "action", "timestamp")
  
  // Complex event processing
  val complexEventStream = streamingDF
    .withWatermark("timestamp", "30 seconds")
    .groupBy($"user_id", window($"timestamp", "5 minutes"))
    .agg(
      collect_list($"action").alias("action_sequence"),
      count("*").alias("action_count")
    )
    .filter(array_contains($"action_sequence", "login") && 
            array_contains($"action_sequence", "view_page") &&
            array_contains($"action_sequence", "click"))
    .select(
      $"user_id",
      $"window.start".alias("session_start"),
      $"action_sequence",
      $"action_count"
    )
  
  val complexQuery = complexEventStream
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()
  
  println("Complex event processing query started (will run for 30 seconds)")
  Thread.sleep(30000)
  complexQuery.stop()

  println("\n=== TUTORIAL COMPLETED ===")
  println("Structured Streaming concepts covered:")
  println("- Basic streaming setup and sources")
  println("- Streaming aggregations and windowing")
  println("- Watermarking for late data handling")
  println("- Stateful operations and session analysis")
  println("- Stream-stream joins")
  println("- Custom sinks and memory sink")
  println("- Query monitoring and error handling")
  println("- Advanced patterns (deduplication, complex events)")
  
  spark.stop()
}

/*
 * Structured Streaming Best Practices:
 * 
 * 1. Watermarking:
 *    - Always set appropriate watermarks for event-time processing
 *    - Balance between lateness tolerance and memory usage
 *    - Consider your data's typical lateness patterns
 * 
 * 2. Checkpointing:
 *    - Always configure checkpoint location for production
 *    - Use reliable storage (HDFS, S3) for checkpoints
 *    - Plan for checkpoint recovery scenarios
 * 
 * 3. Triggers:
 *    - Use ProcessingTime triggers for regular batch processing
 *    - Consider Continuous triggers for low-latency requirements
 *    - Avoid Once triggers in production (mainly for testing)
 * 
 * 4. Output Modes:
 *    - Append: For immutable results (most efficient)
 *    - Update: For changing results (requires key-based sink)
 *    - Complete: For small result sets only
 * 
 * 5. State Management:
 *    - Monitor state store size and growth
 *    - Use appropriate watermarks to clean up old state
 *    - Consider state store provider for large state
 * 
 * 6. Performance:
 *    - Partition streaming data appropriately
 *    - Use broadcast joins for dimension tables
 *    - Monitor micro-batch processing times
 *    - Scale cluster based on input rate and processing complexity
 * 
 * 7. Monitoring:
 *    - Use StreamingQuery.lastProgress for monitoring
 *    - Set up alerts for processing delays
 *    - Monitor checkpoint and state store health
 *    - Use Spark UI streaming tab for debugging
 * 
 * 8. Error Handling:
 *    - Implement proper error handling in UDFs
 *    - Use try-catch blocks for external system interactions
 *    - Consider dead letter queues for failed records
 *    - Plan for schema evolution scenarios
 */