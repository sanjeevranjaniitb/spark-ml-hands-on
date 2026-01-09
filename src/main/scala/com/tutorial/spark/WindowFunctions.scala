package com.tutorial.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Window Functions Tutorial
 * 
 * Comprehensive guide to Spark SQL window functions:
 * - Ranking functions (row_number, rank, dense_rank)
 * - Analytical functions (lag, lead, first_value, last_value)
 * - Aggregate functions over windows
 * - Moving averages and cumulative calculations
 */
object WindowFunctions extends App {

  val spark = SparkSession.builder()
    .appName("Window Functions Tutorial")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  println("=== WINDOW FUNCTIONS TUTORIAL ===")

  // Sample sales data
  case class Sale(date: String, salesperson: String, region: String, product: String, amount: Double)
  
  val salesData = Seq(
    Sale("2023-01-01", "Alice", "North", "Laptop", 1200.0),
    Sale("2023-01-02", "Bob", "South", "Phone", 800.0),
    Sale("2023-01-03", "Alice", "North", "Tablet", 600.0),
    Sale("2023-01-04", "Charlie", "East", "Laptop", 1200.0),
    Sale("2023-01-05", "Bob", "South", "Laptop", 1200.0),
    Sale("2023-01-06", "Alice", "North", "Phone", 800.0),
    Sale("2023-01-07", "Diana", "West", "Tablet", 600.0),
    Sale("2023-01-08", "Charlie", "East", "Phone", 800.0),
    Sale("2023-01-09", "Bob", "South", "Tablet", 600.0),
    Sale("2023-01-10", "Diana", "West", "Laptop", 1200.0),
    Sale("2023-01-11", "Alice", "North", "Laptop", 1200.0),
    Sale("2023-01-12", "Charlie", "East", "Tablet", 600.0)
  )

  val salesDF = salesData.toDF()
    .withColumn("date", to_date($"date", "yyyy-MM-dd"))
    .withColumn("month", date_format($"date", "yyyy-MM"))

  println("Sample sales data:")
  salesDF.show()

  // 1. RANKING FUNCTIONS
  println("\n1. Ranking Functions")
  
  val salesRankingWindow = Window.partitionBy("region").orderBy($"amount".desc)
  
  val rankedSalesDF = salesDF
    .withColumn("row_number", row_number().over(salesRankingWindow))
    .withColumn("rank", rank().over(salesRankingWindow))
    .withColumn("dense_rank", dense_rank().over(salesRankingWindow))
    .withColumn("percent_rank", percent_rank().over(salesRankingWindow))
    .withColumn("ntile_4", ntile(4).over(salesRankingWindow))
  
  println("Sales ranking by region (ordered by amount):")
  rankedSalesDF
    .select("region", "salesperson", "amount", "row_number", "rank", "dense_rank", "percent_rank", "ntile_4")
    .orderBy("region", "row_number")
    .show()

  // 2. ANALYTICAL FUNCTIONS
  println("\n2. Analytical Functions (Lag/Lead)")
  
  val timeSeriesWindow = Window.partitionBy("salesperson").orderBy("date")
  
  val analyticalDF = salesDF
    .withColumn("previous_sale", lag($"amount", 1).over(timeSeriesWindow))
    .withColumn("next_sale", lead($"amount", 1).over(timeSeriesWindow))
    .withColumn("sale_change", $"amount" - lag($"amount", 1).over(timeSeriesWindow))
    .withColumn("first_sale", first_value($"amount").over(timeSeriesWindow))
    .withColumn("last_sale", last_value($"amount").over(timeSeriesWindow))
  
  println("Sales time series analysis by salesperson:")
  analyticalDF
    .select("salesperson", "date", "amount", "previous_sale", "next_sale", "sale_change")
    .orderBy("salesperson", "date")
    .show()

  // 3. AGGREGATE FUNCTIONS OVER WINDOWS
  println("\n3. Aggregate Functions Over Windows")
  
  val aggregateWindow = Window.partitionBy("region").orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  
  val aggregateDF = salesDF
    .withColumn("running_total", sum($"amount").over(aggregateWindow))
    .withColumn("running_avg", avg($"amount").over(aggregateWindow))
    .withColumn("running_count", count($"amount").over(aggregateWindow))
    .withColumn("running_min", min($"amount").over(aggregateWindow))
    .withColumn("running_max", max($"amount").over(aggregateWindow))
  
  println("Running aggregates by region:")
  aggregateDF
    .select("region", "date", "amount", "running_total", "running_avg", "running_count")
    .orderBy("region", "date")
    .show()

  // 4. MOVING AVERAGES
  println("\n4. Moving Averages and Rolling Windows")
  
  val movingWindow3 = Window.partitionBy("salesperson").orderBy("date")
    .rowsBetween(-2, 0) // 3-day moving window
  
  val movingWindow7 = Window.partitionBy("salesperson").orderBy("date")
    .rowsBetween(-6, 0) // 7-day moving window
  
  val movingAverageDF = salesDF
    .withColumn("ma_3_day", avg($"amount").over(movingWindow3))
    .withColumn("ma_7_day", avg($"amount").over(movingWindow7))
    .withColumn("sum_3_day", sum($"amount").over(movingWindow3))
    .withColumn("count_3_day", count($"amount").over(movingWindow3))
  
  println("Moving averages for each salesperson:")
  movingAverageDF
    .select("salesperson", "date", "amount", "ma_3_day", "ma_7_day", "count_3_day")
    .orderBy("salesperson", "date")
    .show()

  // 5. PERCENTILES AND QUANTILES
  println("\n5. Percentiles and Quantiles")
  
  val percentileWindow = Window.partitionBy("product")
  
  val percentileDF = salesDF
    .withColumn("median_amount", expr("percentile_approx(amount, 0.5)").over(percentileWindow))
    .withColumn("q1_amount", expr("percentile_approx(amount, 0.25)").over(percentileWindow))
    .withColumn("q3_amount", expr("percentile_approx(amount, 0.75)").over(percentileWindow))
    .withColumn("amount_percentile", percent_rank().over(Window.partitionBy("product").orderBy("amount")))
  
  println("Percentile analysis by product:")
  percentileDF
    .select("product", "amount", "median_amount", "q1_amount", "q3_amount", "amount_percentile")
    .distinct()
    .orderBy("product", "amount")
    .show()

  // 6. COMPLEX WINDOW SPECIFICATIONS
  println("\n6. Complex Window Specifications")
  
  // Range-based window (by value, not rows)
  val rangeWindow = Window.partitionBy("region").orderBy("amount")
    .rangeBetween(-200, 200) // Within 200 of current amount
  
  val rangeDF = salesDF
    .withColumn("similar_sales_count", count($"amount").over(rangeWindow))
    .withColumn("similar_sales_avg", avg($"amount").over(rangeWindow))
  
  println("Sales within similar amount ranges:")
  rangeDF
    .select("region", "amount", "similar_sales_count", "similar_sales_avg")
    .orderBy("region", "amount")
    .show()

  // 7. BUSINESS ANALYTICS USE CASES
  println("\n7. Business Analytics Use Cases")
  
  // Top N analysis
  val topSalesWindow = Window.partitionBy("region").orderBy($"amount".desc)
  
  val topSalesDF = salesDF
    .withColumn("sales_rank", row_number().over(topSalesWindow))
    .filter($"sales_rank" <= 2) // Top 2 sales per region
  
  println("Top 2 sales per region:")
  topSalesDF
    .select("region", "salesperson", "amount", "sales_rank")
    .orderBy("region", "sales_rank")
    .show()
  
  // Growth analysis
  val growthWindow = Window.partitionBy("salesperson").orderBy("date")
  
  val growthDF = salesDF
    .withColumn("previous_amount", lag($"amount", 1).over(growthWindow))
    .withColumn("growth_rate", 
      when(lag($"amount", 1).over(growthWindow).isNotNull,
        (($"amount" - lag($"amount", 1).over(growthWindow)) / lag($"amount", 1).over(growthWindow)) * 100)
      .otherwise(0.0))
    .withColumn("cumulative_sales", sum($"amount").over(
      Window.partitionBy("salesperson").orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)))
  
  println("Sales growth analysis:")
  growthDF
    .select("salesperson", "date", "amount", "previous_amount", "growth_rate", "cumulative_sales")
    .orderBy("salesperson", "date")
    .show()

  // 8. ADVANCED WINDOW PATTERNS
  println("\n8. Advanced Window Patterns")
  
  // Session-based analysis (gaps and islands)
  val sessionWindow = Window.partitionBy("salesperson").orderBy("date")
  
  val sessionDF = salesDF
    .withColumn("days_since_last_sale", 
      datediff($"date", lag($"date", 1).over(sessionWindow)))
    .withColumn("is_new_session", 
      when($"days_since_last_sale" > 2 || $"days_since_last_sale".isNull, 1).otherwise(0))
    .withColumn("session_id", 
      sum($"is_new_session").over(sessionWindow))
  
  println("Sales sessions (gaps > 2 days create new sessions):")
  sessionDF
    .select("salesperson", "date", "amount", "days_since_last_sale", "session_id")
    .orderBy("salesperson", "date")
    .show()
  
  // Session aggregates
  val sessionAggregates = sessionDF
    .groupBy("salesperson", "session_id")
    .agg(
      min("date").alias("session_start"),
      max("date").alias("session_end"),
      count("*").alias("sales_count"),
      sum("amount").alias("session_total"),
      avg("amount").alias("session_avg")
    )
  
  println("Session-level aggregates:")
  sessionAggregates
    .orderBy("salesperson", "session_id")
    .show()

  // 9. PERFORMANCE CONSIDERATIONS
  println("\n9. Performance Optimization for Window Functions")
  
  // Efficient partitioning
  val efficientWindow = Window.partitionBy("region", "product").orderBy("date")
  
  val optimizedDF = salesDF
    .repartition($"region", $"product") // Align partitioning with window
    .withColumn("product_rank", row_number().over(efficientWindow))
    .withColumn("product_total", sum($"amount").over(efficientWindow))
  
  println("Optimized window function with proper partitioning:")
  optimizedDF.explain()
  
  // Cache for multiple window operations
  val cachedDF = salesDF.cache()
  
  println("DataFrame cached for multiple window operations")

  println("\n=== TUTORIAL COMPLETED ===")
  println("Window function concepts covered:")
  println("- Ranking functions (row_number, rank, dense_rank)")
  println("- Analytical functions (lag, lead, first_value, last_value)")
  println("- Aggregate functions over windows")
  println("- Moving averages and rolling calculations")
  println("- Percentiles and quantiles")
  println("- Range-based windows")
  println("- Business analytics patterns")
  println("- Session analysis and gaps/islands")
  println("- Performance optimization techniques")
  
  spark.stop()
}

/*
 * Window Functions Best Practices:
 * 
 * 1. Partitioning Strategy:
 *    - Choose partition columns that align with your analysis
 *    - Avoid high-cardinality partitioning columns
 *    - Consider data distribution across partitions
 * 
 * 2. Ordering:
 *    - Always specify explicit ordering for deterministic results
 *    - Use multiple columns for tie-breaking when necessary
 *    - Consider the cost of sorting large partitions
 * 
 * 3. Window Frame Specification:
 *    - Understand the difference between ROWS and RANGE
 *    - Use appropriate bounds (UNBOUNDED, CURRENT ROW, etc.)
 *    - Be careful with default frame specifications
 * 
 * 4. Performance:
 *    - Partition data appropriately before window operations
 *    - Cache DataFrames when performing multiple window operations
 *    - Consider using broadcast joins for dimension tables
 *    - Monitor memory usage for large windows
 * 
 * 5. Common Use Cases:
 *    - Time series analysis and trend detection
 *    - Ranking and top-N analysis
 *    - Running totals and moving averages
 *    - Gap and island problems
 *    - Cohort analysis and retention metrics
 * 
 * 6. Debugging:
 *    - Use explain() to understand execution plans
 *    - Check for data skew in partitions
 *    - Validate window frame boundaries with small datasets
 *    - Monitor Spark UI for performance bottlenecks
 */