package com.tutorial

import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.sql.DataFrame

/**
 * Feature Engineering Pipeline
 */
object FeatureEngineering extends App {

  println("\n" + "="*60)
  println("         SPARK ML FEATURE ENGINEERING")
  println("="*60)

  // STEP 1: Initialize Spark
  println("\n[STEP 1] Initializing Spark Session...")
  val spark = SparkSessionFactory.create("Feature-Engineering")
  spark.sparkContext.setLogLevel("ERROR") // Suppress verbose logs
  println("✓ Spark initialized successfully")

  // STEP 2: Create Sample Data
  println("\n[STEP 2] Creating sample customer data...")
  import spark.implicits._
  
  val sampleData = Seq(
    (25, 50000, "M", 0),
    (35, 75000, "F", 1),
    (45, 60000, "M", 0),
    (28, 80000, "F", 1),
    (52, 45000, "M", 0)
  ).toDF("age", "salary", "gender", "churn")
  
  println("✓ Sample data created:")
  sampleData.show(false)

  // STEP 3: Feature Engineering Pipeline
  println("\n[STEP 3] Building feature engineering pipeline...")
  
  println("   → Step 3a: StringIndexer (gender: M/F → 0/1)")
  val indexer = new StringIndexer()
    .setInputCol("gender")
    .setOutputCol("genderIndex")
    .setHandleInvalid("keep")
  
  println("   → Step 3b: OneHotEncoder (0/1 → [1,0]/[0,1])")
  val encoder = new OneHotEncoder()
    .setInputCol("genderIndex")
    .setOutputCol("genderVec")
  
  println("   → Step 3c: VectorAssembler (combine all features)")
  val assembler = new VectorAssembler()
    .setInputCols(Array("age", "salary", "genderVec"))
    .setOutputCol("features")
  
  println("✓ Pipeline components configured")

  // STEP 4: Apply Transformations
  println("\n[STEP 4] Applying transformations...")
  
  println("   → Applying StringIndexer...")
  val indexed = indexer.fit(sampleData).transform(sampleData)
  
  println("   → Applying OneHotEncoder...")
  val encoded = encoder.fit(indexed).transform(indexed)
  
  println("   → Applying VectorAssembler...")
  val result = assembler.transform(encoded)
  
  println("✓ All transformations applied")

  // STEP 5: Show Results
  println("\n[STEP 5] Feature engineering results:")
  println("\nOriginal → Indexed → Encoded → Final Features:")
  result.select("gender", "genderIndex", "genderVec", "features").show(false)
  
  println("\nFinal ML-ready dataset:")
  result.select("age", "salary", "gender", "features", "churn").show(false)

  println("\n" + "="*60)
  println("       FEATURE ENGINEERING COMPLETED")
  println("="*60)
  println("\nData is now ready for machine learning algorithms!")
  
  spark.stop()

  /**
   * Transforms raw DataFrame into ML-ready format
   */
  def transform(df: DataFrame): DataFrame = {
    val indexer = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("genderIndex")
      .setHandleInvalid("keep")
    
    val encoder = new OneHotEncoder()
      .setInputCol("genderIndex")
      .setOutputCol("genderVec")
    
    val assembler = new VectorAssembler()
      .setInputCols(Array("age", "salary", "genderVec"))
      .setOutputCol("features")
    
    val indexed = indexer.fit(df).transform(df)
    val encoded = encoder.fit(indexed).transform(indexed)
    assembler.transform(encoded)
  }
}