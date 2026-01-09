package com.tutorial

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

/**
 * Classification Pipeline - Customer Churn Prediction
 */
object ClassificationPipeline extends App {

  println("\n" + "="*60)
  println("         SPARK ML CLASSIFICATION PIPELINE")
  println("="*60)

  // STEP 1: Initialize Spark
  println("\n[STEP 1] Initializing Spark Session...")
  val spark = SparkSessionFactory.create("Classification")
  spark.sparkContext.setLogLevel("ERROR") // Suppress verbose logs
  println("✓ Spark initialized successfully")

  // STEP 2: Load Data
  println("\n[STEP 2] Loading customer data...")
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/churn.csv")
  
  val totalRecords = df.count()
  println(s"✓ Loaded $totalRecords customer records")
  println("✓ Sample data:")
  df.select("age", "salary", "gender", "churn").show(3, false)

  // STEP 3: Feature Engineering
  println("\n[STEP 3] Engineering ML features...")
  println("   → Converting categorical variables to numbers")
  println("   → Creating feature vectors")
  val featured = FeatureEngineering.transform(df)
  println("✓ Features engineered successfully")
  println("✓ ML-ready data:")
  featured.select("age", "salary", "gender", "features", "churn").show(3, false)

  // STEP 4: Split Data
  println("\n[STEP 4] Splitting data for training and testing...")
  val Array(train, test) = featured.randomSplit(Array(0.8, 0.2), seed = 42)
  val trainCount = train.count()
  val testCount = test.count()
  println(s"✓ Training set: $trainCount records (80%)")
  println(s"✓ Test set: $testCount records (20%)")

  // STEP 5: Create ML Algorithm
  println("\n[STEP 5] Configuring Logistic Regression...")
  val lr = new LogisticRegression()
    .setLabelCol("churn")
    .setFeaturesCol("features")
    .setMaxIter(10)
  println("✓ Algorithm configured for binary classification")

  // STEP 6: Train Model
  println("\n[STEP 6] Training the model...")
  println("   → Learning patterns from training data")
  val model = lr.fit(train)
  println("✓ Model training completed")

  // STEP 7: Make Predictions
  println("\n[STEP 7] Making predictions on test data...")
  val predictions = model.transform(test)
  println("✓ Predictions generated")
  
  println("\n[RESULTS] Sample predictions:")
  println("Actual | Predicted | Confidence")
  println("-------|-----------|------------")
  predictions
    .select("churn", "prediction", "probability")
    .collect()
    .take(8)
    .foreach { row =>
      val actual = row.getAs[Int]("churn")
      val predicted = row.getAs[Double]("prediction").toInt
      val prob = row.getAs[org.apache.spark.ml.linalg.Vector]("probability")
      val confidence = f"${math.max(prob(0), prob(1)) * 100}%.1f%%"
      val status = if (actual == predicted) "✓" else "✗"
      println(f"   $actual    |     $predicted     |   $confidence   $status")
    }

  // STEP 8: Evaluate Model
  println("\n[STEP 8] Evaluating model performance...")
  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("churn")
    .setRawPredictionCol("rawPrediction")
  
  val auc = evaluator.evaluate(predictions)
  println(f"✓ Model AUC Score: ${auc}%.3f")
  
  val performance = auc match {
    case x if x >= 0.9 => "Excellent"
    case x if x >= 0.8 => "Good"
    case x if x >= 0.7 => "Fair"
    case _ => "Needs Improvement"
  }
  println(s"✓ Performance: $performance")

  println("\n" + "="*60)
  println("         CLASSIFICATION PIPELINE COMPLETED")
  println("="*60)
  println("\nNext: Try HyperParameterTuning.scala for better accuracy")
  
  spark.stop()
}
