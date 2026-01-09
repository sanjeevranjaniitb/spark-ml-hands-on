package com.tutorial

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.Pipeline

/**
 * Hyperparameter Tuning - Advanced Model Optimization
 * 
 * This example demonstrates:
 * - Cross-validation for robust model selection
 * - Regularization (L1/L2) to prevent overfitting
 * - Grid search for optimal hyperparameters
 * - Distributed tuning operations in Spark
 * 
 * The Problem:
 * Default model parameters are rarely optimal
 * - Learning rate too high? Model doesn't converge
 * - Regularization too low? Model overfits
 * - Wrong parameters = Poor performance
 * 
 * Spark ML Tuning Components:
 * 1. ParamGridBuilder: Define parameter combinations to try
 * 2. CrossValidator: Test each combination with k-fold CV
 * 3. Evaluator: Metric to optimize (AUC, accuracy, etc.)
 * 4. Best model selection: Automatically picks winner
 * 
 * Why Spark Excels Here:
 * - Cross-validation is embarrassingly parallel
 * - Each fold can run on different cluster nodes
 * - Grid search scales linearly with cluster size
 */
object HyperParameterTuning extends App {

  // Setup
  val spark = SparkSessionFactory.create("Hyperparameter-Tuning")
  println("Spark Session created for advanced ML tuning")

  // Load and prepare data
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/churn.csv")
  
  println("Churn data loaded for hyperparameter tuning")
  
  // Apply feature engineering
  val featured = FeatureEngineering.transform(df)
  println("Feature engineering applied")
  
  // Cache data for cross-validation (CRITICAL for performance)
  // CV will access this data multiple times
  // Without caching, Spark re-reads from disk every time
  featured.cache()
  println("Data cached for cross-validation performance")

  // Create base algorithm
  val lr = new LogisticRegression()
    .setLabelCol("churn")
    .setFeaturesCol("features")
  
  println("Base Logistic Regression created")

  // Define parameter grid
  // regParam = Regularization strength (prevents overfitting)
  //   - 0.01 = Light regularization
  //   - 0.1 = Strong regularization
  // elasticNetParam = L1 vs L2 regularization mix
  //   - 0.0 = Pure L2 (Ridge) - shrinks coefficients
  //   - 1.0 = Pure L1 (Lasso) - removes features
  //   - 0.5 = Elastic Net (mix of both)
  val paramGrid = new ParamGridBuilder()
    .addGrid(lr.regParam, Array(0.01, 0.1))
    .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
    .build()
  
  println(s"Parameter grid created: ${paramGrid.length} combinations to test")
  println("Grid combinations:")
  paramGrid.zipWithIndex.foreach { case (params, idx) =>
    val regParam = params.get(lr.regParam).get
    val elasticNet = params.get(lr.elasticNetParam).get
    val regType = elasticNet match {
      case 0.0 => "L2 (Ridge)"
      case 1.0 => "L1 (Lasso)"
      case _ => "Elastic Net"
    }
    println(f"   ${idx + 1}. regParam=$regParam%.2f, $regType")
  }

  // Create evaluator
  // BinaryClassificationEvaluator for churn prediction
  // Default metric = AUC (Area Under ROC Curve)
  // AUC ranges 0.5-1.0: 0.5=random, 1.0=perfect
  // AUC > 0.8 is considered good in most domains
  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("churn")
    .setRawPredictionCol("rawPrediction")
    .setMetricName("areaUnderROC")
  
  println("Binary Classification Evaluator created (AUC metric)")

  // Create cross-validator
  // CrossValidator = Automated model selection
  // For each parameter combination:
  //   1. Split data into k folds (here k=3)
  //   2. Train on k-1 folds, test on 1 fold
  //   3. Repeat k times, average the results
  //   4. Pick combination with best average score
  // This prevents overfitting to specific train/test split
  val cv = new CrossValidator()
    .setEstimator(lr)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(3)
    .setParallelism(2)
  
  println("Cross-Validator configured:")
  println("   • 3-fold cross-validation")
  println("   • 6 parameter combinations")
  println("   • Total model training: 6 × 3 = 18 models")
  println("   • Parallel execution: 2 models at once")

  // Run hyperparameter tuning
  // This is computationally expensive
  // Spark distributes work across cluster
  // Each fold can run on different nodes
  println("\nStarting hyperparameter tuning...")
  println("(This will train 18 models - watch the Spark logs)")
  
  val startTime = System.currentTimeMillis()
  val cvModel = cv.fit(featured)
  val endTime = System.currentTimeMillis()
  
  println(f"Hyperparameter tuning completed (${(endTime - startTime) / 1000.0}%.1f seconds)")

  // Examine best model
  // cvModel.bestModel = Winner of the parameter search
  // Extract the best parameters found
  val bestLR = cvModel.bestModel.asInstanceOf[LogisticRegression]
  
  println("\nBest model found:")
  println(f"   Best regParam: ${bestLR.getRegParam}%.3f")
  println(f"   Best elasticNetParam: ${bestLR.getElasticNetParam}%.1f")
  
  val regType = bestLR.getElasticNetParam match {
    case 0.0 => "L2 (Ridge) - Shrinks coefficients"
    case 1.0 => "L1 (Lasso) - Removes features"
    case x => f"Elastic Net ($x%.1f mix) - Best of both"
  }
  println(f"   Regularization type: $regType")

  // Evaluate best model
  val Array(train, test) = featured.randomSplit(Array(0.8, 0.2), seed = 42)
  val testPredictions = cvModel.transform(test)
  val finalAUC = evaluator.evaluate(testPredictions)
  
  println("\nFinal model performance:")
  println(f"   Test AUC: ${finalAUC}%.4f")
  
  val aucQuality = finalAUC match {
    case x if x >= 0.9 => "Excellent (>0.9)"
    case x if x >= 0.8 => "Good (0.8-0.9)"
    case x if x >= 0.7 => "Fair (0.7-0.8)"
    case _ => "Poor (<0.7)"
  }
  println(f"   Model quality: $aucQuality")
  
  println("\nSample predictions from best model:")
  testPredictions
    .select("churn", "prediction", "probability")
    .show(5)
  
  println("\nHyperparameter tuning completed")
  println("In production: Save best model and use for real-time predictions")
  
  spark.stop()
}

/*
 * Key Concepts:
 * 
 * 1. Hyperparameter tuning is where good models become great models
 * 2. Cross-validation prevents overfitting to specific train/test split
 * 3. Regularization is like speed limits for your model - prevents overfitting
 * 4. L1 removes features, L2 shrinks them - choose based on your needs
 * 5. This pattern works for ANY ML algorithm: Random Forest, SVM, Neural Networks
 * 6. In production, you'd use more folds (5-10) and more parameters
 * 
 * Production Warnings:
 * - Cross-validation is EXPENSIVE - use smaller grids on large datasets
 * - Always cache data before CV - massive performance impact
 * - Use parallelism wisely - don't overwhelm your cluster
 * - Consider Bayesian optimization for large parameter spaces
 * 
 * Advanced Techniques:
 * - Nested CV: Outer loop for model selection, inner for hyperparameters
 * - Early stopping: Stop training when validation score stops improving
 * - Random search: Sometimes better than grid search
 * - Hyperopt/Optuna: Smarter parameter search algorithms
 * 
 * Interview Questions:
 * Q: How do you prevent overfitting?
 * A: Cross-validation + Regularization + More data
 * 
 * Q: What's the difference between L1 and L2 regularization?
 * A: L1 creates sparse models (feature selection), L2 shrinks all coefficients
 * 
 * Q: How do you choose the number of CV folds?
 * A: 5-10 folds typical. More folds = better estimates but slower training
 */
