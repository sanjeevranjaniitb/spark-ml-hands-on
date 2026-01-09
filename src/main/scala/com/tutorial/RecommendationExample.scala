package com.tutorial

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._

/**
 * Recommendation System - Collaborative Filtering with ALS
 * 
 * This example demonstrates:
 * - Collaborative Filtering ("Users like you also liked...")
 * - Matrix Factorization and ALS algorithm
 * - Implicit vs explicit feedback
 * - Spark ML's flagship algorithm in action
 * 
 * The Problem:
 * "Recommend movies to users based on their rating history"
 * - User 1 liked movies A, B, C
 * - User 2 liked movies A, B, D
 * - Recommend movie D to User 1, movie C to User 2
 * 
 * Collaborative Filtering Types:
 * 1. Explicit Feedback: Ratings (1-5 stars) - what we'll use
 * 2. Implicit Feedback: Views, clicks, purchases (binary)
 * 
 * Why ALS is Spark's Crown Jewel:
 * - Matrix factorization is computationally intensive
 * - ALS (Alternating Least Squares) parallelizes beautifully
 * - Scales to billions of users × millions of items
 * - Netflix Prize winner used similar techniques
 * 
 * Matrix Factorization Intuition:
 * User-Item matrix (sparse) → User factors × Item factors
 * [1M users × 100K movies] → [1M × 50] × [50 × 100K]
 * Missing ratings predicted by: user_factors • item_factors
 */
object RecommendationExample extends App {

  // Setup
  val spark = SparkSessionFactory.create("Movie-Recommendations")
  println("Spark Session created for recommendation system")

  // Load ratings data
  // Ratings format: userId, movieId, rating
  // This is the classic "collaborative filtering" dataset format
  // Each row represents one user's opinion about one movie
  val ratings = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/ratings.csv")
  
  println("Movie ratings data loaded:")
  ratings.show(10)
  
  // Data exploration for recommendations
  println("\nDataset statistics:")
  val numUsers = ratings.select("userId").distinct().count()
  val numMovies = ratings.select("movieId").distinct().count()
  val numRatings = ratings.count()
  val sparsity = 1.0 - (numRatings.toDouble / (numUsers * numMovies))
  
  println(f"   Users: $numUsers")
  println(f"   Movies: $numMovies")
  println(f"   Ratings: $numRatings")
  println(f"   Sparsity: ${sparsity * 100}%.1f%% (typical for recommendation data)")
  
  // Rating distribution
  println("\nRating distribution:")
  ratings.groupBy("rating").count().orderBy("rating").show()

  // Train/test split
  // Same pattern as supervised learning
  // But here we're splitting user-item interactions
  // Test set simulates "future" user behavior
  val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2), seed = 42)
  
  println(s"\nData split for recommendation evaluation:")
  println(s"   Training interactions: ${training.count()}")
  println(s"   Test interactions: ${test.count()}")

  // Configure ALS algorithm
  // ALS = Alternating Least Squares
  // rank = Number of latent factors (like "genres" but learned)
  // maxIter = Training iterations
  // regParam = Regularization to prevent overfitting
  // coldStartStrategy = How to handle new users/items
  val als = new ALS()
    .setMaxIter(10)
    .setRank(10)
    .setRegParam(0.1)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")
    .setColdStartStrategy("drop")
    .setSeed(42)
  
  println("ALS algorithm configured:")
  println("   Rank (latent factors): 10")
  println("   Max iterations: 10")
  println("   Regularization: 0.1")
  println("   Cold start strategy: drop unknown users/items")

  // Train recommendation model
  // fit() learns user and item factor matrices
  // Decomposes sparse rating matrix into dense factor matrices
  println("\nTraining ALS recommendation model...")
  println("(Watch for ALS iteration logs - matrix factorization in action)")
  
  val startTime = System.currentTimeMillis()
  val model = als.fit(training)
  val endTime = System.currentTimeMillis()
  
  println(f"ALS model trained (${(endTime - startTime) / 1000.0}%.1f seconds)")

  // Make predictions
  // transform() predicts ratings for user-movie pairs
  // Uses learned factors: prediction = user_factors • movie_factors
  // Can predict for ANY user-movie combination
  println("\nMaking rating predictions...")
  val predictions = model.transform(test)
  
  // Remove NaN predictions (cold start items)
  val validPredictions = predictions.filter(!isnan(col("prediction")))
  
  println("\nSample predictions:")
  println("userId | movieId | actual_rating | predicted_rating | error")
  println("-------|---------|---------------|------------------|-------")
  validPredictions
    .withColumn("error", abs(col("rating") - col("prediction")))
    .select("userId", "movieId", "rating", "prediction", "error")
    .show(10)

  // Evaluate model performance
  // RMSE measures prediction accuracy
  // Lower RMSE = Better recommendations
  // Typical RMSE for movie ratings: 0.8-1.2
  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  
  val rmse = evaluator.evaluate(validPredictions)
  
  println(f"\nRecommendation model performance:")
  println(f"   RMSE: ${rmse}%.3f")
  
  val rmseQuality = rmse match {
    case x if x <= 0.8 => "Excellent (<0.8)"
    case x if x <= 1.0 => "Good (0.8-1.0)"
    case x if x <= 1.2 => "Fair (1.0-1.2)"
    case _ => "Poor (>1.2)"
  }
  println(f"   Quality: $rmseQuality")

  // Generate top-N recommendations
  // recommendForAllUsers() generates top movies for each user
  // This is what you'd use in production
  // Returns DataFrame with userId and recommendations array
  println("\nGenerating top-3 movie recommendations for each user...")
  
  val userRecs = model.recommendForAllUsers(3)
  println("\nTop recommendations by user:")
  
  userRecs.collect().foreach { row =>
    val userId = row.getAs[Int]("userId")
    val recommendations = row.getAs[Seq[org.apache.spark.sql.Row]]("recommendations")
    
    println(f"\nUser $userId should watch:")
    recommendations.foreach { rec =>
      val movieId = rec.getAs[Int]("movieId")
      val rating = rec.getAs[Float]("rating")
      println(f"   Movie $movieId (predicted rating: ${rating}%.2f)")
    }
  }

  // Item-to-item recommendations
  // "Users who liked this movie also liked..."
  // recommendForAllItems() finds similar movies
  // Useful for "More like this" features
  println("\nGenerating similar movies (item-to-item):")
  
  val itemRecs = model.recommendForAllItems(2)
  println("\nSimilar movies:")
  
  itemRecs.collect().foreach { row =>
    val movieId = row.getAs[Int]("movieId")
    val recommendations = row.getAs[Seq[org.apache.spark.sql.Row]]("recommendations")
    
    println(f"\nIf you liked Movie $movieId, try:")
    recommendations.foreach { rec =>
      val similarMovieId = rec.getAs[Int]("userId") // Note: this is actually movieId in item recs
      val similarity = rec.getAs[Float]("rating")
      println(f"   Movie $similarMovieId (similarity: ${similarity}%.2f)")
    }
  }

  // Model insights
  // Examine learned user and item factors
  // These represent "hidden preferences" and "movie characteristics"
  println("\nModel learned factors:")
  
  val userFactors = model.userFactors
  val itemFactors = model.itemFactors
  
  println(f"   User factors shape: ${userFactors.count()} users × ${model.rank} factors")
  println(f"   Item factors shape: ${itemFactors.count()} items × ${model.rank} factors")
  
  println("\nSample user factors (hidden preferences):")
  userFactors.show(3, false)
  
  println("\nSample item factors (hidden movie characteristics):")
  itemFactors.show(3, false)
  
  println("\nRecommendation system completed")
  println("In production: Update model regularly with new ratings")
  
  spark.stop()
}

/*
 * Key Concepts:
 * 
 * 1. ALS is Netflix's secret sauce - this is how recommendations really work
 * 2. Matrix factorization finds hidden patterns: user preferences × movie characteristics
 * 3. Rank=10 means we're finding 10 hidden 'genres' automatically
 * 4. Cold start problem: What to recommend to new users with no history?
 * 5. This same algorithm powers Amazon, Spotify, YouTube recommendations
 * 6. Implicit feedback (clicks) often works better than explicit (ratings)
 * 
 * Production Challenges:
 * - Cold start: New users/items have no recommendations
 * - Scalability: Billions of users × millions of items
 * - Real-time: Users expect instant recommendations
 * - Diversity: Avoid filter bubbles, recommend variety
 * - Business rules: Don't recommend out-of-stock items
 * 
 * Advanced Techniques:
 * - Implicit feedback: Use view counts, not ratings
 * - Hybrid systems: Combine collaborative + content-based
 * - Deep learning: Neural collaborative filtering
 * - Multi-armed bandits: Exploration vs exploitation
 * - Real-time updates: Streaming recommendation updates
 * 
 * Interview Questions:
 * Q: How do recommendation systems work?
 * A: Matrix factorization finds hidden user preferences and item characteristics
 * 
 * Q: What's the cold start problem?
 * A: New users/items have no history - solve with content-based or popularity-based fallbacks
 * 
 * Q: Implicit vs explicit feedback?
 * A: Explicit = ratings (sparse), Implicit = clicks/views (dense but noisy)
 * 
 * Q: How does ALS scale to billions of users?
 * A: Alternating optimization + distributed computing - user factors and item factors updated separately
 * 
 * Production Architecture:
 * 1. Batch training: Daily/weekly ALS model updates
 * 2. Feature store: Pre-computed user/item factors
 * 3. Real-time serving: Fast lookup of pre-computed recommendations
 * 4. A/B testing: Measure recommendation quality impact
 * 5. Monitoring: Track click-through rates, conversion rates
 * 
 * Business Impact:
 * - Netflix: 80% of watched content comes from recommendations
 * - Amazon: 35% of revenue from "customers who bought this also bought"
 * - Spotify: Discover Weekly drives user engagement
 * - YouTube: Recommendations increase watch time by 700%
 */