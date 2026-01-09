# Exploring Machine Learning Using Spark

## Overview

This comprehensive tutorial demonstrates **Apache Spark from basics to advanced Machine Learning** in a structured format. Perfect for technical interviews, team training, or production readiness assessment.

**Target Audience**: Data Engineers, ML Engineers, Backend Engineers preparing for big-tech interviews

---

## Getting Started

### Prerequisites
```bash
# Install Java 11
brew install openjdk@11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Install Scala and SBT
brew install scala sbt

# Verify installation
java -version
scala -version
sbt --version
```

### Demo Execution Sequence

**For Live Presentation (Recommended Order):**

```bash
# 1. Start with Spark Fundamentals (5-8 minutes)
sbt "runMain com.tutorial.spark.SparkFundamentals"
# Shows: RDDs, DataFrames, lazy evaluation, basic transformations

# 2. Feature Engineering (8-10 minutes)
sbt "runMain com.tutorial.FeatureEngineering"
# Shows: StringIndexer, OneHotEncoder, VectorAssembler pipeline

# 3. Classification Pipeline (10-12 minutes)
sbt "runMain com.tutorial.ClassificationPipeline"
# Shows: Complete ML workflow, train/test split, model evaluation

# 4. Hyperparameter Tuning (5-8 minutes)
sbt "runMain com.tutorial.HyperParameterTuning"
# Shows: Cross-validation, grid search, model selection

# 5. Recommendation System (8-10 minutes)
sbt "runMain com.tutorial.RecommendationExample"
# Shows: ALS matrix factorization, collaborative filtering

# 6. Interactive Web Demo (5 minutes)
python web_ui_with_models.py
# Shows: Real-time predictions, model persistence
```

**Total Demo Time: 40-50 minutes + Q&A**

### Quick Start
```bash
# Clone and navigate to project
cd spark-ml-hands-on

# Compile project
sbt compile

# Run interactive demo (shows all features)
sbt run
```

### Running Individual Components

**1. Spark Fundamentals**
```bash
sbt "runMain com.tutorial.spark.SparkFundamentals"
sbt "runMain com.tutorial.spark.DataFrameOperations"
sbt "runMain com.tutorial.spark.WindowFunctions"
sbt "runMain com.tutorial.spark.OptimizationTechniques"
```

**2. Machine Learning Pipeline**
```bash
# Feature Engineering
sbt "runMain com.tutorial.FeatureEngineering"

# Classification (Churn Prediction)
sbt "runMain com.tutorial.ClassificationPipeline"

# Regression (House Prices)
sbt "runMain com.tutorial.RegressionPipeline"

# Hyperparameter Tuning
sbt "runMain com.tutorial.HyperParameterTuning"

# Clustering
sbt "runMain com.tutorial.ClusteringExample"

# Recommendations
sbt "runMain com.tutorial.RecommendationExample"
```

**3. Interactive Scripts**
```bash
# Run all examples in sequence
./scripts/run-all.sh

# Interactive demo with prompts
./scripts/run-interactive-demo.sh
```

**4. Web UI (Optional)**
```bash
# Install Python dependencies
pip install flask pandas scikit-learn

# Start web interface
python web_ui_with_models.py
# Open http://localhost:5000
```

### What You'll See

**Spark Fundamentals Output:**
- RDD transformations and actions
- DataFrame schema inference
- SQL query execution
- Window function results
- Performance optimization metrics

**ML Pipeline Output:**
- Feature transformation results
- Model training progress
- Evaluation metrics (AUC, RMSE)
- Cross-validation scores
- Cluster assignments
- Movie recommendations

**Interactive Features:**
- Step-by-step execution with explanations
- Real-time model predictions via web UI
- Saved models for reuse
- Performance benchmarks

---

## Tutorial Phases

| **Phase** | **Topic** | **Key Concepts** |
|-----------|-----------|------------------|
| 1 | Spark Architecture & Setup | SparkSession, RDDs, Lazy Evaluation |
| 2 | DataFrames & SQL | Schema, Transformations, SQL Integration |
| 3 | Data Processing & Analytics | Aggregations, Joins, Window Functions |
| 4 | Feature Engineering | StringIndexer, VectorAssembler, Pipelines |
| 5 | Classification Pipeline | Logistic Regression, Train/Test Split |
| 6 | Regression & Evaluation | Linear Regression, RMSE, Model Metrics |
| 7 | Hyperparameter Tuning | Cross-Validation, Grid Search |
| 8 | Clustering & Recommendations | K-Means, ALS Matrix Factorization |
| 9 | Performance & Production | Caching, Optimization, Scaling |
| 10 | Q&A & Wrap-up | Interview Tips, Best Practices |

---

## Project Structure

```
spark-ml-hands-on/
├── README.md                     # This complete guide
├── build.sbt                     # Dependencies & build config
├── data/                         # Datasets (2,500+ records each)
│   ├── churn.csv                 # Customer churn prediction
│   ├── housing.csv               # House price regression
│   └── ratings.csv               # Movie recommendation system
├── src/main/scala/com/tutorial/
│   ├── SparkSessionFactory.scala # Spark configuration
│   ├── FeatureEngineering.scala  # ML feature transformations
│   ├── ClassificationPipeline.scala # Churn prediction
│   ├── RegressionPipeline.scala  # House price prediction
│   ├── HyperParameterTuning.scala # Cross-validation & tuning
│   ├── ClusteringExample.scala   # K-means clustering
│   ├── RecommendationExample.scala # ALS recommendations
│   └── ModelManager.scala        # Model persistence
└── spark-tutorial/               # Spark fundamentals
    ├── SparkFundamentals.scala   # RDDs, DataFrames, SQL
    ├── DataFrameOperations.scala # Advanced transformations
    ├── WindowFunctions.scala     # Analytics & ranking
    ├── StreamingExample.scala    # Real-time processing
    └── OptimizationTechniques.scala # Performance tuning
```

## Scala Source Files Detailed Reference

### Core ML Pipeline Files

| **File** | **Purpose** | **What It Does** | **Key Components** | **Demo Output** |
|----------|-------------|------------------|-------------------|------------------|
| **SparkSessionFactory.scala** | Spark Configuration | Creates optimized SparkSession with proper settings for local development | • SparkSession builder<br>• Log level configuration<br>• Hadoop compatibility settings | Spark initialization messages |
| **FeatureEngineering.scala** | Data Preparation | Transforms raw data into ML-ready format using Spark ML transformers | • StringIndexer (categorical → numeric)<br>• OneHotEncoder (prevents ordinal bias)<br>• VectorAssembler (creates feature vectors) | Step-by-step feature transformation with before/after data |
| **ClassificationPipeline.scala** | Binary Classification | Complete churn prediction pipeline using Logistic Regression | • Data loading and splitting<br>• Model training<br>• Predictions and evaluation<br>• AUC metric calculation | Customer churn predictions with confidence scores |
| **HyperParameterTuning.scala** | Model Optimization | Finds optimal model parameters using cross-validation and grid search | • ParamGridBuilder<br>• CrossValidator (3-fold CV)<br>• Regularization tuning<br>• Best model selection | Grid search results and optimal hyperparameters |
| **RecommendationExample.scala** | Collaborative Filtering | Movie recommendation system using ALS matrix factorization | • ALS algorithm configuration<br>• User-item matrix processing<br>• Top-N recommendations<br>• RMSE evaluation | Movie recommendations for users and similar items |

### Spark Fundamentals Files

| **File** | **Purpose** | **What It Does** | **Key Components** | **Demo Output** |
|----------|-------------|------------------|-------------------|------------------|
| **SparkFundamentals.scala** | Core Concepts | Demonstrates basic Spark operations and concepts | • RDD transformations and actions<br>• DataFrame creation and operations<br>• Lazy evaluation examples<br>• Basic aggregations | RDD operations, DataFrame samples, word count results |
| **WindowFunctions.scala** | Advanced Analytics | Shows window functions for complex analytical queries | • Ranking functions (row_number, rank)<br>• Analytical functions (lag, lead)<br>• Moving averages<br>• Percentile calculations | Sales rankings, time series analysis, moving averages |

### Data Processing Workflow

| **Step** | **File** | **Input** | **Process** | **Output** | **Business Value** |
|----------|----------|-----------|-------------|---------|--------------------|
| 1 | **FeatureEngineering** | Raw CSV (age, salary, gender) | Categorical encoding + vectorization | ML-ready features | Prepares data for algorithms |
| 2 | **ClassificationPipeline** | Engineered features | Train logistic regression model | Churn predictions | Identifies at-risk customers |
| 3 | **HyperParameterTuning** | Same features | Cross-validation + grid search | Optimized model | Improves prediction accuracy |
| 4 | **RecommendationExample** | User-item ratings | ALS matrix factorization | Movie recommendations | Drives user engagement |

### Technical Implementation Details

| **Concept** | **Implementation** | **Files Using It** | **Why Important** |
|-------------|-------------------|-------------------|-------------------|
| **Pipeline Architecture** | `new Pipeline().setStages(Array(...))` | Classification, HyperParameterTuning | Ensures reproducible ML workflows |
| **Feature Vectors** | `VectorAssembler.setInputCols().setOutputCol("features")` | All ML files | Spark ML requires single feature column |
| **Train/Test Split** | `df.randomSplit(Array(0.8, 0.2))` | Classification, HyperParameterTuning | Prevents overfitting, enables evaluation |
| **Cross-Validation** | `CrossValidator.setNumFolds(3)` | HyperParameterTuning | Robust model selection |
| **Model Evaluation** | `BinaryClassificationEvaluator`, `RegressionEvaluator` | Classification, Recommendations | Measures model performance |
| **Lazy Evaluation** | `df.filter().map()` (no action) | SparkFundamentals | Optimizes query execution |

### Algorithm-Specific Details

| **Algorithm** | **File** | **Use Case** | **Key Parameters** | **Evaluation Metric** | **Typical Performance** |
|---------------|----------|--------------|-------------------|----------------------|------------------------|
| **Logistic Regression** | ClassificationPipeline | Binary classification (churn) | `regParam=0.01`, `maxIter=10` | AUC (Area Under ROC) | 0.7-0.9 (good to excellent) |
| **ALS (Matrix Factorization)** | RecommendationExample | Collaborative filtering | `rank=10`, `maxIter=10`, `regParam=0.1` | RMSE | 0.8-1.2 (good range) |
| **Cross-Validation** | HyperParameterTuning | Model selection | `numFolds=3`, `parallelism=2` | Best CV score | Varies by base algorithm |

### Data Flow Architecture

| **Stage** | **Data Format** | **Schema** | **Size** | **Processing** |
|-----------|----------------|------------|----------|----------------|
| **Raw Data** | CSV files | `age: Int, salary: Int, gender: String, churn: Int` | 2,500+ records | File I/O |
| **Feature Engineering** | DataFrame | `+ genderIndex: Double, genderVec: Vector, features: Vector` | Same records, more columns | Transformations |
| **Model Training** | Feature Vectors | `features: Vector, label: Double` | 80% of data | Distributed ML |
| **Predictions** | Predictions DataFrame | `+ prediction: Double, probability: Vector` | 20% of data | Model inference |
| **Evaluation** | Metrics | `AUC: Double, RMSE: Double` | Single values | Performance assessment |

---

## Phase 1: Spark Architecture & Setup

### Core Concepts
- **SparkSession**: Gateway to all Spark functionality
- **RDDs**: Resilient Distributed Datasets (low-level API)
- **DataFrames**: Structured data with schema (high-level API)
- **Lazy Evaluation**: Transformations build execution plan, actions trigger computation

### Demo Code
```scala
// SparkSessionFactory.scala
val spark = SparkSession.builder()
  .appName("Spark ML Demo")
  .master("local[*]")
  .config("spark.sql.adaptive.enabled", "true")
  .getOrCreate()

// Basic RDD operations
val numbers = spark.sparkContext.parallelize(1 to 1000)
val squares = numbers.map(x => x * x)  // Transformation (lazy)
val sum = squares.reduce(_ + _)        // Action (triggers execution)
```

---

## Phase 2: DataFrames & SQL

### Core Concepts
- **Schema**: Structured data with types
- **Catalyst Optimizer**: Query optimization engine
- **SQL Integration**: Use SQL on DataFrames
- **Column Operations**: Type-safe transformations

### Demo Code
```scala
// Load data with schema inference
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/churn.csv")

// DataFrame operations
df.select("customerID", "tenure", "monthlyCharges")
  .filter($"tenure" > 12)
  .groupBy("contract")
  .agg(avg("monthlyCharges").as("avg_charges"))
  .show()

// SQL operations
df.createOrReplaceTempView("customers")
spark.sql("""
  SELECT contract, AVG(monthlyCharges) as avg_charges
  FROM customers 
  WHERE tenure > 12 
  GROUP BY contract
""").show()
```

---

## Phase 3: Data Processing & Analytics

### Core Concepts
- **Aggregations**: GroupBy, statistical functions
- **Joins**: Inner, outer, broadcast joins
- **Window Functions**: Ranking, moving averages
- **Data Quality**: Null handling, validation

### Demo Code
```scala
// Advanced aggregations
val customerStats = df.groupBy("contract", "paymentMethod")
  .agg(
    count("*").as("customer_count"),
    avg("monthlyCharges").as("avg_monthly"),
    stddev("monthlyCharges").as("stddev_monthly")
  )

// Window functions for ranking
import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy("contract").orderBy($"monthlyCharges".desc)

val rankedCustomers = df.withColumn("rank", 
  row_number().over(windowSpec))
  .filter($"rank" <= 5)
```

---

## Phase 4: Feature Engineering

### Core Concepts
- **StringIndexer**: Convert categorical to numeric
- **OneHotEncoder**: Avoid ordinal bias
- **VectorAssembler**: Create feature vectors
- **Pipeline**: Chain transformations

### Demo Code
```scala
// Feature engineering pipeline
val categoricalCols = Array("contract", "paymentMethod", "internetService")
val numericCols = Array("tenure", "monthlyCharges", "totalCharges")

// String indexing for categorical features
val indexers = categoricalCols.map { colName =>
  new StringIndexer()
    .setInputCol(colName)
    .setOutputCol(s"${colName}_indexed")
    .setHandleInvalid("keep")
}

// One-hot encoding
val encoders = categoricalCols.map { colName =>
  new OneHotEncoder()
    .setInputCol(s"${colName}_indexed")
    .setOutputCol(s"${colName}_encoded")
}

// Vector assembly
val assembler = new VectorAssembler()
  .setInputCols(encoders.map(_.getOutputCol) ++ numericCols)
  .setOutputCol("features")

// Create pipeline
val featurePipeline = new Pipeline()
  .setStages(indexers ++ encoders ++ Array(assembler))
```

---

## Phase 5: Classification Pipeline

### Core Concepts
- **Logistic Regression**: Binary classification
- **Train/Test Split**: Avoid overfitting
- **Probability Predictions**: Confidence scores
- **Model Evaluation**: Accuracy, precision, recall

### Demo Code
```scala
// Prepare label column
val labelIndexer = new StringIndexer()
  .setInputCol("churn")
  .setOutputCol("label")

// Create classifier
val lr = new LogisticRegression()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setRegParam(0.01)

// Complete pipeline
val pipeline = new Pipeline()
  .setStages(Array(featurePipeline, labelIndexer, lr))

// Train/test split
val Array(training, test) = df.randomSplit(Array(0.8, 0.2), seed = 42)

// Train model
val model = pipeline.fit(training)
val predictions = model.transform(test)

// Evaluate
val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("rawPrediction")

val auc = evaluator.evaluate(predictions)
println(s"AUC: $auc")
```

---

## Phase 6: Regression & Evaluation

### Core Concepts
- **Linear Regression**: Continuous predictions
- **RMSE**: Root Mean Square Error
- **Feature Importance**: Model interpretability
- **Residual Analysis**: Model validation

### Demo Code
```scala
// Load housing data
val housingDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/housing.csv")

// Feature engineering for regression
val housingFeatures = Array("bedrooms", "bathrooms", "sqft_living", "grade")
val housingAssembler = new VectorAssembler()
  .setInputCols(housingFeatures)
  .setOutputCol("features")

// Linear regression
val lr = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("price")
  .setRegParam(0.1)

// Train model
val housingPipeline = new Pipeline().setStages(Array(housingAssembler, lr))
val Array(trainData, testData) = housingDF.randomSplit(Array(0.8, 0.2))
val housingModel = housingPipeline.fit(trainData)

// Evaluate
val predictions = housingModel.transform(testData)
val evaluator = new RegressionEvaluator()
  .setLabelCol("price")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

val rmse = evaluator.evaluate(predictions)
println(s"RMSE: $rmse")
```

---

## Phase 7: Hyperparameter Tuning

### Core Concepts
- **Cross-Validation**: Robust model selection
- **Grid Search**: Systematic parameter exploration
- **Regularization**: L1/L2 penalty terms
- **Model Selection**: Best parameter combination

### Demo Code
```scala
// Parameter grid
val paramGrid = new ParamGridBuilder()
  .addGrid(lr.regParam, Array(0.01, 0.1, 1.0))
  .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
  .build()

// Cross validator
val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)
  .setParallelism(2)

// Find best model
val cvModel = cv.fit(training)
val bestModel = cvModel.bestModel

// Evaluate on test set
val finalPredictions = bestModel.transform(test)
val finalAUC = evaluator.evaluate(finalPredictions)
println(s"Best model AUC: $finalAUC")
```

---

## Phase 8: Clustering & Recommendations

### Core Concepts
- **K-Means**: Unsupervised clustering
- **ALS**: Alternating Least Squares for recommendations
- **Collaborative Filtering**: User-item interactions
- **Matrix Factorization**: Dimensionality reduction

### Demo Code
```scala
// K-Means clustering
val kmeans = new KMeans()
  .setK(3)
  .setFeaturesCol("features")
  .setPredictionCol("cluster")

val clusterModel = kmeans.fit(featureData)
val clustered = clusterModel.transform(featureData)

// ALS Recommendations
val ratingsDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/ratings.csv")

val als = new ALS()
  .setMaxIter(10)
  .setRegParam(0.01)
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")

val alsModel = als.fit(ratingsDF)
val recommendations = alsModel.recommendForAllUsers(5)
```

---

## Phase 9: Performance & Production

### Core Concepts
- **Caching**: Persist frequently accessed data
- **Partitioning**: Optimize data distribution
- **Broadcast Variables**: Efficient small data sharing
- **Monitoring**: Track job performance

### Demo Code
```scala
// Caching for iterative algorithms
training.cache()
val cachedModel = pipeline.fit(training)

// Optimal partitioning
val partitionedDF = df.repartition(200, $"contract")

// Broadcast small lookup tables
val broadcastVar = spark.sparkContext.broadcast(lookupMap)

// Performance monitoring
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

## Phase 10: Production Readiness

### Key Production Considerations

**Model Persistence**:
```scala
// Save trained model
model.write.overwrite().save("models/churn_model")

// Load for inference
val loadedModel = PipelineModel.load("models/churn_model")
```

**Scaling to Clusters**:
- Replace `local[*]` with cluster manager (YARN, Kubernetes)
- Use Parquet instead of CSV for better performance
- Implement proper error handling and logging

**Monitoring & Maintenance**:
- Track model drift with statistical tests
- Implement A/B testing for model updates
- Set up automated retraining pipelines

---

## Interview Readiness Checklist

After this demo, you should confidently explain:

**Spark Architecture**:
- Driver vs Executor roles
- Lazy evaluation benefits
- RDD vs DataFrame trade-offs

**ML Pipeline Design**:
- Feature engineering best practices
- Train/validation/test split strategies
- Cross-validation for model selection

**Performance Optimization**:
- When to cache data
- Partition strategy selection
- Join optimization techniques

**Production Deployment**:
- Model versioning and persistence
- Monitoring and drift detection
- Scaling considerations

---

## Common Interview Questions & Answers

**Q: "How does Spark handle data skew?"**
A: Adaptive Query Execution automatically detects skew and applies techniques like broadcast joins, dynamic partition pruning, and skew join optimization.

**Q: "When would you use RDDs vs DataFrames?"**
A: DataFrames for structured data with schema (90% of cases). RDDs for unstructured data or when you need fine-grained control over partitioning.

**Q: "How do you handle categorical features with many categories?"**
A: Use StringIndexer with frequency-based indexing, consider feature hashing for extremely high cardinality, or apply dimensionality reduction techniques.

**Q: "What's the difference between cache() and persist()?"**
A: cache() uses default storage level (MEMORY_AND_DISK). persist() allows you to specify storage level (memory only, disk only, serialized, etc.).

---

## Running the Complete Demo

```bash
# Setup environment
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Run complete demo sequence
sbt "runMain com.tutorial.SparkSessionFactory"
sbt "runMain com.tutorial.FeatureEngineering"
sbt "runMain com.tutorial.ClassificationPipeline"
sbt "runMain com.tutorial.RegressionPipeline"
sbt "runMain com.tutorial.HyperParameterTuning"
sbt "runMain com.tutorial.ClusteringExample"
sbt "runMain com.tutorial.RecommendationExample"

# Spark fundamentals
sbt "runMain com.tutorial.spark.SparkFundamentals"
sbt "runMain com.tutorial.spark.OptimizationTechniques"
```

---

## Final Advice for Success

**Technical Mastery**:
- Understand the **why** behind each concept, not just the **how**
- Practice explaining trade-offs and design decisions
- Focus on **system thinking at scale**

**Interview Strategy**:
- Start with high-level architecture, then dive into implementation
- Discuss performance implications of each choice
- Mention production considerations (monitoring, scaling, maintenance)

**Key Differentiator**:
> Spark ML success comes from understanding **distributed systems principles** more than individual algorithms.

This comprehensive guide provides everything needed for a successful Spark ML demonstration, from fundamental concepts to production-ready implementations.