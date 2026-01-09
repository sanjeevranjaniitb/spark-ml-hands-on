#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

echo "Apache Spark Tutorial - Complete Guide"
echo "======================================"
echo ""
echo "Select a tutorial to run:"
echo "1. Spark Fundamentals"
echo "2. DataFrame Operations"
echo "3. Window Functions"
echo "4. Structured Streaming"
echo "5. Performance Optimization"
echo "6. Run All Tutorials"
echo ""
read -p "Enter your choice (1-6): " choice

case $choice in
    1)
        echo "Running Spark Fundamentals Tutorial..."
        cd .. && sbt "runMain com.tutorial.spark.SparkFundamentals"
        ;;
    2)
        echo "Running DataFrame Operations Tutorial..."
        cd .. && sbt "runMain com.tutorial.spark.DataFrameOperations"
        ;;
    3)
        echo "Running Window Functions Tutorial..."
        cd .. && sbt "runMain com.tutorial.spark.WindowFunctions"
        ;;
    4)
        echo "Running Structured Streaming Tutorial..."
        cd .. && sbt "runMain com.tutorial.spark.StreamingExample"
        ;;
    5)
        echo "Running Performance Optimization Tutorial..."
        cd .. && sbt "runMain com.tutorial.spark.OptimizationTechniques"
        ;;
    6)
        echo "Running all tutorials..."
        cd .. && sbt "runMain com.tutorial.spark.SparkFundamentals"
        cd .. && sbt "runMain com.tutorial.spark.DataFrameOperations"
        cd .. && sbt "runMain com.tutorial.spark.WindowFunctions"
        cd .. && sbt "runMain com.tutorial.spark.StreamingExample"
        cd .. && sbt "runMain com.tutorial.spark.OptimizationTechniques"
        ;;
    *)
        echo "Invalid choice. Please run the script again."
        ;;
esac