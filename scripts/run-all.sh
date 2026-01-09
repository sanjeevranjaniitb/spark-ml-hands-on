#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Change to project root directory
cd "$(dirname "$0")/.."

echo "Spark ML Hands-On Tutorial"
echo "=========================="
echo ""
echo "Select an option:"
echo "1. Feature Engineering"
echo "2. Classification Pipeline"
echo "3. Hyperparameter Tuning"
echo "4. Recommendation System"
echo "5. Run All Examples"
echo "6. Interactive Demo (Full Walkthrough)"
echo ""
read -p "Enter your choice (1-6): " choice

case $choice in
    1)
        echo "Running Feature Engineering..."
        sbt "runMain com.tutorial.FeatureEngineering" 2>/dev/null
        ;;
    2)
        echo "Running Classification Pipeline..."
        sbt "runMain com.tutorial.ClassificationPipeline" 2>/dev/null
        ;;
    3)
        echo "Running Hyperparameter Tuning..."
        sbt "runMain com.tutorial.HyperParameterTuning" 2>/dev/null
        ;;
    4)
        echo "Running Recommendation System..."
        sbt "runMain com.tutorial.RecommendationExample" 2>/dev/null
        ;;
    5)
        echo "Running all examples..."
        echo "1. Feature Engineering..."
        sbt "runMain com.tutorial.FeatureEngineering" 2>/dev/null
        echo "2. Classification Pipeline..."
        sbt "runMain com.tutorial.ClassificationPipeline" 2>/dev/null
        echo "3. Hyperparameter Tuning..."
        sbt "runMain com.tutorial.HyperParameterTuning" 2>/dev/null
        echo "4. Recommendation System..."
        sbt "runMain com.tutorial.RecommendationExample" 2>/dev/null
        echo "All examples completed!"
        ;;
    6)
        echo "Starting Interactive Demo..."
        ./scripts/run-interactive-demo.sh
        ;;
    *)
        echo "Invalid choice. Please run the script again."
        ;;
esac