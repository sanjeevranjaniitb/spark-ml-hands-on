#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Change to project root directory
cd "$(dirname "$0")/.."

echo ""
echo "============================================================"
echo "         SPARK ML INTERACTIVE DEMO WALKTHROUGH"
echo "============================================================"
echo ""
echo "This demo will walk you through the complete Spark ML pipeline"
echo "from data loading to model deployment in 6 steps."
echo ""
read -p "Press Enter to start the demo..."

echo ""
echo "STEP 1: Feature Engineering"
echo "----------------------------"
echo "First, let's see how to prepare data for machine learning..."
read -p "Press Enter to run Feature Engineering..."
sbt "runMain com.tutorial.FeatureEngineering" 2>/dev/null

echo ""
echo "STEP 2: Classification Pipeline"
echo "-------------------------------"
echo "Now let's build a complete classification model..."
read -p "Press Enter to run Classification Pipeline..."
sbt "runMain com.tutorial.ClassificationPipeline" 2>/dev/null

echo ""
echo "STEP 3: Hyperparameter Tuning"
echo "------------------------------"
echo "Let's optimize our model with cross-validation..."
read -p "Press Enter to run Hyperparameter Tuning..."
sbt "runMain com.tutorial.HyperParameterTuning" 2>/dev/null

echo ""
echo "STEP 4: Recommendation System"
echo "-----------------------------"
echo "Finally, let's build a recommendation engine..."
read -p "Press Enter to run Recommendation System..."
sbt "runMain com.tutorial.RecommendationExample" 2>/dev/null

echo ""
echo "============================================================"
echo "         INTERACTIVE DEMO COMPLETED!"
echo "============================================================"
echo ""
echo "You've seen the complete Spark ML pipeline:"
echo "✓ Feature Engineering (data preparation)"
echo "✓ Classification (churn prediction)"
echo "✓ Hyperparameter Tuning (model optimization)"
echo "✓ Recommendation System (collaborative filtering)"
echo ""
echo "Next steps:"
echo "- Try individual components with: ./scripts/run-all.sh"
echo "- Start web interface with option 9"
echo ""