#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Change to project root directory
cd "$(dirname "$0")/.."

echo "Running Clustering Example..."
echo "Note: ClusteringExample.scala was removed due to compilation issues."
echo "Available examples:"
echo "1. Feature Engineering"
echo "2. Classification Pipeline"
echo "3. Hyperparameter Tuning"
echo "4. Recommendation System"
echo ""
echo "Try: ./scripts/run-all.sh for the menu"