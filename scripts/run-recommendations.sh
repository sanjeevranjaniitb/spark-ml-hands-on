#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Change to project root directory
cd "$(dirname "$0")/.."

echo "Running Recommendation System..."
sbt "runMain com.tutorial.RecommendationExample" 2>/dev/null