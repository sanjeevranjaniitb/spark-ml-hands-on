#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Change to project root directory
cd "$(dirname "$0")/.."

echo "Running Feature Engineering Demo..."
sbt "runMain com.tutorial.FeatureEngineering" 2>/dev/null