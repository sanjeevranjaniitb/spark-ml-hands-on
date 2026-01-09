#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Change to project root directory
cd "$(dirname "$0")/.."

echo "Running Hyperparameter Tuning..."
sbt "runMain com.tutorial.HyperParameterTuning" 2>/dev/null