#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

echo "Running Regression Pipeline..."
sbt "runMain com.tutorial.RegressionPipeline"