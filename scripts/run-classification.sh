#!/bin/bash

# Set Java 11 for Spark compatibility
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Suppress all Java/Hadoop logging completely
export JAVA_OPTS="-Djava.util.logging.config.file= -Dlog4j.configuration=log4j2.xml -Dhadoop.home.dir=/tmp"

# Change to project root directory
cd "$(dirname "$0")/.."

echo "Running Spark ML Classification Pipeline..."
echo "(Initializing Spark - this may take a moment...)"
echo ""

# Complete suppression of all stderr and background exceptions
(
  exec 2>/dev/null
  sbt "runMain com.tutorial.ClassificationPipeline"
) 2>/dev/null