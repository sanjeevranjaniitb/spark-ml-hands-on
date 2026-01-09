#!/bin/bash

export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

echo "Running Model Management Demo..."
sbt "runMain com.tutorial.ModelManagementDemo"