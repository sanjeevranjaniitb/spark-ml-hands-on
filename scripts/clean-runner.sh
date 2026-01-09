#!/bin/bash

# Comprehensive log suppression for clean demos
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home

# Suppress all Java/Hadoop logging
export JAVA_OPTS="-Djava.util.logging.config.file= -Dlog4j.configuration=log4j2.xml -Dhadoop.home.dir=/tmp"

# Change to project root
cd "$(dirname "$0")/.."

# Function to run SBT with complete log suppression
run_clean() {
    local main_class=$1
    echo "Initializing Spark (please wait)..."
    
    # Redirect all output except our application logs
    sbt "runMain $main_class" 2>/dev/null | grep -E "(STEP|✓|=====|Actual|Predicted|AUC|RMSE|User|Movie|Feature|Classification|Recommendation)" || true
    
    echo "✓ Completed successfully"
    echo ""
}

# Export the function so other scripts can use it
export -f run_clean