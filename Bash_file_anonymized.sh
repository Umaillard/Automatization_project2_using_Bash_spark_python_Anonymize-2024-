#!/bin/bash
# -*- coding: utf-8 -*-

# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# The return value of a pipeline is the status of the last command to exit with a non-zero status.
set -o pipefail

# --- Initialization ---
current_timestamp=$(date +%Y-%m-%d_%H-%M-%S)

# Project and script paths
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# Configure logging
log_base_name=$(basename "$0" .sh)
log_file_path="${PROJECT_DIR}/logs/${log_base_name}_${current_timestamp}.log"
mkdir -p "${PROJECT_DIR}/logs/" && touch "${log_file_path}"
# Delete logs older than 92 days
find "${PROJECT_DIR}/logs/" -type f -mtime +92 -delete

# Redirect stdout and stderr to the log file and the console
exec > >(tee -a "${log_file_path}")
exec 2>&1

# --- Authentication ---
# Kerberos connection
server_principal=$USER
username=$(echo "$server_principal" | cut -d@ -f1)
echo "Authenticating user: $username"
kinit -kt "/path/to/keytabs/${username}.keytab" "${username}"

# --- Environment Configuration ---
# Determine environment based on the project path
if [[ "$PROJECT_DIR" =~ "/production/" ]]; then
    export execution_environment='production'
    HDFS_INPUT_DIR="/path/to/prod/data"
else
    export execution_environment='staging'
    HDFS_INPUT_DIR="/path/to/staging/data"
fi
echo "Running in environment: $execution_environment"
echo "Input HDFS Directory: $HDFS_INPUT_DIR"

# --- Main Logic ---
# Find the newest Excel file in the HDFS directory
echo "Searching for the latest .xlsx file in: $HDFS_INPUT_DIR"
echo "Criteria: Filename must start with 'FILE_PREFIX_' and contain no spaces."

# Find all files matching the pattern, sorted by modification time (newest first)
mapfile -t HDFS_FILE_PATHS < <(hdfs dfs -ls -t "$HDFS_INPUT_DIR" | \
                                  awk '{print $NF}' | \
                                  grep -E '\/FILE_PREFIX_[^ ]+\.xlsx$')

FILE_COUNT=${#HDFS_FILE_PATHS[@]}
echo "Found $FILE_COUNT file(s) matching the criteria."

if [ "$FILE_COUNT" -eq 0 ]; then
    echo "No matching .xlsx files were found. Exiting."
    exit 0
fi

# Select the newest file (the first one in the sorted list)
FILE_TO_PROCESS="${HDFS_FILE_PATHS[0]}"
echo "Selected the newest file to process: $FILE_TO_PROCESS"
echo "----------------------------------------"

# --- Spark Job Execution ---
# Execute the Spark submission command
spark-submit \
    "${SCRIPT_DIR}/process_excel_data.py" \
    "${FILE_TO_PROCESS}"

# Check the exit code of the spark-submit command
if [ $? -ne 0 ]; then
    echo "FATAL: spark-submit command failed. Check the logs for details."
    exit 1
fi

echo "Script finished successfully."
