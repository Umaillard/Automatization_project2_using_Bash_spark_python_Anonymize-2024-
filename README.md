## Automated Excel Data Processing Pipeline

### Important Notice

This repository contains the source code for an automated data processing pipeline. The code has been anonymized to remove proprietary information, such as server names, file paths, and specific business logic markers.

This program and its workflow have been reviewed and authorized by the firm for internal distribution and operational use.
Overview

This project automates the process of ingesting and processing specific Excel files from an HDFS directory into managed Hive tables. It is designed to be a robust, scheduled job that identifies the latest data file, processes its contents, and maintains a historical log of its activity.

The pipeline is composed of two main components: a Bash wrapper script for execution and environment setup, and a PySpark script for the core data processing logic.

### Workflow

The automated process follows these steps:
  1) Execution Trigger: The run_spark_job.sh script is executed, typically by a scheduler like Cron or Airflow.
  2) Environment Setup:
      - A timestamped log file is created to capture all output.
      - The script authenticates with Kerberos using a keytab file.
      - It detects the execution environment (e.g., production or staging) based on its file path and sets the appropriate HDFS source directory.

  3) File Discovery: The script scans the designated HDFS directory and identifies the most recently modified .xlsx file that matches a specific naming pattern (e.g.,               FILE_PREFIX_*.xlsx).

  4)  Spark Job Submission: The Bash script launches a spark-submit command, passing the full HDFS path of the newest Excel file to the process_excel_data.py script.

  5) Data Processing (PySpark):
        - The Spark application reads the specified Excel file. It is resilient to minor variations in the sheet name, searching for a sheet that starts with a predefined prefix.
        - The data is loaded, and column names are also identified resiliently, allowing for minor variations in headers.
        - The script filters out invalid or empty rows.
        - The cleaned data is written to an intermediate Hive table, overwriting any previous content.

  6) Historical Logging:
        - The script calculates key metrics from the processed data (e.g., a count of rows with a specific flag).
        - It creates a new history record containing the source filename, the processing timestamp, and the calculated metrics.
        - To prevent duplicate entries, it compares this new record with the most recent record in the history log table.
        - If there are any changes, the new record is appended to the final history log Hive table. Otherwise, it is discarded.

### Key Features

        - Automated File Discovery: Automatically finds and processes the latest relevant file, requiring no manual intervention.
        - Resilient Parsing: Tolerant to minor changes in Excel sheet names and column headers, reducing failures due to small formatting changes.
        - Environment-Aware: Uses different configurations for production and staging environments.
        - Idempotent Logging: The history log is only updated when data changes, preventing redundant records on re-runs with the same source file.
        - Comprehensive Logging: All actions and outputs from both the Bash and Python scripts are captured in a dated log file for easy debugging and auditing.

### Components

1. run_spark_job.sh (Bash Launcher)

This script acts as the entry point for the pipeline. Its responsibilities include:
      - Setting up the execution environment and logging.
      - Handling Kerberos authentication.
      - Finding the correct input file in HDFS.
      - Calling spark-submit with the correct arguments.

2. process_excel_data.py (PySpark Application)

This is the core of the pipeline where all data manipulation occurs. Its responsibilities include:
     - Reading the Excel file from HDFS.
     - Cleaning, transforming, and validating the data.
     - Writing the processed data to an intermediate Hive table.
     - Calculating summary statistics for the history log.
     - Appending the new record to the final history log table.

### Usage

The pipeline is executed by running the Bash script from the command line.
