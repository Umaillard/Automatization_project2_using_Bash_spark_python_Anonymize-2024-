import sys
import os
import datetime
import openpyxl
from typing import List, Tuple, Optional
import io
import pandas as pd
import unicodedata
import re
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Determine repository path based on the environment
environment_name = os.environ.get('environment_name')
if environment_name == 'production':
    repository_path = "/path/to/prod/repo"
else:
    repository_path = "/path/to/dev/repo"

# Initialize SparkSession globally
try:
    spark = SparkSession.builder \
        .appName("GenericExcelProcessor") \
        .config("spark.sql.session.timeZone", "UTC") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession initialized successfully.")
except Exception as e:
    print(f"FATAL: Failed to initialize SparkSession: {e}", file=sys.stderr)
    sys.exit(1)

# --- UTILITY FUNCTIONS ---
def normalize_text_for_matching(text: str) -> str:
    if not text:
        return ""
    nfkd_form = unicodedata.normalize('NFKD', text)
    only_ascii = nfkd_form.encode('ascii', 'ignore').decode('utf-8', 'ignore')
    lower_case = only_ascii.lower()
    no_whitespace = re.sub(r'\s+', '', lower_case)
    return no_whitespace

def find_resilient_sheet_name(
    spark_context,
    hdfs_excel_path: str,
    target_prefix_raw: str
) -> str:
    normalized_target_prefix = normalize_text_for_matching(target_prefix_raw)
    print(f"Normalized target prefix for sheet search: '{normalized_target_prefix}'")
    try:
        file_content_rdd = spark_context.binaryFiles(hdfs_excel_path, minPartitions=1).take(1)
        if not file_content_rdd:
            raise FileNotFoundError(f"Excel file not found or empty at HDFS path: {hdfs_excel_path}")
        excel_bytes = file_content_rdd[0][1]
        with io.BytesIO(excel_bytes) as excel_buffer:
            workbook = openpyxl.load_workbook(excel_buffer, read_only=True)
            available_sheet_names = workbook.sheetnames
            workbook.close()

        print(f"Available sheet names in '{os.path.basename(hdfs_excel_path)}': {available_sheet_names}")
        for sheet_name_original in available_sheet_names:
            normalized_current_sheet = normalize_text_for_matching(sheet_name_original)
            if normalized_current_sheet.startswith(normalized_target_prefix):
                print(f"Matching sheet found: '{sheet_name_original}' (normalized: '{normalized_current_sheet}')")
                return sheet_name_original
        raise ValueError(
            f"No sheet found starting with '{target_prefix_raw}' (normalized: '{normalized_target_prefix}') "
            f"in Excel file '{hdfs_excel_path}'. Available sheets: {available_sheet_names}"
        )
    except Exception as e:
        print(f"Error while trying to enumerate sheets from '{hdfs_excel_path}': {e}", file=sys.stderr)
        raise Exception(f"Could not enumerate sheets from Excel file '{hdfs_excel_path}'. Original error: {e}")

def get_resilient_column_names(actual_columns: List[str], target_prefixes_raw: List[str]) -> List[str]:
    selected_original_cols = []
    normalized_target_prefixes = [normalize_text_for_matching(p) for p in target_prefixes_raw]
    for norm_prefix in normalized_target_prefixes:
        for actual_col_original in actual_columns:
            normalized_actual_col = normalize_text_for_matching(actual_col_original)
            if normalized_actual_col.startswith(norm_prefix):
                if actual_col_original not in selected_original_cols:
                    selected_original_cols.append(actual_col_original)

    if not selected_original_cols:
        print(f"Warning: No columns were selected by get_resilient_column_names for prefixes: {target_prefixes_raw} from {actual_columns}")
    return selected_original_cols

def quote_column_if_needed(col_name: str) -> str:
    if re.search(r'[\s\(\)-]', col_name) and not (col_name.startswith('`') and col_name.endswith('`')):
        return f"`{col_name}`"
    return col_name

def find_flag_columns_for_aggregation(actual_columns: List[str]) -> Optional[str]:
    """
    Finds the name of the 'flag' column for aggregation
    from a list of column names.
    Searches for markers like "marker1" and "marker2".
    """
    col_flag_name = None

    def normalize_for_substring_search(text: str) -> str:
        nfkd_form = unicodedata.normalize('NFKD', text)
        only_ascii = nfkd_form.encode('ascii', 'ignore').decode('utf-8', 'ignore')
        return only_ascii.lower().replace(" ", "")

    norm_marker1 = normalize_for_substring_search("marker1")
    norm_marker2 = normalize_for_substring_search("marker2")
    norm_flag_prefix = normalize_text_for_matching("flag")

    for col_name in actual_columns:
        norm_col_full = normalize_text_for_matching(col_name)
        norm_col_substring = normalize_for_substring_search(col_name)

        if norm_col_full.startswith(norm_flag_prefix):
            if (norm_marker1 in norm_col_substring or norm_marker2 in norm_col_substring) and not col_flag_name:
                col_flag_name = col_name

    return col_flag_name

def process_excel(hdfs_excel_path: str) -> None:
    """
    Reads an Excel file, processes it, and appends to Hive tables.
    """
    target_sheet_prefix_raw = "sheet_prefix_to_find"
    table_hive_inter = repository_path + '.' + "intermediate_table_name"
    target_hive_table = repository_path + '.' + "history_log_table_name"

    print(f"Processing HDFS Excel file: {hdfs_excel_path}")

    try:
        excel_sheet_name = find_resilient_sheet_name(spark.sparkContext, hdfs_excel_path, target_sheet_prefix_raw)

        print(f"Reading Excel sheet '{excel_sheet_name}' from: {hdfs_excel_path}...")

        # 1. Retrieve the binary content of the Excel file from HDFS on the Driver
        file_content_rdd = spark.sparkContext.binaryFiles(hdfs_excel_path, minPartitions=1).take(1)
        if not file_content_rdd:
            raise FileNotFoundError(f"Excel file not found or empty at HDFS path: {hdfs_excel_path}")
        excel_bytes = file_content_rdd[0][1]

        # 2. Use io.BytesIO so pandas can read from bytes in memory
        with io.BytesIO(excel_bytes) as excel_buffer:
            # 3. Read the Excel file into a Pandas DataFrame
            df_pandas = pd.read_excel(
                excel_buffer,
                sheet_name=excel_sheet_name,
                header=0,
                dtype={'column_b': str}
            )

        print(f"Excel file read into Pandas DataFrame with {len(df_pandas)} rows.")

        # 4. Convert the Pandas DataFrame to a Spark DataFrame
        df_spark_raw = spark.createDataFrame(df_pandas)

        print(f"Spark DataFrame created with {df_spark_raw.count()} rows and the following schema:")
        df_spark_raw.printSchema()

        raw_count = df_spark_raw.count()
        print(f"Number of rows initially read (before filtering): {raw_count}")

        # Filter empty rows based on column_a (name to be found dynamically)
        column_a_candidates = get_resilient_column_names(df_spark_raw.columns, ["column_a"])
        if not column_a_candidates:
            raise ValueError(f"Column 'column_a' not found in headers: {df_spark_raw.columns}")
        actual_column_a_for_filter = column_a_candidates[0]

        df_spark = df_spark_raw.filter(
            F.col(quote_column_if_needed(actual_column_a_for_filter)).isNotNull() & \
            (F.trim(F.col(quote_column_if_needed(actual_column_a_for_filter))) != "")
        )

        initial_row_count = df_spark.count()
        print(f"Number of valid rows (after filtering on '{actual_column_a_for_filter}'): {initial_row_count}")

        # Find the actual names of the columns for the intermediate table and aggregations
        actual_column_a_for_select = actual_column_a_for_filter
        
        column_b_candidates = get_resilient_column_names(df_spark_raw.columns, ["column_b"])
        if not column_b_candidates:
            raise ValueError(f"Column 'column_b' not found in headers: {df_spark_raw.columns}")
        actual_column_b = column_b_candidates[0]

        actual_flag_col = find_flag_columns_for_aggregation(df_spark.columns)

        # Prepare the DataFrame for the intermediate Hive table with clean column names
        select_expressions = [
            F.col(quote_column_if_needed(actual_column_a_for_select)).alias("column_a"),
            F.col(quote_column_if_needed(actual_column_b)).alias("column_b")
        ]
        if actual_flag_col:
            select_expressions.append(F.col(quote_column_if_needed(actual_flag_col)).alias("flag"))

        df_for_hive_intermediate = df_spark.select(*select_expressions)

        print(f"Columns for intermediate Hive table '{table_hive_inter}': {df_for_hive_intermediate.columns}")
        df_for_hive_intermediate.write.mode('overwrite').saveAsTable(table_hive_inter)
        print(f"Refreshing table metadata: {table_hive_inter}")
        spark.sql(f'REFRESH TABLE {table_hive_inter}')

        # Calculate data for the history record
        excel_file_name_only = os.path.basename(hdfs_excel_path)
        processing_timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

        flagged_line_count = 0

        if initial_row_count > 0 and actual_flag_col:
            summary_stats_row = df_spark.agg(
                F.count(F.col(quote_column_if_needed(actual_flag_col))).alias("flag_count")
            ).first()
            if summary_stats_row and summary_stats_row["flag_count"] is not None:
                flagged_line_count = summary_stats_row["flag_count"]
        
        print(f"Calculated statistics: flagged_lines={flagged_line_count}")

        # Create and append the history record
        historical_data = [(
            excel_file_name_only,
            processing_timestamp_str,
            flagged_line_count
        )]
        historical_schema = StructType([
            StructField("filename", StringType(), True),
            StructField("processing_timestamp", StringType(), True),
            StructField("flagged_line_count", LongType(), True),
        ])
        df_historical_spark = spark.createDataFrame(data=historical_data, schema=historical_schema)

        # Attempt to retrieve the last history record
        last_record_changed = True
        try:
            latest_history_row = spark.sql(f"""
                SELECT filename, flagged_line_count
                FROM {target_hive_table}
                ORDER BY processing_timestamp DESC
                LIMIT 1
            """).collect()

            if latest_history_row:
                last_record = latest_history_row[0]
                if (excel_file_name_only == last_record["filename"]) and \
                   (flagged_line_count == last_record["flagged_line_count"]):
                    last_record_changed = False
                    print("No change detected compared to the most recent record.")
                else:
                    print("Change detected compared to the most recent record.")
            else:
                print("No history found for the table. The first record will be added.")

        except Exception as e:
            print(f"Cannot read history (table '{target_hive_table}' may not exist): {e}. The first record will be added.")
            last_record_changed = True

        # Conditionally append the new record
        if last_record_changed:
            print(f"Appending record to Hive table: {target_hive_table}")
            df_historical_spark.write.mode('append').saveAsTable(target_hive_table)
            spark.sql(f'REFRESH TABLE {target_hive_table}')
        else:
            print(f"Record not appended to Hive table: {target_hive_table} as no significant change was detected.")

    except Exception as e:
        print(f"An error occurred during Spark processing: {e}", file=sys.stderr)
        raise

# --- Main execution block ---
if __name__ == "__main__":
    script_name = os.path.basename(__file__)
    if len(sys.argv) != 2:
        print(f"Usage: spark-submit <options> {script_name} <hdfs_input_excel_path>", file=sys.stderr)
        sys.exit(1)

    excel_file_hdfs_arg = sys.argv[1]

    try:
        process_excel(excel_file_hdfs_arg)
        print(f"{script_name} completed successfully.")
    except Exception as e:
        print(f"FATAL: {script_name} failed. See error messages above.", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'spark' in globals() and spark:
            spark.stop()
            print("SparkSession stopped.")
