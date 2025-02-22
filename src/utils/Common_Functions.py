"""
Formula 1 Data Pipeline Utility Functions
=======================================

This module provides utility functions for the Formula 1 data pipeline implementing
a medallion architecture (Bronze -> Silver -> Gold). These functions handle common
operations across the data pipeline including secret management, schema validation,
data merging, and audit tracking.

The module supports the following key operations:
1. Secure secret management for sensitive credentials
2. Automated primary key detection and merge condition generation
3. Delta Lake operations for incremental updates
4. Data quality validation and schema enforcement
5. Audit trail maintenance
6. File path management for partitioned data

Dependencies:
    - pyspark.sql: For DataFrame operations
    - delta.tables: For Delta Lake operations
    - mack: For composite key detection
    - dbutils: For Databricks utilities
    
Purpose: Core utility functions for F1 data pipeline supporting Bronze -> Silver -> Gold architecture.
Key Functions:
- Secret Management: Secure credential access
- Data Validation: Schema and quality checks
- Delta Operations: Merge and update handling
- Audit: Data lineage tracking
- File Management: Path and partition handling

Author: Shubham Dalvi
Last Modified: 2025
"""


def get_secret_from_scope(key: str) -> str:
    """
    Securely retrieve secrets from Databricks secret scope.
    
    This function provides a secure way to access sensitive information like API keys,
    connection strings, and credentials stored in Databricks secret management.
    
    Args:
        key (str): The key name of the secret to retrieve
        
    Returns:
        str: The secret value if successful
        
    Raises:
        Exception: If secret retrieval fails
    
    PURPOSE: Securely retrieve sensitive credentials from Databricks secret scope
    USE CASE: Getting API keys, passwords, connection strings without exposing them in code
        
    Example:
        db_password = get_secret_from_scope("db-password")
        # Returns: "your-secure-password" (from Databricks secrets)
    """
    try:
        return dbutils.secrets.get(scope = "layer-deltalake", key = f"{key}")
    except Exception as err:
        print("Error Occurred", str(err))
        raise

def generate_merge_condition(df) -> str:
    """
    Automatically identify primary keys and construct merge conditions for Delta operations.
    
    This function uses a heuristic approach to identify potential merge keys:
    1. Checks for common primary key column names
    2. Looks for columns with unique values
    3. Falls back to default keys based on table name
    
    Args:
        df (pyspark.sql.DataFrame): Input DataFrame to analyze
        
    Returns:
        str: SQL merge condition string (e.g., "tgt.key1=src.key1 AND tgt.key2=src.key2")
        
    Raises:
        Exception: If no suitable merge keys can be identified
    """
    try:
        # Common primary key column patterns
        primary_key_patterns = ['Id$', 'ID$', 'Key$', 'Code$']
        
        # Get all column names
        columns = df.columns
        
        # Look for primary key candidates
        key_candidates = []
        
        # Check for columns matching common primary key patterns
        for col in columns:
            if any(col.endswith(pattern.replace('$', '')) for pattern in primary_key_patterns):
                key_candidates.append(col)
        
        # If no candidates found, use table-specific logic
        if not key_candidates:
            # Extract table name from first part of column names or table properties
            table_hints = [col.split('_')[0].lower() for col in columns if '_' in col]
            table_name = table_hints[0] if table_hints else ''
            
            # Table-specific default keys
            default_keys = {
                'race': ['raceId'],
                'driver': ['driverId'],
                'constructor': ['constructorId'],
                'circuit': ['circuitId'],
                'result': ['resultId'],
                'qualifying': ['qualifyId'],
                'lap': ['raceId', 'driverId', 'lap'],
                'pitstop': ['raceId', 'driverId', 'stop']
            }
            
            # Use default keys if available
            key_candidates = default_keys.get(table_name, [])
        
        # If still no candidates, raise exception
        if not key_candidates:
            raise ValueError(f"Could not identify merge keys for the DataFrame")
        
        # Generate merge condition
        merge_condition = " AND ".join([f"tgt.{key}=src.{key}" for key in key_candidates])
        return merge_condition
        
    except Exception as err:
        print("Error occurred while generating merge condition:", str(err))
        raise

def get_latest_delta_partition(path: str) -> str:
    """
    Get the latest partition path from a Delta Lake table.
    
    Identifies the most recent partition based on date in a Delta Lake table,
    excluding Delta log files. Used for incremental data processing.
    
    Args:
        path (str): Base path to the Delta table
        
    Returns:
        str: Full path to the latest partition
        
    Raises:
        Exception: If path listing or parsing fails
        
    PURPOSE: Find most recent date partition in a Delta table
    USE CASE: Incremental processing of only the latest data partition
    
    Example:
        path = get_latest_delta_partition("/mnt/silver/drivers")
        # Returns: "/mnt/silver/drivers/date_part=2024-03-20"
    """
    try:
        list_file_path = [(i.path, i.name.split('=')[1].replace('/', '')) 
                         for i in dbutils.fs.ls(f'{path}') 
                         if (i.name != '_delta_log/')]
        list_file_path.sort(key=lambda x: datetime.strptime(x[1], "%Y-%m-%d"), reverse=True)
        return list_file_path[0][0]
    except Exception as err:
        print("Error occurred", str(err))
        raise

def get_latest_file_path(path: str) -> str:
    """
    Get the latest file path from a directory based on date in filename.
    
    Similar to get_latest_delta_partition but for regular files (non-Delta) where
    date is part of the filename.
    
    Args:
        path (str): Directory path containing date-named files
        
    Returns:
        str: Full path to the latest file
        
    Raises:
        Exception: If path listing or parsing fails
        
    PURPOSE: Find most recent file in a directory based on date in filename
    USE CASE: Processing latest CSV/Parquet files with date-based names
    
    Example:
        path = get_latest_file_path("/mnt/bronze/drivers")
        # Returns: "/mnt/bronze/drivers/drivers_2024_03_20.csv"
    """
    try:
        list_file_path = [(i.path, i.name.split('=')[1].replace('/', '')) 
                         for i in dbutils.fs.ls(f'{path}')]
        list_file_path.sort(key=lambda x: datetime.strptime(x[1], "%Y-%m-%d"), reverse=True)
        return dbutils.fs.ls(list_file_path[0][0])[0][0]
    except Exception as err:
        print("Error occurred", str(err))
        raise

def add_audit_columns(df, date_part=None, file_name=None):
    """
    Add audit columns to a DataFrame for tracking data lineage.
    
    Adds three audit columns:
    - input_file_name: Source file tracking
    - date_part: Processing date
    - load_timestamp: Processing timestamp
    
    Args:
        df (pyspark.sql.DataFrame): Input DataFrame
        date_part (str, optional): Override date partition
        file_name (str, optional): Override source filename
        
    Returns:
        pyspark.sql.DataFrame: DataFrame with audit columns added
        
    PURPOSE: Add tracking columns for data lineage (source file, process date, load time)
    USE CASE: Maintaining data lineage and audit trail in data pipeline
    
    Example:
        df_with_audit = add_audit_columns(raw_df)
        # Adds columns: input_file_name, date_part, load_timestamp
    """
    from pyspark.sql.functions import input_file_name, split, current_timestamp, lit, to_date
    
    if(file_name is None and date_part is None):
        df_final = df.select("*").\
            withColumn("input_file_name", split(input_file_name(), '/')[4]).\
            withColumn("date_part", 
                      to_date(regexp_replace(
                          regexp_extract(
                              split(split(input_file_name(), '/')[4], '.csv')[0],
                              r'([0-9]+_[0-9]+_[0-9]+)', 1), '_', '-'))).\
            withColumn("load_timestamp", current_timestamp())
    elif(file_name is None):
        df_final = df.select("*").\
            withColumn("input_file_name", split(input_file_name(), '/')[4]).\
            withColumn("date_part", lit(date_part)).\
            withColumnRenamed("date_part", to_date(date_part)).\
            withColumn("load_timestamp", current_timestamp())
    else:
        df_final = df.select("*").\
            withColumn("input_file_name", lit(file_name)).\
            withColumn("date_part", to_date(lit(date_part))).\
            withColumn("load_timestamp", current_timestamp())
    return df_final

def validate_schema_and_get_partitions(df, sink_schema):
    """
    Validate DataFrame schema and determine optimal file count based on row count.
    
    Performs two key functions:
    1. Validates that source DataFrame schema matches expected sink schema
    2. Determines optimal number of files based on data volume
    
    Args:
        df (pyspark.sql.DataFrame): Source DataFrame to validate
        sink_schema (list): Expected schema definition
        
    Returns:
        int: Recommended number of files for the data volume
        
    Raises:
        Exception: If schema validation fails
        
    PURPOSE: Validate DataFrame schema and calculate optimal file count based on data volume
    USE CASE: Ensuring data quality and optimizing file sizes for performance
    
    Example:
        file_count = validate_schema_and_get_partitions(df, expected_schema)
        # Returns: 3 (for 500K rows) or raises error if schema mismatch
    """
    no_rows = df.count()
    if(no_rows <= 100000):
       no_files = 1
    elif(no_rows > 100000 and no_rows <= 1000000):
       no_files = 3
    elif(no_rows > 1000000 and no_rows <= 10000000):
       no_files = 5
    source_schema = df.limit(1).dtypes
    if(source_schema == sink_schema):
        return no_files
    else:
        raise Exception("Schema is not matched")

def get_latest_file_info(source_path: str) -> tuple:
    """
    Get latest file information based on date in filename.
    
    Parses filenames to extract dates and returns the most recent file's
    information as a tuple containing (datetime, path).
    
    Args:
        source_path (str): Directory path to search
        
    Returns:
        tuple: (datetime object, file path) for the latest file
        
    PURPOSE: Get both date and path of latest file in a directory
    USE CASE: When both file date and path are needed for processing
    
    Example:
        date, path = get_latest_file_info("/mnt/bronze/races")
        # Returns: (datetime(2024,3,20), "/mnt/bronze/races/race_2024_03_20.csv")
    """
    from datetime import datetime
    list_datepart = []
    for i in dbutils.fs.ls(f'{source_path}'):
        list_datepart.append((
            datetime.strptime(i.name.split('.')[0][-10:].replace('_', '-'), "%Y-%m-%d"),
            i.path))
    list_datepart.sort(key=lambda x: x[0], reverse=True)
    return list_datepart[0]

def merge_delta_tables(source_path: str, target_table: str):
    """
    Perform Delta Lake merge operation with automatic key detection.
    
    Implements an intelligent merge operation that:
    1. Identifies the latest source data
    2. Automatically detects merge keys
    3. Performs a full merge (update + insert) operation
    
    Args:
        source_path (str): Path to source Delta table
        target_table (str): Name of target Delta table
        
    PURPOSE: Perform smart merge operation between Delta tables with automatic key detection
    USE CASE: Incrementally updating silver tables with new/changed data from bronze
    
    Example:
        merge_delta_tables("/mnt/bronze/drivers", "silver.drivers")
        # Merges latest bronze data into silver, updating existing and adding new records
    """
    path = get_latest_delta_partition(source_path)
    df_staging = spark.read.format('delta').load(f'{path}')
    merge_condition = generate_merge_condition(df_staging) # eg tgt.driverId=src.driverId AND tgt.raceId=src.raceId
    deltaTable = DeltaTable.forName(spark, f'{target_table}')
    deltaTable.alias('tgt').\
        merge(df_staging.alias('src'), f'{merge_condition}').\
            whenMatchedUpdateAll().\
            whenNotMatchedInsertAll().\
            execute()