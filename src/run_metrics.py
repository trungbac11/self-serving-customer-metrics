import duckdb
import yaml
import os
import glob
from pathlib import Path

def load_metric_yaml(file_path):
    """Read and parse YAML metric file"""
    try:
        with open(file_path, 'r') as file:
            metric = yaml.safe_load(file)
        return metric
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def execute_metric(con, metric):
    """Execute a metric and create table in DuckDB"""
    try:
        metric_name = metric['metric_name']
        sql_query = metric['sql']
        
        print(f"Executing metric: {metric_name}")
        
        # Create or replace table with name = metric_name
        create_table_sql = f"CREATE OR REPLACE TABLE {metric_name} AS {sql_query}"
        
        con.execute(create_table_sql)
        
        # Check result
        result_count = con.execute(f"SELECT COUNT(*) FROM {metric_name}").fetchone()[0]
        print(f"Created table '{metric_name}' with {result_count} records")
        
        return True
        
    except Exception as e:
        print(f"Error executing metric {metric_name}: {e}")
        return False

def run_all_metrics():
    """Run all metrics in /metrics directory"""
    
    # Connect to DuckDB
    try:
        con = duckdb.connect('metrics.duckdb')
        print("Connected to DuckDB")
    except Exception as e:
        print(f"DuckDB connection error: {e}")
        return False
    
    # Find all YAML files
    metrics_dir = Path('metrics')
    yaml_files = list(metrics_dir.glob('*.yaml')) + list(metrics_dir.glob('*.yml'))
    
    if not yaml_files:
        print("No YAML files found in /metrics directory")
        return False
    
    print(f"Found {len(yaml_files)} metric files")
    
    success_count = 0
    failed_count = 0
    
    # Process each YAML file
    for yaml_file in yaml_files:
        print(f"\n" + "="*40)
        print(f"Processing: {yaml_file.name}")
        
        metric = load_metric_yaml(yaml_file)
        if not metric:
            failed_count += 1
            continue
            
        # Execute metric
        if execute_metric(con, metric):
            success_count += 1
        else:
            failed_count += 1
    
    # Close connection
    con.close()
    
    print(f"\n" + "="*40)
    print("EXECUTION RESULTS:")
    print(f"Success: {success_count} metrics")
    print(f"Failed: {failed_count} metrics")
    print(f"Total: {success_count + failed_count} metrics")
    
    return failed_count == 0

def list_metric_tables():
    """List all metric tables in database"""
    try:
        con = duckdb.connect('metrics.duckdb')
        tables = con.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' 
            AND table_name NOT IN ('customers', 'orders', 'order_items')
        """).fetchall()
        
        print("\nMETRIC TABLES:")
        for table in tables:
            count = con.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
            print(f"  - {table[0]}: {count} records")
            
        con.close()
    except Exception as e:
        print(f"Error listing tables: {e}")

if __name__ == "__main__":
    print("STARTING METRICS EXECUTION")
    
    # Run all metrics
    success = run_all_metrics()
    
    # Show results
    if success:
        list_metric_tables()
        print("\nCOMPLETED! All metrics executed successfully!")
    else:
        print("\nEXECUTION FAILED! Please check the errors above.")