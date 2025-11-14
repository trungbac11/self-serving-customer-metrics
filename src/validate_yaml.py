import yaml
import os
import glob
from pathlib import Path
import duckdb
import re

class MetricValidator:
    def __init__(self):
        self.required_fields = ['metric_name', 'description', 'owner', 'schedule', 'sql']
        self.validations = []
        
    def validate_yaml_structure(self, file_path, metric):
        """Validate YAML structure"""
        errors = []
        warnings = []
        
        # Check required fields
        for field in self.required_fields:
            if field not in metric:
                errors.append(f"Missing required field: '{field}'")
            elif not metric[field]:
                errors.append(f"Field '{field}' cannot be empty")
        
        # Validate metric_name format
        if 'metric_name' in metric:
            metric_name = metric['metric_name']
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', metric_name):
                errors.append(f"Invalid metric name '{metric_name}'. Only letters, numbers and underscores allowed")
        
        # Validate owner email format
        if 'owner' in metric and metric['owner']:
            if '@' not in metric['owner']:
                warnings.append(f"Owner '{metric['owner']}' doesn't look like email format")
        
        # Validate schedule format
        if 'schedule' in metric and metric['schedule']:
            schedule = metric['schedule']
            if len(schedule.split()) != 5:
                warnings.append(f"Schedule '{schedule}' doesn't match cron format (5 fields expected)")
        
        return errors, warnings
    
    def validate_sql_syntax(self, metric):
        """Validate basic SQL syntax"""
        errors = []
        
        if 'sql' not in metric or not metric['sql']:
            errors.append("SQL query cannot be empty")
            return errors
        
        sql = metric['sql'].strip().upper()
        
        # Check for SELECT statement
        if not sql.startswith('SELECT'):
            errors.append("SQL must start with SELECT")
        
        # Check required SQL keywords
        required_keywords = ['SELECT', 'FROM']
        for keyword in required_keywords:
            if keyword not in sql:
                errors.append(f"SQL missing required keyword: {keyword}")
        
        return errors
    
    def test_sql_execution(self, metric):
        """Test SQL execution (optional bonus)"""
        errors = []
        
        if 'sql' not in metric or not metric['sql']:
            return errors
        
        try:
            # Connect to DuckDB for testing
            con = duckdb.connect('metrics.duckdb')
            
            # Try to explain the query to check syntax
            test_sql = f"EXPLAIN {metric['sql']}"
            con.execute(test_sql)
            
            con.close()
            
        except Exception as e:
            errors.append(f"SQL execution error: {str(e)}")
        
        return errors
    
    def validate_metric_file(self, file_path):
        """Validate a single metric file"""
        print(f"\n" + "="*50)
        print(f"VALIDATING: {file_path.name}")
        print("="*50)
        
        try:
            with open(file_path, 'r') as file:
                metric = yaml.safe_load(file)
        except yaml.YAMLError as e:
            print(f"YAML syntax error: {e}")
            return False
        except Exception as e:
            print(f"File read error: {e}")
            return False
        
        # Perform validations
        structure_errors, structure_warnings = self.validate_yaml_structure(file_path, metric)
        sql_syntax_errors = self.validate_sql_syntax(metric)
        sql_execution_errors = self.test_sql_execution(metric)
        
        # Combine all errors
        all_errors = structure_errors + sql_syntax_errors + sql_execution_errors
        
        # Show warnings
        for warning in structure_warnings:
            print(f"  WARNING: {warning}")
        
        # Show errors
        if all_errors:
            for error in all_errors:
                print(f"  ERROR: {error}")
            print(f"VALIDATION FAILED: {file_path.name}")
            return False
        else:
            print(f"VALIDATION PASSED: {file_path.name}")
            print(f"  Metric: {metric.get('metric_name', 'N/A')}")
            print(f"  Description: {metric.get('description', 'N/A')}")
            print(f"  Owner: {metric.get('owner', 'N/A')}")
            return True
    
    def validate_all_metrics(self):
        """Validate all metrics in the directory"""
        metrics_dir = Path('metrics')
        yaml_files = list(metrics_dir.glob('*.yaml')) + list(metrics_dir.glob('*.yml'))
        
        if not yaml_files:
            print("No YAML files found in /metrics directory")
            return False
        
        print(f"Starting validation - Found {len(yaml_files)} files")
        
        success_count = 0
        failed_count = 0
        
        for yaml_file in yaml_files:
            if self.validate_metric_file(yaml_file):
                success_count += 1
            else:
                failed_count += 1
        
        # Summary
        print(f"\n" + "="*50)
        print("VALIDATION SUMMARY:")
        print(f"Passed: {success_count} files")
        print(f"Failed: {failed_count} files")
        print(f"Total: {len(yaml_files)} files")
        
        return failed_count == 0

def main():
    """Main function"""
    print("METRICS VALIDATION SCRIPT")
    print("Validating YAML structure and SQL syntax...")
    
    validator = MetricValidator()
    success = validator.validate_all_metrics()
    
    if success:
        print("\nALL METRICS PASSED VALIDATION!")
        print("You can now run run_metrics.py to execute the metrics.")
    else:
        print("\nVALIDATION FAILED! Please fix the errors before running.")
        exit(1)

if __name__ == "__main__":
    main()