# SELF-SERVING-CUSTOMER-METRICS

**Context**: The Data Analysts in our team currently depend on data engineers to implement data pipelines for new customer metrics. This creates a bottleneck and slows down analysis.

**Goal**: to build a simple prototype for that system.

**Objective**: A lightweight prototype system that allows Data Analysts to define customer metrics using YAML + SQL, with automatic materialization in DuckDB.

## Setup instructions
### Prerequisites
- Python 3.12+
- Pip package manager
- Using Linux

### Installation Steps
1. **Download the project files**

2. **Create virtual enviroments**
`python -m venv venv`

3. **Active**
`source venv/bin/activate`

4. **Install dependencies:**
`pip install --upgrade pip`
`pip install -r requirements.txt`

5. **Prepare data files:**
Place "customers.csv", "orders.csv", and "order_items.csv" in the data/ directory

6. **Initialize the database:**
`python src/setup_database.py`

7. **Verify setup:**
`python src/validate_yaml.py`

## How to add a new metric
**Step 1: Create YAML Metric Definition**
- Create a new .yaml file in the metrics/ directory with this structure

**Step 2: Validate the Metric**
- Run validation to check for errors: `python src/validate_yaml.py`

**Step 3:  Execute the Metric**
- Run the metric to create the table in DuckDB: `python src/run_metrics.py`

**This will:**
- Read all YAML files from /metrics
- Execute each SQL query against DuckDB
- Create or replace tables named after each metric


## Development guideline

### Project Structure
<img width="799" height="351" alt="image" src="https://github.com/user-attachments/assets/0a37675a-0f76-4570-8de8-9237fec6dc42" />

### Script Execution Guide

#### Database Initialization:
`python src/setup_database.py`

#### Metric Definition Validation:
`python src/validate_yaml.py`

#### Metric Calculation and Storage:
`python src/run_metrics.py`

#### Database Cleanup:
`python src/clean_database.py`

### System Testing
#### Validation Test Cases
- Test with missing required fields

- Test with invalid YAML syntax

- Test with malformed SQL queries

- Test with valid complete metric definitions

#### Error Handling
- All scripts include try-catch blocks for proper error handling

- Validation provides clear error messages for debugging

- Execution script continues processing other metrics if one fails

#### Maintenance
- Regular dependency updates: `pip install -r requirements.txt --upgrade`

- Database cleanup when needed: `python src/clean_database.py`

- Backup important data before major changes
