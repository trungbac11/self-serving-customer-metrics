# SELF-SERVING-CUSTOMER-METRICS

**Context**: The Data Analysts in our team currently depend on data engineers to implement data pipelines for new customer metrics. This creates a bottleneck and slows down analysis.

**Goal**: to build a simple prototype for that system.

**Objective**: A lightweight prototype system that allows Data Analysts to define customer metrics using YAML + SQL, with automatic materialization in DuckDB.

## Setup instructions
### Prerequisites
- Python 3.12+
- Using Linux

### Installation Steps
1. **Clone the project**  `git clone https://github.com/trungbac11/self-serving-customer-metrics.git`

2. **Create virtual enviroments**
`python -m venv venv`

3. **Active**
`source venv/bin/activate`

4. **Install dependencies:**
`pip install --upgrade pip`
`pip install -r requirements.txt`

5. **Initialize the database into DuckDB:**
`python src/setup_database.py`

## Development guideline

### Project Structure
<img width="799" height="351" alt="image" src="https://github.com/user-attachments/assets/0a37675a-0f76-4570-8de8-9237fec6dc42" />
```text
project/
├── data/                   # Source CSV files
├── metrics/               # Metric definitions (YAML)
│   ├── lifetime_revenue.yaml
│   ├── avg_order_revenue.yaml
│   └── customer_geography.yaml
├── src/                   # Python scripts
│   ├── validate_yaml.py   # YAML validation
│   ├── run_metrics.py     # Metric execution
│   ├── setup_database.py  # DB initialization
│   └── clean_database.py # DB cleanup
├── requirements.txt       # Dependencies
├── README.md              # Documentation
└── AI_USAGE.md            # AI collaboration documentation
```

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
