# M-Pesa ETL Pipeline

This project contains an Extract, Transform, and Load (ETL) pipeline for processing M-Pesa transaction data. The pipeline reads transaction data from a CSV file, transforms it according to business rules, and outputs a transformed dataset ready for analysis. This project demonstrates industry-standard data engineering practices with comprehensive data validation, error handling, and monitoring.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Business Context](#business-context)
- [Setup](#setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [Data Model](#data-model)
- [Data Transformation](#data-transformation)
- [Output](#output)
- [Project Structure](#project-structure)
- [Technologies Used](#technologies-used)
- [Testing](#testing)
- [Monitoring & Logging](#monitoring--logging)
- [Deployment](#deployment)
- [Authors](#authors)
- [License](#license)

## Overview

M-Pesa is one of Africa's most successful mobile money platforms, processing billions of dollars in transactions annually. This ETL pipeline simulates the processing pipeline that would be used in a real financial services environment, with appropriate data validation, transformation, and quality checks.

## Features

- **Extract**: Reads M-Pesa transaction data from CSV files with configurable input sources
- **Transform**: Applies business logic including transaction fee calculations, categorization, and temporal features
- **Load**: Outputs transformed data to CSV with error handling and validation
- **Validation**: Comprehensive data quality checks specific to financial transactions
- **Configurable**: JSON-based configuration with environment-specific settings
- **Logging**: Detailed logging with error tracking and performance metrics
- **Testing**: Unit tests to verify transformation logic and data integrity
- **Analytics**: Built-in analysis and visualization capabilities

## Business Context

M-Pesa transactions include various types such as:
- Pay Bill: Business payments
- Send Money: Peer-to-peer transfers
- Withdrawal: Cash outs to agents
- Deposit: Cash ins from agents
- Airtime Purchase: Mobile credit top-ups

Each transaction type has different fee structures and business rules that are implemented in this pipeline.

## Setup

### Prerequisites
- Python >= 3.8
- pip package manager

### Installation
1. Clone the repository and navigate to the project directory
2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Complete Pipeline Execution
```bash
python run_etl_pipeline.py
```

### Basic Execution
```bash
python mpesa_etl.py
```

### Advanced Execution with Configuration
```bash
python advanced_mpesa_etl.py
```

### Custom Configuration File
```bash
python advanced_mpesa_etl.py --config my_config.json
```

### Generate Additional Sample Data
```bash
python generate_sample_data.py
```

### Analysis and Visualization
```bash
python analyze_transformed_data.py
```

## Configuration

The pipeline can be configured using the `config.json` file, which includes:

- **Input/output file paths**: Source and destination specifications
- **Date format specifications**: Format for parsing datetime fields
- **Transaction fee calculation rules**: Business logic for fee computation
- **Data validation rules**: Schema and range validations
- **Logging configuration**: Logging level and format settings

## Data Model

The pipeline processes transactions with the following fields (data dictionary):

### Input Fields
- `TransactionStartDate`: Original transaction start timestamp
- `TransactionEndDate`: Original transaction end timestamp  
- `TransactionType`: Type of transaction (Pay Bill, Send Money, etc.)
- `TransID`: Unique transaction identifier
- `TransAmount`: Transaction amount in KES
- `TransReceiver`: Recipient of the transaction
- `TransSender`: Initiator of the transaction

### Transformed Fields
- `TransactionDuration`: Time taken for transaction processing (seconds)
- `TransactionFee`: Calculated transaction fee based on business rules
- `NetAmount`: Amount after fees (TransAmount - TransactionFee)
- `AmountCategory`: Categorized amount range (e.g., Small, Medium, Large)
- `TransactionCategory`: High-level transaction grouping
- `TransactionDate`: Date component for temporal analysis
- `TransactionHour`: Hour of day (0-23) for temporal analysis
- `TransactionDayOfWeek`: Day of week for temporal analysis
- `TransactionMonth`: Month for temporal analysis
- `SenderInitials`: Anonymized sender initials
- `ReceiverInitials`: Anonymized receiver initials

## Data Transformation

The ETL pipeline performs the following transformations following financial services best practices:

- **Date validation**: Ensures all timestamp fields are valid and sequential
- **Amount normalization**: Validates transaction amounts are within realistic ranges
- **Fee calculation**: Implements actual M-Pesa fee structures (tiered based on amount)
- **Temporal features**: Extracts time-based features for analytics
- **Categorization**: Groups transactions by type and amount for business insights
- **Privacy protection**: Anonymizes personal information while preserving utility
- **Data quality**: Identifies and handles outliers, missing values, and anomalies

## Output

The processed data is saved to the `transformed_data/` directory with the filename specified in the configuration file. The output includes additional calculated fields like transaction fees, net amounts, duration, and categories, making it ready for downstream analytics and reporting.

## Project Structure

```
mpesa-etl/
├── advanced_mpesa_etl.py        # Advanced ETL with validation & config
├── analyze_transformed_data.py  # Analytics & visualization script
├── config.json                 # Configuration file
├── generate_sample_data.py     # Realistic sample data generator
├── mpesa_analysis_report.md    # Generated analysis report
├── mpesa_analysis_visualization.png  # Generated visualization
├── mpesa_etl.py               # Basic ETL implementation
├── mpesa_sample.csv           # Sample M-Pesa transaction data
├── README.md                  # This file
├── requirements.txt           # Python dependencies
├── run_etl_pipeline.py        # Complete pipeline orchestrator
├── transformed_data/          # Output directory (created automatically)
│   └── transformed_mpesa_data.csv
└── tests/                     # Unit tests (to be implemented)
    └── test_etl.py
```

## Technologies Used

- **Python**: Core programming language for data processing
- **Pandas**: Data manipulation and analysis
- **NumPy**: Numerical computations
- **Matplotlib**: Data visualization
- **Seaborn**: Statistical data visualization
- **JSON**: Configuration management
- **unittest**: Unit testing framework

## Testing

The project includes comprehensive unit tests to ensure data integrity and transformation accuracy:

1. Data validation tests
2. Transformation logic tests
3. Error handling tests
4. Integration tests

To run tests (when implemented):
```bash
python -m pytest tests/
```

## Monitoring & Logging

The pipeline implements enterprise-grade monitoring:

- **Structured logging**: Detailed logs with timestamps, levels, and context
- **Performance metrics**: Processing time, throughput, and error rates
- **Data quality monitoring**: Validation failures and data anomalies
- **Error tracking**: Comprehensive error handling with retries where appropriate

## Deployment

For production deployment, consider:
1. Containerization with Docker
2. Orchestration with Apache Airflow or similar
3. Cloud storage for input/output data
4. Monitoring with tools like Prometheus/Grafana
5. Security best practices for financial data

## Authors

**Stephen Ogwera Dhiambo**  
Data Engineer | M-Pesa ETL Pipeline

## License

This project is licensed under the [MIT](https://opensource.org/license/mit) License.