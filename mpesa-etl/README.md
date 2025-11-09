# M-Pesa ETL Pipeline

This project contains an Extract, Transform, and Load (ETL) pipeline for processing M-Pesa transaction data. The pipeline reads transaction data from a CSV file, transforms it according to business rules, and outputs a transformed dataset ready for analysis.

## Features

- Extracts M-Pesa transaction data from CSV files
- Transforms data with date/time calculations, fee computations, and categorization
- Validates data against required schema
- Outputs transformed data in a new CSV file
- Configurable through JSON configuration file
- Comprehensive logging

## Setup

1. Clone the repository and navigate to the project directory
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

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

## Configuration

The pipeline can be configured using the `config.json` file, which includes:

- Input/output file paths
- Date format specifications
- Transaction fee calculation rules
- Data validation rules
- Logging configuration

## Data Transformation

The ETL pipeline performs the following transformations:

- Converts date strings to datetime objects
- Calculates transaction duration
- Computes transaction fees (1% for amounts under KES 1000, 0.5% for higher amounts)
- Categorizes transactions by amount and type
- Adds time-based features (hour, day of week, month)
- Anonymizes personal information by using initials

## Output

The processed data is saved to the `transformed_data/` directory with the filename specified in the configuration file. The output includes additional calculated fields like transaction fees, net amounts, duration, and categories.

## File Structure

```
mpesa-etl/
├── mpesa_sample.csv          # Sample input data
├── mpesa_etl.py             # Basic ETL script
├── advanced_mpesa_etl.py    # Advanced ETL script with config support
├── config.json              # Configuration file
├── requirements.txt         # Python dependencies
├── README.md               # This file
└── transformed_data/       # Output directory (created automatically)
    └── transformed_mpesa_data.csv
```

## Sample Output Fields

- `TransactionStartDate`: Original transaction start time
- `TransactionEndDate`: Original transaction end time
- `TransactionDuration`: Duration in seconds
- `TransAmount`: Original transaction amount
- `TransactionFee`: Calculated transaction fee
- `NetAmount`: Amount after fees
- `AmountCategory`: Category based on amount range
- `TransactionCategory`: Payment, Transfer, or Deposit/Withdrawal
- `TransactionDate`: Date component only
- `TransactionHour`: Hour of day (0-23)
- `TransactionDayOfWeek`: Name of the day
- `TransactionMonth`: Name of the month
- `SenderInitials`: Initials of sender
- `ReceiverInitials`: Initials of receiver