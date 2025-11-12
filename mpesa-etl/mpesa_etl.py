"""
M-Pesa Data ETL Pipeline
Extracts, transforms, and loads M-Pesa transaction data for analysis.
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MPesaETLPipeline:
    def __init__(self, input_file_path, output_dir='transformed_data'):
        """
        Initialize the ETL pipeline

        Args:
            input_file_path (str): Path to the input CSV file
            output_dir (str): Directory to store transformed data
        """
        self.input_file_path = input_file_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def validate_data(self, df):
        """
        Validate the extracted data against required schema and business rules

        Args:
            df (pandas.DataFrame): DataFrame to validate

        Returns:
            bool: True if data is valid, False otherwise
        """
        validation_results = {
            'schema_valid': True,
            'business_rules_valid': True,
            'data_quality_score': 100.0
        }

        # Check required columns exist
        required_columns = [
            'TransactionStartDate', 'TransactionEndDate', 'TransactionType',
            'TransID', 'TransAmount', 'TransReceiver', 'TransSender'
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            validation_results['schema_valid'] = False

        # Check for missing values in critical fields
        critical_fields = ['TransAmount', 'TransactionStartDate', 'TransactionType']
        missing_values_count = 0
        
        for field in critical_fields:
            if field in df.columns:
                missing_count = df[field].isnull().sum()
                missing_values_count += missing_count
                if missing_count > 0:
                    logger.warning(f"Found {missing_count} missing values in critical field: {field}")
        
        if missing_values_count > 0:
            validation_results['business_rules_valid'] = False
            # Calculate data quality score based on missing values
            validation_results['data_quality_score'] = max(0, 100 - (missing_values_count / len(df) * 100))

        # Validate transaction amount range (M-Pesa limits)
        min_val, max_val = 50, 150000  # KES 50 to KES 150,000 (M-Pesa limits as of 2023)
        invalid_amounts = df[
            (df['TransAmount'] < min_val) |
            (df['TransAmount'] > max_val)
        ]

        if len(invalid_amounts) > 0:
            logger.warning(f"Found {len(invalid_amounts)} transactions with invalid amounts (outside {min_val}-{max_val} range)")
            validation_results['business_rules_valid'] = False
            validation_results['data_quality_score'] = max(0, validation_results['data_quality_score'] - (len(invalid_amounts) / len(df) * 20))

        # Validate date ranges - Start date should not be after end date
        invalid_dates = df[df['TransactionStartDate'] > df['TransactionEndDate']]
        if len(invalid_dates) > 0:
            logger.error(f"Found {len(invalid_dates)} transactions with start date after end date")
            validation_results['business_rules_valid'] = False
            validation_results['data_quality_score'] = max(0, validation_results['data_quality_score'] - (len(invalid_dates) / len(df) * 15))

        # Validate date ranges - should be within reasonable timeframe
        current_time = pd.Timestamp.now()
        future_transactions = df[df['TransactionStartDate'] > current_time]
        if len(future_transactions) > 0:
            logger.error(f"Found {len(future_transactions)} transactions with future dates")
            validation_results['business_rules_valid'] = False
            validation_results['data_quality_score'] = max(0, validation_results['data_quality_score'] - (len(future_transactions) / len(df) * 10))

        logger.info(f"Data validation results: Schema Valid = {validation_results['schema_valid']}, "
                         f"Business Rules Valid = {validation_results['business_rules_valid']}, "
                         f"Data Quality Score = {validation_results['data_quality_score']:.2f}%")
        
        overall_valid = validation_results['schema_valid'] and validation_results['business_rules_valid']
        return overall_valid

    def data_quality_report(self, df):
        """
        Generate a data quality report for the dataset

        Args:
            df (pandas.DataFrame): DataFrame to analyze

        Returns:
            dict: Data quality metrics
        """
        quality_report = {}

        # Overall statistics
        quality_report['total_records'] = len(df)
        quality_report['missing_values_per_column'] = df.isnull().sum().to_dict()
        quality_report['duplicate_records'] = df.duplicated().sum()
        
        # Specific field quality checks
        if 'TransAmount' in df.columns:
            quality_report['amount_outliers'] = len(df[df['TransAmount'] > df['TransAmount'].quantile(0.95)])
        
        if 'TransactionType' in df.columns:
            quality_report['transaction_type_distribution'] = df['TransactionType'].value_counts().to_dict()
        
        # Date quality checks
        if 'TransactionStartDate' in df.columns and 'TransactionEndDate' in df.columns:
            quality_report['negative_duration'] = len(df[df['TransactionStartDate'] > df['TransactionEndDate']])
        
        logger.info("Data quality report generated")
        return quality_report

    def extract(self):
        """
        Extract data from the source CSV file

        Returns:
            pandas.DataFrame: Raw data from the CSV file
        """
        logger.info(f"Extracting data from {self.input_file_path}")
        try:
            df = pd.read_csv(self.input_file_path)
            logger.info(f"Successfully extracted {len(df)} records")
            
            # Generate initial data quality report
            quality_report = self.data_quality_report(df)
            logger.info(f"Initial data quality: {quality_report}")
            
            return df
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            raise

    def transform(self, df):
        """
        Transform the extracted data

        Args:
            df (pandas.DataFrame): Raw data from the CSV file

        Returns:
            pandas.DataFrame: Transformed data
        """
        logger.info("Starting data transformation")

        # Validate the data
        if not self.validate_data(df):
            # Log warning but continue processing if possible
            logger.warning("Data validation failed, but continuing with transformation")
        
        # Make a copy to avoid modifying the original dataframe
        transformed_df = df.copy()

        # Convert date columns to datetime
        transformed_df['TransactionStartDate'] = pd.to_datetime(transformed_df['TransactionStartDate'], errors='coerce')
        transformed_df['TransactionEndDate'] = pd.to_datetime(transformed_df['TransactionEndDate'], errors='coerce')

        # Calculate transaction duration in seconds
        transformed_df['TransactionDuration'] = (
            transformed_df['TransactionEndDate'] - transformed_df['TransactionStartDate']
        ).dt.total_seconds()

        # Handle invalid durations (negative or too long)
        transformed_df.loc[transformed_df['TransactionDuration'] < 0, 'TransactionDuration'] = 0
        transformed_df.loc[transformed_df['TransactionDuration'] > 3600, 'TransactionDuration'] = 3600  # Cap at 1 hour

        # Convert TransAmount to numeric
        transformed_df['TransAmount'] = pd.to_numeric(transformed_df['TransAmount'], errors='coerce')

        # Add transaction date and time components
        transformed_df['TransactionDate'] = transformed_df['TransactionStartDate'].dt.date
        transformed_df['TransactionHour'] = transformed_df['TransactionStartDate'].dt.hour
        transformed_df['TransactionDayOfWeek'] = transformed_df['TransactionStartDate'].dt.day_name()
        transformed_df['TransactionMonth'] = transformed_df['TransactionStartDate'].dt.month_name()
        transformed_df['TransactionYear'] = transformed_df['TransactionStartDate'].dt.year

        # Categorize transaction amounts based on actual M-Pesa tier structure
        bins = [0, 100, 500, 3000, 10000, 35000, 150000, float('inf')]
        labels = ['Very Small (≤100)', 'Small (101-500)', 'Medium (501-3K)', 
                  'Large (3K-10K)', 'X-Large (10K-35K)', '2X-Large (35K-150K)', 'Jumbo (>150K)']
        transformed_df['AmountCategory'] = pd.cut(transformed_df['TransAmount'], bins=bins, labels=labels)

        # Calculate transaction fees (simplified M-Pesa fee structure)
        def calculate_fees(amount):
            if pd.isna(amount):
                return np.nan
            # Apply simplified tiered fee structure
            if amount < 1000:
                fee = amount * 0.01  # 1% for amounts under 1000
                # Apply minimum fee of KES 5 for low amounts
                return max(round(fee, 2), 5.0)
            else:
                fee = amount * 0.005  # 0.5% for amounts 1000 and above
                # Apply maximum fee cap of KES 100
                return min(round(fee, 2), 100.0)

        transformed_df['TransactionFee'] = transformed_df['TransAmount'].apply(calculate_fees)
        transformed_df['NetAmount'] = transformed_df['TransAmount'] - transformed_df['TransactionFee']

        # Categorize transaction types with more precision
        def categorize_transaction_type(trans_type):
            trans_type_lower = trans_type.lower()
            if 'pay' in trans_type_lower or 'bill' in trans_type_lower:
                return 'Bill Payment'
            elif 'send' in trans_type_lower or 'money' in trans_type_lower and 'send' in trans_type_lower:
                return 'Peer-to-Peer Transfer'
            elif 'withdrawal' in trans_type_lower:
                return 'Cash Out'
            elif 'deposit' in trans_type_lower:
                return 'Cash In'
            elif 'airtime' in trans_type_lower:
                return 'Airtime/Data Purchase'
            else:
                return 'Other'
                
        transformed_df['TransactionCategory'] = transformed_df['TransactionType'].apply(categorize_transaction_type)

        # Add sender and receiver initials for privacy
        transformed_df['SenderInitials'] = transformed_df['TransSender'].apply(
            lambda x: ''.join([name[0] for name in str(x).split()]) if pd.notna(x) and str(x) != 'nan' else 'UNKNOWN'
        )
        transformed_df['ReceiverInitials'] = transformed_df['TransReceiver'].apply(
            lambda x: ''.join([name[0] for name in str(x).split()]) if pd.notna(x) and str(x) != 'nan' else 'UNKNOWN'
        )

        # Add transaction summary flags for analysis
        transformed_df['IsWeekend'] = transformed_df['TransactionStartDate'].dt.dayofweek.isin([5, 6])
        transformed_df['IsBusinessHour'] = transformed_df['TransactionHour'].between(8, 17)  # 8 AM to 5 PM
        transformed_df['IsHighValue'] = transformed_df['TransAmount'] > 10000  # High-value threshold

        # Sort by transaction date
        transformed_df = transformed_df.sort_values(by='TransactionStartDate')

        logger.info(f"Successfully transformed {len(transformed_df)} records")
        return transformed_df

    def load(self, transformed_df, output_filename='transformed_mpesa_data.csv'):
        """
        Load transformed data to a new CSV file

        Args:
            transformed_df (pandas.DataFrame): Transformed data
            output_filename (str): Name of the output file
        """
        output_path = self.output_dir / output_filename
        logger.info(f"Loading data to {output_path}")

        try:
            transformed_df.to_csv(output_path, index=False)
            logger.info(f"Data successfully loaded to {output_path}")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def run(self):
        """
        Run the complete ETL pipeline
        """
        logger.info("Starting ETL pipeline")
        try:
            raw_data = self.extract()
            transformed_data = self.transform(raw_data)
            self.load(transformed_data)
            logger.info("ETL pipeline completed successfully")
            return transformed_data
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

def main():
    # Define file paths
    input_file = 'mpesa_sample.csv'

    # Check if input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file {input_file} does not exist")
        return

    # Create and run the ETL pipeline
    etl_pipeline = MPesaETLPipeline(input_file_path=input_file)
    result = etl_pipeline.run()

    # Print summary statistics
    print("\nM-Pesa Transaction Summary:")
    print(f"Total Transactions: {len(result)}")
    print(f"Date Range: {result['TransactionStartDate'].min()} to {result['TransactionStartDate'].max()}")
    print(f"Total Transaction Amount: KES {result['TransAmount'].sum():,.2f}")
    print(f"Average Transaction Amount: KES {result['TransAmount'].mean():,.2f}")
    print(f"Transaction Types: {result['TransactionType'].value_counts().to_dict()}")
    print(f"Transaction Categories: {result['TransactionCategory'].value_counts().to_dict()}")

    # Show additional metrics
    print("\nAdditional Business Metrics:")
    print(f"Average Transaction Fee: KES {result['TransactionFee'].mean():.2f}")
    print(f"Total Fees Generated: KES {result['TransactionFee'].sum():,.2f}")
    print(f"Average Transaction Duration: {result['TransactionDuration'].mean():.2f} seconds")
    print(f"High-value transactions (KES > 10,000): {len(result[result['IsHighValue']])}")


if __name__ == "__main__":
    main()