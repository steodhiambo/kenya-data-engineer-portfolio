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
        
        # Make a copy to avoid modifying the original dataframe
        transformed_df = df.copy()
        
        # Convert date columns to datetime
        transformed_df['TransactionStartDate'] = pd.to_datetime(transformed_df['TransactionStartDate'])
        transformed_df['TransactionEndDate'] = pd.to_datetime(transformed_df['TransactionEndDate'])
        
        # Calculate transaction duration in seconds
        transformed_df['TransactionDuration'] = (
            transformed_df['TransactionEndDate'] - transformed_df['TransactionStartDate']
        ).dt.total_seconds()
        
        # Convert TransAmount to numeric
        transformed_df['TransAmount'] = pd.to_numeric(transformed_df['TransAmount'], errors='coerce')
        
        # Add transaction date and time components
        transformed_df['TransactionDate'] = transformed_df['TransactionStartDate'].dt.date
        transformed_df['TransactionHour'] = transformed_df['TransactionStartDate'].dt.hour
        transformed_df['TransactionDayOfWeek'] = transformed_df['TransactionStartDate'].dt.day_name()
        
        # Categorize transaction amounts
        bins = [0, 500, 1000, 2000, 5000, float('inf')]
        labels = ['Very Small', 'Small', 'Medium', 'Large', 'Very Large']
        transformed_df['AmountCategory'] = pd.cut(transformed_df['TransAmount'], bins=bins, labels=labels)
        
        # Calculate transaction fees (assuming 1% for amounts under 1000, 0.5% for higher amounts)
        def calculate_fees(amount):
            if amount < 1000:
                return amount * 0.01
            else:
                return amount * 0.005
        transformed_df['TransactionFee'] = transformed_df['TransAmount'].apply(calculate_fees)
        transformed_df['NetAmount'] = transformed_df['TransAmount'] - transformed_df['TransactionFee']
        
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
    
if __name__ == "__main__":
    main()