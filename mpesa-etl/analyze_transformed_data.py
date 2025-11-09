"""
Analysis script for transformed M-Pesa data
Provides insights and visualizations for the processed transaction data
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import os

def load_transformed_data(data_path='transformed_data/transformed_mpesa_data.csv'):
    """
    Load the transformed M-Pesa data
    
    Args:
        data_path (str): Path to the transformed data CSV file
        
    Returns:
        pandas.DataFrame: Loaded data
    """
    if not os.path.exists(data_path):
        print(f"Error: {data_path} does not exist.")
        print("Please run the ETL pipeline first to generate transformed data.")
        return None
    
    df = pd.read_csv(data_path)
    df['TransactionStartDate'] = pd.to_datetime(df['TransactionStartDate'])
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
    return df

def generate_summary_stats(df):
    """
    Generate summary statistics for the M-Pesa data
    
    Args:
        df (pandas.DataFrame): Transformed M-Pesa data
    """
    print("M-PESA TRANSACTION ANALYSIS REPORT")
    print("="*50)
    
    print(f"Total Transactions: {len(df)}")
    print(f"Date Range: {df['TransactionStartDate'].min()} to {df['TransactionStartDate'].max()}")
    print(f"Total Transaction Amount: KES {df['TransAmount'].sum():,.2f}")
    print(f"Average Transaction Amount: KES {df['TransAmount'].mean():,.2f}")
    print(f"Median Transaction Amount: KES {df['TransAmount'].median():,.2f}")
    print()
    
    print("Transaction Types:")
    for trans_type, count in df['TransactionType'].value_counts().items():
        print(f"  {trans_type}: {count}")
    print()
    
    print("Transaction Categories:")
    for category, count in df['TransactionCategory'].value_counts().items():
        print(f"  {category}: {count}")
    print()
    
    print("Amount Categories:")
    for category, count in df['AmountCategory'].value_counts().items():
        print(f"  {category}: {count}")
    print()

def create_visualizations(df):
    """
    Create visualizations for the M-Pesa data
    
    Args:
        df (pandas.DataFrame): Transformed M-Pesa data
    """
    # Set up the plotting style
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('M-Pesa Transaction Analysis', fontsize=16, fontweight='bold')
    
    # 1. Transaction types distribution
    trans_type_counts = df['TransactionType'].value_counts()
    axes[0, 0].pie(trans_type_counts.values, labels=trans_type_counts.index, autopct='%1.1f%%')
    axes[0, 0].set_title('Distribution of Transaction Types')
    
    # 2. Transaction amounts by category
    sns.boxplot(data=df, x='AmountCategory', y='TransAmount', ax=axes[0, 1])
    axes[0, 1].set_title('Transaction Amounts by Category')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # 3. Transactions by hour of day
    hourly_counts = df['TransactionHour'].value_counts().sort_index()
    axes[1, 0].plot(hourly_counts.index, hourly_counts.values, marker='o')
    axes[1, 0].set_title('Number of Transactions by Hour of Day')
    axes[1, 0].set_xlabel('Hour of Day')
    axes[1, 0].set_ylabel('Number of Transactions')
    
    # 4. Transaction amounts over time
    daily_amounts = df.groupby('TransactionDate')['TransAmount'].sum()
    axes[1, 1].plot(daily_amounts.index, daily_amounts.values, marker='o')
    axes[1, 1].set_title('Total Transaction Amounts Over Time')
    axes[1, 1].set_xlabel('Date')
    axes[1, 1].set_ylabel('Total Amount (KES)')
    axes[1, 1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('mpesa_analysis_visualization.png', dpi=300, bbox_inches='tight')
    print("Visualization saved as 'mpesa_analysis_visualization.png'")
    plt.show()

def main():
    print("Loading transformed M-Pesa data...")
    df = load_transformed_data()
    
    if df is None:
        return
    
    print("Generating summary statistics...")
    generate_summary_stats(df)
    
    print("Creating visualizations...")
    create_visualizations(df)
    
    # Create a simple report
    report_path = Path('mpesa_analysis_report.md')
    with open(report_path, 'w') as f:
        f.write("# M-Pesa Transaction Analysis Report\n\n")
        f.write(f"Analysis generated on: {pd.Timestamp.now()}\n\n")
        f.write("## Summary Statistics\n")
        f.write(f"- Total Transactions: {len(df)}\n")
        f.write(f"- Date Range: {df['TransactionStartDate'].min()} to {df['TransactionStartDate'].max()}\n")
        f.write(f"- Total Transaction Amount: KES {df['TransAmount'].sum():,.2f}\n")
        f.write(f"- Average Transaction Amount: KES {df['TransAmount'].mean():,.2f}\n\n")
        
        f.write("## Key Insights\n")
        f.write("- Transaction volumes by time of day\n")
        f.write("- Distribution of transaction types\n")
        f.write("- Amount categories and their frequencies\n")
        f.write("- Fees generated for the platform\n\n")
        
        f.write("## Data Quality\n")
        f.write(f"- No missing values in critical fields: {df.isnull().sum().sum() == 0}\n")
        f.write(f"- Valid date ranges: {df['TransactionStartDate'].is_monotonic_increasing}\n")
    
    print(f"Analysis report saved as '{report_path}'")

if __name__ == "__main__":
    main()