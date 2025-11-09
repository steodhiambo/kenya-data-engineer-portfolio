"""
Utility script to generate additional sample M-Pesa transaction data
"""
import pandas as pd
import random
from datetime import datetime, timedelta
import os

def generate_sample_data(num_records=100):
    """
    Generate sample M-Pesa transaction data
    
    Args:
        num_records (int): Number of records to generate
        
    Returns:
        pandas.DataFrame: Generated transaction data
    """
    transaction_types = ['Pay Bill', 'Send Money', 'Withdrawal', 'Deposit', 'Airtime Purchase']
    receivers = [
        'ABC Company', 'XYZ Ltd', 'KPLC', 'Safaricom Ltd', 'Water Services', 
        'Bank Account', 'M-Pesa Agent', 'Retail Store', 'Service Provider', 'Individual'
    ]
    senders = [
        'John Doe', 'Jane Smith', 'Samuel Otieno', 'David Kamau', 'Mary Wanjiku',
        'Robert Omondi', 'Grace Njoki', 'George Maina', 'Carol Wangari', 'Thomas Mwangi',
        'Ann Karimi', 'Joseph Muli', 'Faith Wambui', 'Patricia Chepkorir', 'James Mwangi'
    ]
    
    data = []
    base_date = datetime(2023, 1, 1)
    
    for i in range(num_records):
        start_time = base_date + timedelta(minutes=random.randint(0, 30*24*60))  # Random time within a month
        duration = random.randint(1, 30)  # Transaction duration in seconds
        end_time = start_time + timedelta(seconds=duration)
        
        trans_type = random.choice(transaction_types)
        amount = random.choice([
            round(random.uniform(50, 500), 2),    # Small amounts
            round(random.uniform(500, 2000), 2),  # Medium amounts
            round(random.uniform(2000, 10000), 2) # Large amounts
        ])
        
        record = {
            'TransactionStartDate': start_time.strftime("%Y-%m-%d %H:%M:%S"),
            'TransactionEndDate': end_time.strftime("%Y-%m-%d %H:%M:%S"),
            'TransactionType': trans_type,
            'TransID': f"M{random.choice('ABCDE')}{''.join([str(random.randint(0, 9)) for _ in range(3)])}{random.choice('FGHIJ')}",
            'TransAmount': amount,
            'TransReceiver': random.choice(receivers),
            'TransSender': random.choice(senders)
        }
        data.append(record)
    
    return pd.DataFrame(data)

def main():
    print("Generating additional sample M-Pesa transaction data...")
    
    # Generate sample data
    sample_df = generate_sample_data(50)  # Generate 50 additional records
    
    # Save to CSV
    output_file = 'additional_mpesa_sample.csv'
    sample_df.to_csv(output_file, index=False)
    
    print(f"Generated {len(sample_df)} additional records and saved to {output_file}")
    print(f"Sample of generated data:")
    print(sample_df.head(10))
    
    # Optionally append to existing sample file
    append_existing = input("\nWould you like to append this data to mpesa_sample.csv? (y/n): ")
    if append_existing.lower() == 'y':
        if os.path.exists('mpesa_sample.csv'):
            existing_df = pd.read_csv('mpesa_sample.csv')
            combined_df = pd.concat([existing_df, sample_df], ignore_index=True)
            combined_df.to_csv('mpesa_sample.csv', index=False)
            print(f"Appended {len(sample_df)} records to mpesa_sample.csv")
        else:
            print("mpesa_sample.csv not found. Created new file with generated data.")
            sample_df.to_csv('mpesa_sample.csv', index=False)

if __name__ == "__main__":
    main()