"""
Data loader script for realistic M-Pesa transaction data
This script loads realistic M-Pesa transaction data based on authentic patterns from Kenya's mobile money ecosystem
"""
import pandas as pd
import random
from datetime import datetime, timedelta
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_realistic_transaction_data():
    """
    Load realistic M-Pesa transaction data with patterns based on actual usage in Kenya
    
    Returns:
        pandas.DataFrame: Realistic transaction data with authentic patterns
    """
    logger.info("Loading realistic M-Pesa transaction data with authentic patterns")
    
    # Realistic transaction data based on M-Pesa usage patterns in Kenya
    # Including more authentic business names, transaction types, amounts, and names
    realistic_transactions = [
        # Pay Bill transactions - common utilities and services
        {'TransactionStartDate': '2023-01-01 08:30:15', 'TransactionEndDate': '2023-01-01 08:30:18', 'TransactionType': 'Pay Bill', 'TransID': 'LBS1A', 'TransAmount': 3480.00, 'TransReceiver': 'KPLC-Kenya Power', 'TransSender': 'Mercy Wanjiku Maina'},
        {'TransactionStartDate': '2023-01-01 09:12:45', 'TransactionEndDate': '2023-01-01 09:12:47', 'TransactionType': 'Pay Bill', 'TransID': 'LBX2B', 'TransAmount': 1200.00, 'TransReceiver': 'Safaricom PLC', 'TransSender': 'James Odhiambo'},
        {'TransactionStartDate': '2023-01-01 10:30:22', 'TransactionEndDate': '2023-01-01 10:30:25', 'TransactionType': 'Pay Bill', 'TransID': 'LBY3C', 'TransAmount': 2500.00, 'TransReceiver': 'Daima Sacco', 'TransSender': 'Grace Njeri Mwangi'},
        {'TransactionStartDate': '2023-01-01 11:45:30', 'TransactionEndDate': '2023-01-01 11:45:33', 'TransactionType': 'Pay Bill', 'TransID': 'LCC4D', 'TransAmount': 1500.00, 'TransReceiver': 'KRA Services', 'TransSender': 'Samuel Otieno Kipchirchir'},
        {'TransactionStartDate': '2023-01-01 12:20:10', 'TransactionEndDate': '2023-01-01 12:20:13', 'TransactionType': 'Pay Bill', 'TransID': 'LDV5E', 'TransAmount': 1800.00, 'TransReceiver': 'Chania River Water', 'TransSender': 'Mary Wanjiku Kamau'},
        {'TransactionStartDate': '2023-01-01 15:45:20', 'TransactionEndDate': '2023-01-01 15:45:23', 'TransactionType': 'Pay Bill', 'TransID': 'LEF6F', 'TransAmount': 10000.00, 'TransReceiver': 'University of Nairobi', 'TransSender': 'David Kipchoge'},
        {'TransactionStartDate': '2023-01-02 09:30:15', 'TransactionEndDate': '2023-01-02 09:30:18', 'TransactionType': 'Pay Bill', 'TransID': 'LGH7G', 'TransAmount': 4500.00, 'TransReceiver': 'NHIF Kenya', 'TransSender': 'Ann Wangari Mburu'},
        {'TransactionStartDate': '2023-01-02 11:20:45', 'TransactionEndDate': '2023-01-02 11:20:48', 'TransactionType': 'Pay Bill', 'TransID': 'LHI8H', 'TransAmount': 7500.00, 'TransReceiver': 'KRA PAYMENTS', 'TransSender': 'Peter Kipkemboi'},
        {'TransactionStartDate': '2023-01-02 13:15:30', 'TransactionEndDate': '2023-01-02 13:15:33', 'TransactionType': 'Pay Bill', 'TransID': 'LIJ9I', 'TransAmount': 2000.00, 'TransReceiver': 'Safaricom Postpaid', 'TransSender': 'Cynthia Chepkemoi'},
        {'TransactionStartDate': '2023-01-02 14:55:10', 'TransactionEndDate': '2023-01-02 14:55:13', 'TransactionType': 'Pay Bill', 'TransID': 'LJK10J', 'TransAmount': 3000.00, 'TransReceiver': 'Co-op Bank Loan', 'TransSender': 'Samuel Kipyego'},
        
        # Send Money transactions - P2P transfers and business payments
        {'TransactionStartDate': '2023-01-01 13:05:40', 'TransactionEndDate': '2023-01-01 13:05:43', 'TransactionType': 'Send Money', 'TransID': 'MSJ11K', 'TransAmount': 1500.00, 'TransReceiver': 'John Omondi', 'TransSender': 'Jane Wanjiku'},
        {'TransactionStartDate': '2023-01-01 14:17:25', 'TransactionEndDate': '2023-01-01 14:17:28', 'TransactionType': 'Send Money', 'TransID': 'MTB12L', 'TransAmount': 3000.00, 'TransReceiver': 'Daniel Njoroge', 'TransSender': 'Sarah Mumbi'},
        {'TransactionStartDate': '2023-01-01 15:35:55', 'TransactionEndDate': '2023-01-01 15:35:58', 'TransactionType': 'Send Money', 'TransID': 'MUC13M', 'TransAmount': 2000.00, 'TransReceiver': 'Mike Waweru', 'TransSender': 'Esther Wangari'},
        {'TransactionStartDate': '2023-01-01 16:42:12', 'TransactionEndDate': '2023-01-01 16:42:15', 'TransactionType': 'Send Money', 'TransID': 'MVD14N', 'TransAmount': 5000.00, 'TransReceiver': 'Faith Mutua', 'TransSender': 'Peter Kimani'},
        {'TransactionStartDate': '2023-01-02 17:28:30', 'TransactionEndDate': '2023-01-02 17:28:33', 'TransactionType': 'Send Money', 'TransID': 'MWE15O', 'TransAmount': 1000.00, 'TransReceiver': 'Joseph Muthomi', 'TransSender': 'Agnes Nyambura'},
        {'TransactionStartDate': '2023-01-02 10:15:45', 'TransactionEndDate': '2023-01-02 10:15:48', 'TransactionType': 'Send Money', 'TransID': 'MXF16P', 'TransAmount': 8000.00, 'TransReceiver': 'Business Partner', 'TransSender': 'Business Revenue'},
        {'TransactionStartDate': '2023-01-02 11:50:20', 'TransactionEndDate': '2023-01-02 11:50:23', 'TransactionType': 'Send Money', 'TransID': 'MYG17Q', 'TransAmount': 12000.00, 'TransReceiver': 'School Fees Payment', 'TransSender': 'Parent Support'},
        {'TransactionStartDate': '2023-01-03 13:25:10', 'TransactionEndDate': '2023-01-03 13:25:13', 'TransactionType': 'Send Money', 'TransID': 'MZH18R', 'TransAmount': 15000.00, 'TransReceiver': 'KCB Bank', 'TransSender': 'Investment Return'},
        {'TransactionStartDate': '2023-01-03 14:40:35', 'TransactionEndDate': '2023-01-03 14:40:38', 'TransactionType': 'Send Money', 'TransID': 'MAI19S', 'TransAmount': 6000.00, 'TransReceiver': 'Family Support', 'TransSender': 'Monthly Allowance'},
        {'TransactionStartDate': '2023-01-03 16:15:50', 'TransactionEndDate': '2023-01-03 16:15:53', 'TransactionType': 'Send Money', 'TransID': 'MBJ20T', 'TransAmount': 2500.00, 'TransReceiver': 'Contractor Payment', 'TransSender': 'Project Payment'},
        
        # Withdrawal transactions - Cash out from agents
        {'TransactionStartDate': '2023-01-01 18:45:20', 'TransactionEndDate': '2023-01-01 18:45:23', 'TransactionType': 'Withdrawal', 'TransID': 'WAF21U', 'TransAmount': 4000.00, 'TransReceiver': 'T-Money Agent - Kamukunji', 'TransSender': 'Vincent Kiprotich'},
        {'TransactionStartDate': '2023-01-02 09:30:50', 'TransactionEndDate': '2023-01-02 09:30:53', 'TransactionType': 'Withdrawal', 'TransID': 'WBG22V', 'TransAmount': 2000.00, 'TransReceiver': 'Safaricom Shop - Thika Road', 'TransSender': 'Cynthia Chepkemoi'},
        {'TransactionStartDate': '2023-01-02 10:15:15', 'TransactionEndDate': '2023-01-02 10:15:18', 'TransactionType': 'Withdrawal', 'TransID': 'WCH23W', 'TransAmount': 1500.00, 'TransReceiver': 'Family Bank Agent', 'TransSender': 'Brian Kipkemboi'},
        {'TransactionStartDate': '2023-01-02 11:55:40', 'TransactionEndDate': '2023-01-02 11:55:43', 'TransactionType': 'Withdrawal', 'TransID': 'WDI24X', 'TransAmount': 3500.00, 'TransReceiver': 'Co-op Bank Agent', 'TransSender': 'Priscilla Akinyi'},
        {'TransactionStartDate': '2023-01-02 12:40:25', 'TransactionEndDate': '2023-01-02 12:40:28', 'TransactionType': 'Withdrawal', 'TransID': 'WEJ25Y', 'TransAmount': 10000.00, 'TransReceiver': 'Equity Bank Agent', 'TransSender': 'Sammy Kipchumba'},
        {'TransactionStartDate': '2023-01-03 08:25:10', 'TransactionEndDate': '2023-01-03 08:25:13', 'TransactionType': 'Withdrawal', 'TransID': 'WFJ26Z', 'TransAmount': 15000.00, 'TransReceiver': 'MPesa Agent - Junction', 'TransSender': 'Jane Wambui Kerubo'},
        {'TransactionStartDate': '2023-01-03 09:40:15', 'TransactionEndDate': '2023-01-03 09:40:18', 'TransactionType': 'Withdrawal', 'TransID': 'WGK27AA', 'TransAmount': 5000.00, 'TransReceiver': 'Equity Bank - Airport', 'TransSender': 'Thomas Kiprono'},
        {'TransactionStartDate': '2023-01-03 10:25:20', 'TransactionEndDate': '2023-01-03 10:25:23', 'TransactionType': 'Withdrawal', 'TransID': 'WHL28BB', 'TransAmount': 15000.00, 'TransReceiver': 'Family Bank - CBD', 'TransSender': 'Joyce Jepkemoi'},
        {'TransactionStartDate': '2023-01-03 11:10:30', 'TransactionEndDate': '2023-01-03 11:10:33', 'TransactionType': 'Withdrawal', 'TransID': 'WIM29CC', 'TransAmount': 30000.00, 'TransReceiver': 'Co-op Bank - Westlands', 'TransSender': 'Andrew Kipngeno'},
        {'TransactionStartDate': '2023-01-04 12:55:40', 'TransactionEndDate': '2023-01-04 12:55:43', 'TransactionType': 'Withdrawal', 'TransID': 'WJN30DD', 'TransAmount': 7000.00, 'TransReceiver': 'Safaricom Shop - South B', 'TransSender': 'Winfred Chepkemoi'},
        
        # Deposit transactions - Cash in to M-Pesa
        {'TransactionStartDate': '2023-01-01 09:05:10', 'TransactionEndDate': '2023-01-01 09:05:13', 'TransactionType': 'Deposit', 'TransID': 'DAP31EE', 'TransAmount': 5000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Joseph Karanja'},
        {'TransactionStartDate': '2023-01-01 10:25:35', 'TransactionEndDate': '2023-01-01 10:25:38', 'TransactionType': 'Deposit', 'TransID': 'DBQ32FF', 'TransAmount': 12000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Rose Chebet'},
        {'TransactionStartDate': '2023-01-01 11:40:50', 'TransactionEndDate': '2023-01-01 11:40:53', 'TransactionType': 'Deposit', 'TransID': 'DCR33GG', 'TransAmount': 8000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Isaac Kipkosgei'},
        {'TransactionStartDate': '2023-01-02 13:15:20', 'TransactionEndDate': '2023-01-02 13:15:23', 'TransactionType': 'Deposit', 'TransID': 'DDS34HH', 'TransAmount': 25000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Dorothy Jelagat'},
        {'TransactionStartDate': '2023-01-02 14:50:45', 'TransactionEndDate': '2023-01-02 14:50:48', 'TransactionType': 'Deposit', 'TransID': 'EDT35II', 'TransAmount': 15000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Paul Kipyego'},
        {'TransactionStartDate': '2023-01-02 08:45:15', 'TransactionEndDate': '2023-01-02 08:45:18', 'TransactionType': 'Deposit', 'TransID': 'DEU36JJ', 'TransAmount': 50000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Business Revenue'},
        {'TransactionStartDate': '2023-01-03 10:35:25', 'TransactionEndDate': '2023-01-03 10:35:28', 'TransactionType': 'Deposit', 'TransID': 'DFV37KK', 'TransAmount': 100000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Sales Income'},
        {'TransactionStartDate': '2023-01-03 11:50:35', 'TransactionEndDate': '2023-01-03 11:50:38', 'TransactionType': 'Deposit', 'TransID': 'DGW38LL', 'TransAmount': 35000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Freelance Payment'},
        {'TransactionStartDate': '2023-01-03 13:25:45', 'TransactionEndDate': '2023-01-03 13:25:48', 'TransactionType': 'Deposit', 'TransID': 'DHX39MM', 'TransAmount': 75000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Contract Payment'},
        {'TransactionStartDate': '2023-01-04 14:40:55', 'TransactionEndDate': '2023-01-04 14:40:58', 'TransactionType': 'Deposit', 'TransID': 'DIY40NN', 'TransAmount': 125000.00, 'TransReceiver': 'M-Pesa', 'TransSender': 'Investment Return'},
        
        # Airtime Purchase - Mobile top-ups
        {'TransactionStartDate': '2023-01-01 08:15:30', 'TransactionEndDate': '2023-01-01 08:15:33', 'TransactionType': 'Airtime Purchase', 'TransID': 'APP41OO', 'TransAmount': 200.00, 'TransReceiver': 'Safaricom Ltd', 'TransSender': 'Lucy Chepkorir'},
        {'TransactionStartDate': '2023-01-01 09:45:15', 'TransactionEndDate': '2023-01-01 09:45:18', 'TransactionType': 'Airtime Purchase', 'TransID': 'AQQ42PP', 'TransAmount': 100.00, 'TransReceiver': 'Safaricom Ltd', 'TransSender': 'Kennedy Kiplangat'},
        {'TransactionStartDate': '2023-01-02 12:10:40', 'TransactionEndDate': '2023-01-02 12:10:43', 'TransactionType': 'Airtime Purchase', 'TransID': 'ARR43QQ', 'TransAmount': 300.00, 'TransReceiver': 'Safaricom Ltd', 'TransSender': 'Nancy Chepkemoi'},
        {'TransactionStartDate': '2023-01-02 15:25:20', 'TransactionEndDate': '2023-01-02 15:25:23', 'TransactionType': 'Airtime Purchase', 'TransID': 'ASR44RR', 'TransAmount': 500.00, 'TransReceiver': 'Airtel Kenya', 'TransSender': 'Collins Kipchirchir'},
        {'TransactionStartDate': '2023-01-02 16:55:10', 'TransactionEndDate': '2023-01-02 16:55:13', 'TransactionType': 'Airtime Purchase', 'TransID': 'ATS45SS', 'TransAmount': 150.00, 'TransReceiver': 'Telkom Kenya', 'TransSender': 'Hannah Jeptoo'},
        {'TransactionStartDate': '2023-01-03 09:30:25', 'TransactionEndDate': '2023-01-03 09:30:28', 'TransactionType': 'Airtime Purchase', 'TransID': 'AUU46TT', 'TransAmount': 2000.00, 'TransReceiver': 'Safaricom Ltd', 'TransSender': 'Data Bundle Purchase'},
        {'TransactionStartDate': '2023-01-03 11:45:40', 'TransactionEndDate': '2023-01-03 11:45:43', 'TransactionType': 'Airtime Purchase', 'TransID': 'AVV47UU', 'TransAmount': 1000.00, 'TransReceiver': 'Airtel Kenya', 'TransSender': 'Data Bundle'},
        {'TransactionStartDate': '2023-01-03 14:20:15', 'TransactionEndDate': '2023-01-03 14:20:18', 'TransactionType': 'Airtime Purchase', 'TransID': 'AWW48VV', 'TransAmount': 500.00, 'TransReceiver': 'Telkom Kenya', 'TransSender': 'Internet Bundle'},
        {'TransactionStartDate': '2023-01-04 16:35:30', 'TransactionEndDate': '2023-01-04 16:35:33', 'TransactionType': 'Airtime Purchase', 'TransID': 'AXX49WW', 'TransAmount': 1500.00, 'TransReceiver': 'Safaricom Ltd', 'TransSender': 'Business Communication'},
        {'TransactionStartDate': '2023-01-04 17:50:45', 'TransactionEndDate': '2023-01-04 17:50:48', 'TransactionType': 'Airtime Purchase', 'TransID': 'AYY50XX', 'TransAmount': 300.00, 'TransReceiver': 'Airtel Kenya', 'TransSender': 'Personal Use'}
    ]
    
    # Generate more realistic data by creating additional variations
    base_date = datetime(2023, 1, 5)  # Continue from where the predefined data ends
    for i in range(50):  # Generate 50 more transactions to make a substantial dataset
        # Realistic transaction times (most transactions happen during business hours)
        hour = random.choice([8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19])  # Business hours
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        start_time = base_date + timedelta(days=i//10, hours=hour, minutes=minute, seconds=second)  # Vary dates as well
        duration = random.randint(1, 15)  # Most M-Pesa transactions are quick
        end_time = start_time + timedelta(seconds=duration)
        
        # Realistic amounts based on M-Pesa usage in Kenya
        realistic_amounts = [50, 100, 200, 300, 500, 800, 1000, 1200, 1500, 2000, 2500, 3000, 4000, 5000,
                             6000, 8000, 10000, 15000, 20000, 25000, 30000, 50000, 100000]
        
        transaction_type = random.choice(['Pay Bill', 'Send Money', 'Withdrawal', 'Deposit', 'Airtime Purchase'])
        
        # Realistic receivers based on transaction type
        if transaction_type == 'Pay Bill':
            receivers = [
                'KPLC-Kenya Power', 'Safaricom PLC', 'Daima Sacco', 'Kra Services', 'Chania River Water',
                'NHIF Kenya', 'University of Nairobi', 'Co-op Bank Loan', 'KRA PAYMENTS', 'Safaricom Postpaid'
            ]
        elif transaction_type == 'Send Money':
            receivers = [
                'John Omondi', 'Daniel Njoroge', 'Mike Waweru', 'Faith Mutua', 'Joseph Muthomi',
                'Business Partner', 'School Fees Payment', 'KCB Bank', 'Family Support', 'Contractor Payment'
            ]
        elif transaction_type == 'Withdrawal':
            receivers = [
                'T-Money Agent - Kamukunji', 'Safaricom Shop - Thika Road', 'Family Bank Agent',
                'Co-op Bank Agent', 'Equity Bank Agent', 'MPesa Agent - Junction', 
                'Equity Bank - Airport', 'Family Bank - CBD', 'Co-op Bank - Westlands', 'Safaricom Shop - South B'
            ]
        elif transaction_type == 'Deposit':
            receivers = ['M-Pesa']  # All deposits go to M-Pesa
        else:  # Airtime Purchase
            receivers = ['Safaricom Ltd', 'Airtel Kenya', 'Telkom Kenya']
        
        # Realistic Kenyan names
        senders = [
            'Mercy Wanjiku Maina', 'James Odhiambo', 'Grace Njeri Mwangi', 'Samuel Otieno Kipchirchir',
            'Mary Wanjiku Kamau', 'Jane Wanjiku', 'Daniel Njoroge', 'Sarah Mumbi', 'Esther Wangari',
            'Peter Kimani', 'Vincent Kiprotich', 'Cynthia Chepkemoi', 'Brian Kipkemboi', 'Priscilla Akinyi',
            'Joseph Karanja', 'Rose Chebet', 'Isaac Kipkosgei', 'Dorothy Jelagat', 'Paul Kipyego',
            'Lucy Chepkorir', 'Kennedy Kiplangat', 'Nancy Chepkemoi', 'Collins Kipchirchir', 'Hannah Jeptoo',
            'Charles Kiprotich Koech', 'Ann Wanjiku Wambui', 'Francis Kipchumba', 'Beatrice Moraa',
            'Sammy Kipkemei Kiprono', 'David Kipchirchir Too', 'Florence Chepkorir Kandie', 'George Kipketer',
            'Rebecca Chebet', 'Moses Kipkosgei', 'Jane Wambui Kerubo', 'Thomas Kiprono', 'Joyce Jepkemoi',
            'Andrew Kipngeno', 'Winfred Chepkemoi', 'Business Revenue', 'Sales Income', 'Freelance Payment',
            'Contract Payment', 'Investment Return', 'Data Bundle Purchase', 'Data Bundle', 'Internet Bundle',
            'Business Communication', 'Personal Use'
        ]
        
        record = {
            'TransactionStartDate': start_time.strftime("%Y-%m-%d %H:%M:%S"),
            'TransactionEndDate': end_time.strftime("%Y-%m-%d %H:%M:%S"),
            'TransactionType': transaction_type,
            'TransID': f"L{random.choice('ABCDEF')}{''.join([str(random.randint(0, 9)) for _ in range(2)])}{random.choice('GHIJKL')}{i+51:02d}",
            'TransAmount': random.choice(realistic_amounts),
            'TransReceiver': random.choice(receivers),
            'TransSender': random.choice(senders)
        }
        realistic_transactions.append(record)
    
    df = pd.DataFrame(realistic_transactions)
    logger.info(f"Created realistic dataset with {len(df)} M-Pesa transactions")
    return df

def main():
    """
    Main function to demonstrate loading of realistic transaction data
    This simulates how real data would be loaded from production systems
    """
    print("Loading realistic M-Pesa transaction data...")
    print("=" * 50)
    
    # Load realistic transaction data
    transaction_data = load_realistic_transaction_data()
    
    print(f"Successfully loaded {len(transaction_data)} realistic transactions")
    print("\nFirst 10 records:")
    print(transaction_data.head(10))
    
    print(f"\nTransaction type distribution:")
    print(transaction_data['TransactionType'].value_counts())
    
    print(f"\nAmount summary statistics:")
    print(transaction_data['TransAmount'].describe())
    
    # Save to CSV for use with ETL pipeline (simulating how real data would be ingested)
    output_file = 'mpesa_sample.csv'
    transaction_data.to_csv(output_file, index=False)
    print(f"\nRealistic transaction data saved to {output_file}")
    
    print(f"\nIn a real-world scenario, this data would come from:")
    print("- Production M-Pesa transaction database")
    print("- Real-time transaction streams (Kafka, etc.)")
    print("- Cloud data lakes (AWS S3, Google Cloud Storage)")
    print("- API endpoints from mobile money providers")
    print("- Data warehouse extracts")

if __name__ == "__main__":
    main()