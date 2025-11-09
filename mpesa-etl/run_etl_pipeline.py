"""
Complete M-Pesa ETL pipeline execution script
This script runs the entire ETL process and analysis in one go
"""
import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, description):
    """
    Run a shell command with a description
    
    Args:
        cmd (str): Command to execute
        description (str): Description of what the command does
    """
    print(f"\n{description}")
    print(f"Running: {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✓ Successfully completed: {description}")
        if result.stdout.strip():
            print(f"Output: {result.stdout}")
    else:
        print(f"✗ Failed to execute: {description}")
        print(f"Error: {result.stderr}")
        return False
    
    return True

def main():
    print("M-PESA ETL PIPELINE - COMPLETE EXECUTION")
    print("="*50)
    
    # Verify directory and files
    if not os.path.exists("mpesa_sample.csv"):
        print("Error: mpesa_sample.csv not found in current directory")
        return
    
    print("✓ Found mpesa_sample.csv")
    
    # Install dependencies
    if not run_command("pip install -r requirements.txt", "Installing required dependencies"):
        return
    
    # Run the basic ETL pipeline
    if not run_command("python mpesa_etl.py", "Running basic ETL pipeline"):
        return
    
    # Run the advanced ETL pipeline
    if not run_command("python advanced_mpesa_etl.py", "Running advanced ETL pipeline"):
        return
    
    # Run the analysis
    if not run_command("python analyze_transformed_data.py", "Running analysis on transformed data"):
        return
    
    # List output files
    print("\nGenerated files:")
    print("- transformed_data/transformed_mpesa_data.csv (transformed data)")
    print("- mpesa_analysis_visualization.png (data visualization)")
    print("- mpesa_analysis_report.md (analysis report)")
    
    print("\n" + "="*50)
    print("M-Pesa ETL Pipeline execution completed successfully!")
    print("Check the transformed_data/ directory for processed data.")
    print("Check the root directory for analysis visualization and report.")

if __name__ == "__main__":
    main()