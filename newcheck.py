import pandas as pd
import sqlite3
import requests
import time
from tqdm import tqdm
from datetime import datetime

def connect_to_database(db_path):
    """Connect to SQLite database and return connection"""
    try:
        conn = sqlite3.connect(db_path)
        print(f"Connected to database at {db_path}")
        return conn
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

def make_api_request(pan_number):
    """Make API request to validate PAN with retry logic"""
    url = "https://eportal.incometax.gov.in/iec/registrationapi/saveEntity"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "sn": "checkPanDetailsService",
        "Content-Type": "application/json",
        "Priority": "u=0"
    }
    
    payload = {
        "serviceName": "checkPanDetailsService",
        "userId": pan_number
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            desc = data['messages'][0]['desc'] if data['messages'] else "No message available"
            status = data.get('status', '\x00')
            is_valid = status == '\x00'
            return is_valid, desc
        else:
            print(f"Request failed with status code {response.status_code}")
            return False, f"API Error: Status {response.status_code}"
    
    except requests.exceptions.Timeout:
        print("Request timed out. Retrying...")
        time.sleep(5)
        return make_api_request(pan_number)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return False, f"API Request Error: {str(e)}"

def process_all_records_one_by_one(db_path, table_name):
    """Process all records one by one with immediate updates"""
    conn = connect_to_database(db_path)
    if not conn:
        return False
    
    try:
        # Initialize columns if needed
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'pan_valid' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN pan_valid INTEGER")
        if 'pan_msg' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN pan_msg TEXT")
        if 'last_checked' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN last_checked TEXT")
        conn.commit()
        
        # Get all records that need processing
        cursor.execute(f"""
        SELECT rowid, * FROM {table_name} 
        WHERE pan_valid IS NULL OR pan_msg IS NULL
        ORDER BY rowid
        LIMIT 108357  OFFSET 1999 
        """)
        records = cursor.fetchall()
        
        if not records:
            print("No unprocessed records found")
            return True
        
        print(f"Found {len(records)} records to process")
        
        # Process each record with progress bar
        for record in tqdm(records, desc="Processing records"):
            rowid = record[0]
            pan_number = str(record[1]).strip().upper()  # Assuming PAN is first column
            
            # Make API request with rate limiting
            time.sleep(10)  # 10 second delay between requests
            pan_valid, pan_msg = make_api_request(pan_number)
            
            # Update the record immediately
            update_query = f"""
            UPDATE {table_name}
            SET pan_valid = ?,
                pan_msg = ?,
                last_checked = ?
            WHERE rowid = ?
            """
            cursor.execute(update_query, (
                int(pan_valid) if pan_valid is not None else None,
                pan_msg,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                rowid
            ))
            conn.commit()
            
        return True
    
    except Exception as e:
        print(f"Error during processing: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    DB_PATH = "mydatabase.db"
    TABLE_NAME = "excel_data"
    
    success = process_all_records_one_by_one(DB_PATH, TABLE_NAME)
    
    if success:
        print("All records processed successfully!")
    else:
        print("Processing completed with errors")