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

def process_records_by_pan(db_path, table_name, limit=108357, offset=1999):
    """Process records using PAN Number as key with immediate updates"""
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
        
        # Get all PANs that need processing with OFFSET and LIMIT
        cursor.execute(f"""
        SELECT DISTINCT `PAN Number` FROM {table_name}
        WHERE pan_valid IS NULL OR pan_msg IS NULL
        ORDER BY `PAN Number` desc
        LIMIT {limit} OFFSET {offset}
        """)
        pan_numbers = [row[0] for row in cursor.fetchall()]
        
        if not pan_numbers:
            print("No unprocessed PAN numbers found")
            return True
        
        print(f"Found {len(pan_numbers)} PAN numbers to process")
        
        # Process each PAN with progress bar
        for pan_number in tqdm(pan_numbers, desc="Processing PAN numbers"):
            clean_pan = str(pan_number).strip().upper()
            
            # Skip empty PANs
            if not clean_pan:
                continue
                
            # Make API request with rate limiting
            time.sleep(10)  # 10 second delay between requests
            pan_valid, pan_msg = make_api_request(clean_pan)
            print(pan_msg, clean_pan)
            # Update all records with this PAN number
            update_query = f"""
            UPDATE {table_name}
            SET pan_valid = ?,
                pan_msg = ?,
                last_checked = ?
            WHERE `PAN Number` = ?
            """
            cursor.execute(update_query, (
                int(pan_valid) if pan_valid is not None else None,
                pan_msg,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                clean_pan
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
    
    # Configuration
    PROCESS_LIMIT = 108357  # Maximum number of PANs to process
    PROCESS_OFFSET = 1999   # Starting offset
    
    success = process_records_by_pan(DB_PATH, TABLE_NAME, PROCESS_LIMIT, PROCESS_OFFSET)
    
    if success:
        print("PAN verification completed successfully!")
    else:
        print("Processing completed with errors")