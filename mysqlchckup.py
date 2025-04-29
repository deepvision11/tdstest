import mysql.connector
import requests
import time
from tqdm import tqdm
from datetime import datetime
from mysql.connector import Error

def create_mysql_connection():
    """Create a connection to MySQL database"""
    try:
        conn = mysql.connector.connect(
            host='103.250.149.152',
            user='vision',
            password='Vision@123',
            database='tds'
        )
        print("Connected to MySQL database")
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
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
            return False, f"API Error: Status"
    
    except requests.exceptions.Timeout:
        print("Request timed out. Retrying...")
        time.sleep(5)
        return make_api_request(pan_number)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return False, f"API Request Error: {str(e)}"

def process_records_by_pan(mysql_table, limit=1000, offset=0):
    """Process records using PAN Number as key with immediate updates in MySQL"""
    conn = create_mysql_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Select PAN Numbers to process
        cursor.execute(f"""
        SELECT DISTINCT `PAN Number` FROM `{mysql_table}`
        WHERE  pan_msg IS NULL
        ORDER BY `PAN Number` DESC
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
            time.sleep(10)  # 10 second delay
            pan_valid, pan_msg = make_api_request(clean_pan)
            print(pan_msg, clean_pan)
            
            # Update all records with this PAN number
            update_query = f"""
            UPDATE `{mysql_table}`
            SET pan_valid = %s,
                pan_msg = %s,
                last_checked = %s
            WHERE `PAN Number` = %s
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
    TABLE_NAME = "tds_data"  # <-- Change to your table name here!
    
    # Configuration
    PROCESS_LIMIT = 1000    # How many records you want to process in one run
    PROCESS_OFFSET = 0      # From where you want to start
    
    success = process_records_by_pan(TABLE_NAME, PROCESS_LIMIT, PROCESS_OFFSET)
    
    if success:
        print("✅ PAN verification completed successfully!")
    else:
        print("⚠️ Processing completed with some errors")
