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

    for attempt in range(3):  # Retry up to 3 times
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                desc = data['messages'][0]['desc'] if data['messages'] else "No message available"
                status = data.get('status', '\x00')
                is_valid = status == '\x00'
                return is_valid, desc
            else:
                print(f"Attempt {attempt+1}: Request failed with status code {response.status_code}")
                if attempt < 2:
                    time.sleep(5)
                    continue
                return False, f"API Error: Status {response.status_code}"
        
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1}: Error occurred: {e}")
            if attempt < 2:
                time.sleep(5)
                continue
            return False, f"API Request Error: {str(e)}"

def get_next_unprocessed_record(conn, table_name):
    """Get the next unprocessed record with highest payment amount"""
    try:
        cursor = conn.cursor(dictionary=True)
        
        # Get one unprocessed record with highest payment amount
        cursor.execute(f"""
        SELECT id, `PAN Number`, `Amount of Payment` 
        FROM `{table_name}`
        WHERE pan_msg IS NULL
        ORDER BY `Amount of Payment` DESC
        LIMIT 1
        """)
        
        return cursor.fetchone()
    
    except Error as e:
        print(f"Error fetching next record: {e}")
        return None

def update_record(conn, table_name, record_id, is_valid, message):
    """Update the processed record in database"""
    try:
        cursor = conn.cursor()
        update_query = f"""
        UPDATE `{table_name}`
        SET pan_valid = %s,
            pan_msg = %s,
            last_checked = %s
        WHERE id = %s
        """
        cursor.execute(update_query, (
            int(is_valid) if is_valid is not None else None,
            message,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            record_id
        ))
        conn.commit()
        return True
    except Error as e:
        print(f"Error updating record {record_id}: {e}")
        conn.rollback()
        return False

def process_records_one_by_one(table_name):
    """Process records one by one with immediate updates in MySQL"""
    total_processed = 0
    total_errors = 0
    
    while True:
        conn = create_mysql_connection()
        if not conn:
            time.sleep(60)  # Wait before retrying connection
            continue
            
        try:
            # Start transaction
            conn.start_transaction()
            
            # Get next record to process
            record = get_next_unprocessed_record(conn, table_name)
            if not record:
                print("No more unprocessed records found")
                break
                
            pan_number = record['PAN Number']
            record_id = record['id']
            amount = record['Amount of Payment']
            clean_pan = str(pan_number).strip().upper() if pan_number else None
            
            print(f"\nProcessing record ID {record_id} (Amount: {amount})")
            
            if not clean_pan:
                # Mark empty PANs as invalid
                success = update_record(
                    conn, table_name, record_id, 
                    False, "Empty PAN number"
                )
                if success:
                    total_processed += 1
                continue
                
            # Make API request with rate limiting
            time.sleep(5)  # 10 second delay between requests
            pan_valid, pan_msg = make_api_request(clean_pan)
            
            # Update the current record
            success = update_record(
                conn, table_name, record_id, 
                pan_valid, pan_msg
            )
            
            if success:
                total_processed += 1
                print(f"Updated record ID {record_id}: {pan_msg}")
            else:
                total_errors += 1
                
        except Exception as e:
            total_errors += 1
            print(f"Error processing record: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
                
        # Progress update every 10 records
        if total_processed % 10 == 0:
            print(f"\nProgress: {total_processed} processed, {total_errors} errors")
    
    print(f"\nProcessing completed! Total processed: {total_processed}, Errors: {total_errors}")
    return total_errors == 0

if __name__ == "__main__":
    TABLE_NAME = "tds_data"  # Your table name
    
    print("Starting PAN verification process...")
    success = process_records_one_by_one(TABLE_NAME)
    
    if success:
        print("✅ PAN verification completed successfully!")
    else:
        print("⚠️ Processing completed with some errors")