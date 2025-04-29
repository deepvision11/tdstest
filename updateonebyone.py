import mysql.connector
import requests
import time
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
    """Get the next unprocessed record with highest total_tds"""
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"""
        SELECT pan, name, total_tds
        FROM `{table_name}`
        WHERE pan_msg IS NULL
        ORDER BY total_tds DESC
        LIMIT 1 OFFSET 0
        """)
        return cursor.fetchone()
    except Error as e:
        print(f"Error fetching next record: {e}")
        return None

def update_record(conn, table_name, pan, is_valid, message):
    """Update the processed record in database by PAN"""
    try:
        cursor = conn.cursor()
        update_query = f"""
        UPDATE `{table_name}`
        SET pan_valid = %s,
            pan_msg = %s,
            last_checked = %s
        WHERE pan = %s
        """
        cursor.execute(update_query, (
            int(is_valid) if is_valid is not None else None,
            message,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            pan
        ))
        conn.commit()
        return True
    except Error as e:
        print(f"Error updating record for PAN {pan}: {e}")
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
            conn.start_transaction()

            record = get_next_unprocessed_record(conn, table_name)
            if not record:
                print("No more unprocessed records found")
                break

            pan_number = record['pan']
            name = record['name']
            amount = record['total_tds']
            clean_pan = str(pan_number).strip().upper() if pan_number else None

            print(f"\nProcessing PAN: {clean_pan} | Name: {name} | TDS: {amount}")

            if not clean_pan:
                success = update_record(conn, table_name, pan_number, False, "Empty PAN number")
                if success:
                    total_processed += 1
                continue

            # Rate limit
            time.sleep(5)

            pan_valid, pan_msg = make_api_request(clean_pan)

            success = update_record(conn, table_name, pan_number, pan_valid, pan_msg)

            if success:
                total_processed += 1
                print(f"Updated PAN {clean_pan}: {pan_msg}")
            else:
                total_errors += 1

        except Exception as e:
            total_errors += 1
            print(f"Error processing PAN: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

        if total_processed % 10 == 0:
            print(f"\nProgress: {total_processed} processed, {total_errors} errors")

    print(f"\nProcessing completed! Total processed: {total_processed}, Errors: {total_errors}")
    return total_errors == 0

if __name__ == "__main__":
    TABLE_NAME = "tds_data_haanaa"

    print("Starting PAN verification process...")
    success = process_records_one_by_one(TABLE_NAME)

    if success:
        print("✅ PAN verification completed successfully!")
    else:
        print("⚠️ Processing completed with some errors")
