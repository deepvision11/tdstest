import pandas as pd
import sqlite3
import mysql.connector
from mysql.connector import Error
from tqdm import tqdm
from datetime import datetime

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

def clean_data(value):
    """Clean and convert data values"""
    if pd.isna(value) or value == '' or str(value).lower() == 'nan':
        return None
    return value

def normalize_column_name(name):
    """Normalize column names: remove unwanted characters"""
    return ' '.join(name.split()).replace('\n', ' ').strip()

def transfer_to_mysql(sqlite_db, sqlite_table, mysql_table):
    """Transfer data from SQLite to MySQL with proper schema"""
    # Connect to SQLite
    sqlite_conn = sqlite3.connect(sqlite_db)
    
    # Read data
    query = f"SELECT * FROM {sqlite_table}"
    sqlite_df = pd.read_sql(query, sqlite_conn)
    
    # Normalize column names
    sqlite_df.columns = [normalize_column_name(col) for col in sqlite_df.columns]
    
    # Clean data
    sqlite_df = sqlite_df.applymap(clean_data)
    
    # Connect to MySQL
    mysql_conn = create_mysql_connection()
    if mysql_conn:
        try:
            cursor = mysql_conn.cursor()
            
            # Create MySQL table
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{mysql_table}` (
                `Challan Serial No` INT,
                `Section Code` TEXT,
                `Deductee Code` TEXT,
                `PAN Number` VARCHAR(10),
                `Name of Deductee` TEXT,
                `Amount of Payment` DECIMAL(15,2),
                `Date on which Amount paid / credited` DATE,
                `Rate at which Tax deducted` DECIMAL(5,2),
                `Amount of Tax deducted` DECIMAL(15,2),
                `pan_valid` INTEGER,
                `pan_msg` TEXT,
                `last_checked` DATETIME,
                INDEX `pan_index` (`PAN Number`),
                INDEX `challan_index` (`Challan Serial No`)
            )
            """)
            
            # Prepare insert query
            insert_query = f"""
            INSERT INTO `{mysql_table}` 
            (`Challan Serial No`, `Section Code`, `Deductee Code`, `PAN Number`,
             `Name of Deductee`, `Amount of Payment`, `Date on which Amount paid / credited`,
             `Rate at which Tax deducted`, `Amount of Tax deducted`,
             `pan_valid`, `pan_msg`, `last_checked`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Prepare data
            data_to_insert = []
            for _, row in sqlite_df.iterrows():
                data_row = (
                    clean_data(row.get('Challan Serial No.', None)),
                    clean_data(row.get('Section Code', None)),
                    clean_data(row.get('Deductee Code', None)),
                    clean_data(row.get('PAN Number', None)),
                    clean_data(row.get('Name of Deductee', None)),
                    clean_data(row.get('Amount of Payment', None)),
                    pd.to_datetime(row.get('Date on which Amount paid / credited')) if pd.notna(row.get('Date on which Amount paid / credited')) else None,
                    clean_data(row.get('Rate at which Tax deducted', None)),
                    clean_data(row.get('Amount of Tax deducted', None)),
                    clean_data(row.get('pan_valid', None)),
                    clean_data(row.get('pan_msg', None)),
                    pd.to_datetime(row.get('last_checked')) if pd.notna(row.get('last_checked')) else None
                )
                data_to_insert.append(data_row)
            
            # Insert in batches
            batch_size = 100
            for i in tqdm(range(0, len(data_to_insert), batch_size), desc="Transferring to MySQL"):
                batch = data_to_insert[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                mysql_conn.commit()
            
            print(f"✅ Successfully transferred {len(sqlite_df)} records to MySQL")
            
        except Error as e:
            print(f"❌ Error during MySQL operation: {e}")
            mysql_conn.rollback()
        finally:
            mysql_conn.close()
    
    sqlite_conn.close()

if __name__ == "__main__":
    # Configuration
    SQLITE_DB = "mydatabase.db"
    SQLITE_TABLE = "excel_data"
    MYSQL_TABLE = "tds_data"
    
    transfer_to_mysql(SQLITE_DB, SQLITE_TABLE, MYSQL_TABLE)
