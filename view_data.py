"""
Quick database viewer to see extracted data
"""

import sqlite3
import pandas as pd
from config import DATABASE_PATH

def view_all_data():
    """Show all extracted data"""
    conn = sqlite3.connect(DATABASE_PATH)
    
    print("\n" + "="*80)
    print("PRODUCTS TABLE")
    print("="*80)
    
    df = pd.read_sql_query("SELECT * FROM products", conn)
    
    if df.empty:
        print("No data found!")
    else:
        print(f"\nTotal products: {len(df)}\n")
        
        # Show all columns for first 5 rows
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        
        print(df.head(10).to_string(index=False))
        
    print("\n" + "="*80)
    print("COMPONENT SERIALS TABLE")
    print("="*80)
    
    df_serials = pd.read_sql_query("SELECT * FROM component_serials", conn)
    
    if df_serials.empty:
        print("No component serials found!")
    else:
        print(f"\nTotal component serials: {len(df_serials)}\n")
        print(df_serials.head(20).to_string(index=False))
    
    print("\n" + "="*80)
    print("EXTRACTION LOG (Errors Only)")
    print("="*80)
    
    df_errors = pd.read_sql_query(
        "SELECT * FROM extraction_log WHERE status = 'error'", 
        conn
    )
    
    if df_errors.empty:
        print("✓ No errors logged!")
    else:
        print(f"\n{len(df_errors)} errors:\n")
        print(df_errors.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    view_all_data()