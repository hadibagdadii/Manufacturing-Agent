"""
Safe Tools for Manufacturing Data Extraction Agent
Controlled, read-only file access with database writing
"""

from typing import List, Dict, Optional
import pandas as pd
from pathlib import Path
import sqlite3
from datetime import datetime
import logging
from config import MAX_FILES_PER_RUN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SafeTools:
    """Controlled tool set with read-only file access"""
    
    def __init__(self, root_path: str, db_path: str):
        self.root_path = Path(root_path)
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.setup_database()
    
    def setup_database(self):
        """Create database schema for manufacturing test data"""
        cursor = self.conn.cursor()
        
        # Main products table with test data fields
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_name TEXT,
                model_name TEXT,
                product_serial TEXT UNIQUE,
                part_number TEXT,
                customer_part_number TEXT,
                description TEXT,
                test_date TEXT,
                test_operator TEXT,
                assembler TEXT,
                bench_serial TEXT,
                file_path TEXT,
                last_updated TIMESTAMP,
                extraction_status TEXT
            )
        ''')
        
        # Component serial numbers found in the file
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS component_serials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_serial TEXT,
                column_name TEXT,
                serial_value TEXT,
                FOREIGN KEY (product_serial) REFERENCES products(product_serial)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extraction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT,
                status TEXT,
                message TEXT,
                timestamp TIMESTAMP
            )
        ''')
        
        # Create indexes for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_product_serial ON products(product_serial)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_component_product ON component_serials(product_serial)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_component_serial ON component_serials(serial_value)')
        
        self.conn.commit()
        logger.info("✓ Database initialized")
    
    def list_excel_files(self, max_files: int = MAX_FILES_PER_RUN) -> List[Dict]:
        """
        Tool: List Excel files in directory structure
        ONLY processes 1.0_Lam folder and files starting with 'TP'
        """
        if max_files is None:
            max_files = MAX_FILES_PER_RUN

        files = []
        count = 0
        
        if not self.root_path.exists():
            logger.error(f"Root path does not exist: {self.root_path}")
            return []
        
        # ONLY process 1.0_Lam folder
        target_client = '1.0_Lam'
        client_path = self.root_path / target_client
        
        if not client_path.exists():
            logger.error(f"Target folder does not exist: {client_path}")
            return []
        
        logger.info(f"Scanning client folder: {target_client}")
        
        # Traverse: Model -> Serial -> Excel
        for model_folder in client_path.iterdir():
            if not model_folder.is_dir():
                continue
            
            for serial_folder in model_folder.iterdir():
                if not serial_folder.is_dir():
                    continue
                
                # Find Excel files starting with "TP"
                excel_files = []
                for pattern in ['TP*.xls', 'TP*.xlsx']:
                    excel_files.extend(list(serial_folder.glob(pattern)))
                
                for excel_file in excel_files:
                    files.append({
                        'path': str(excel_file),
                        'client': target_client,
                        'model': model_folder.name,
                        'serial': serial_folder.name,
                        'filename': excel_file.name
                    })
                    count += 1
                    if count >= 10:  # HARD LIMIT FOR TESTING
                        logger.info(f"✓ Reached limit of 10 files")
                        return files
        
        if count >= 10:
            logger.info(f"Reached test limit: returning first 10 files")
        else:
            logger.info(f"Found {len(files)} TP*.xlsx files")
        return files
    
    def read_excel_structure(self, file_path: str, sheet_name: Optional[str] = None) -> Dict:
        """
        Tool: Read Excel file and return structure info
        Looks for 'all_data' sheet (case-insensitive)
        """
        try:
            xl_file = pd.ExcelFile(file_path)
            
            # If no sheet specified, try to find "all_data" sheet (case-insensitive)
            if not sheet_name:
                for name in xl_file.sheet_names:
                    if 'all' in name.lower() and 'data' in name.lower():
                        sheet_name = name
                        logger.info(f"   Found data sheet: {name}")
                        break
            
            # If still no sheet found
            if not sheet_name:
                return {
                    'status': 'no_all_data_sheet',
                    'available_sheets': xl_file.sheet_names,
                    'message': 'No sheet containing "all data" found'
                }
            
            # Read the sheet
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Convert sample data to strings to handle datetime objects
            sample_df = df.head(5).copy()
            
            # Convert all values to strings
            for col in sample_df.columns:
                sample_df[col] = sample_df[col].astype(str)
            
            return {
                'status': 'success',
                'sheet_name': sheet_name,
                'columns': df.columns.tolist(),
                'sample_data': sample_df.to_dict('records'),
                'row_count': len(df)
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)[:200]
            }
    
    def extract_manufacturing_data(
        self, 
        file_path: str, 
        extraction_plan: Dict
    ) -> Dict:
        """
        Tool: Extract manufacturing test data based on LLM's extraction plan
        
        Args:
            file_path: Path to Excel file
            extraction_plan: {
                'sheet_name': 'All_Data',
                'part_number_column': 'P/N',
                'serial_number_column': 'Serial Number',
                'description_column': 'Description',
                'customer_part_number_column': 'Customer P/N',
                'test_date_column': 'Test Date',
                'test_operator_column': 'Test Operator',
                'assembler_column': 'Assembler',
                'bench_serial_column': 'Bench S/N',
                'other_serial_columns': ['Component S/N', 'Module SN']
            }
        """
        try:
            sheet_name = extraction_plan.get('sheet_name')
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Extract main fields (usually from first row or header area)
            result = {
                'part_number': '',
                'serial_number': '',
                'description': '',
                'customer_part_number': '',
                'test_date': '',
                'test_operator': '',
                'assembler': '',
                'bench_serial': '',
                'other_serials': []
            }
            
            # Helper function to get value from column
            def get_value(column_name):
                if not column_name or column_name not in df.columns:
                    return ''
                # Try first few rows
                for idx in range(min(10, len(df))):
                    val = df[column_name].iloc[idx]
                    if pd.notna(val) and str(val).strip() and str(val) not in ['nan', 'None', '']:
                        return str(val).strip()
                return ''
            
            # Extract each field
            result['part_number'] = get_value(extraction_plan.get('part_number_column'))
            result['serial_number'] = get_value(extraction_plan.get('serial_number_column'))
            result['description'] = get_value(extraction_plan.get('description_column'))
            result['customer_part_number'] = get_value(extraction_plan.get('customer_part_number_column'))
            result['test_date'] = get_value(extraction_plan.get('test_date_column'))
            result['test_operator'] = get_value(extraction_plan.get('test_operator_column'))
            result['assembler'] = get_value(extraction_plan.get('assembler_column'))
            result['bench_serial'] = get_value(extraction_plan.get('bench_serial_column'))
            
            # Extract other serial numbers
            other_serial_cols = extraction_plan.get('other_serial_columns', [])
            for col in other_serial_cols:
                val = get_value(col)
                if val:
                    result['other_serials'].append({
                        'column': col,
                        'value': val
                    })
            
            return {
                'status': 'success',
                'data': result
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)[:200]
            }
    
    def save_to_database(
        self, 
        client: str, 
        model: str, 
        serial: str,
        file_path: str, 
        data: Dict
    ) -> Dict:
        """
        Tool: Save extracted manufacturing data to database
        """
        try:
            cursor = self.conn.cursor()
            
            # Insert/update product with all fields
            cursor.execute('''
                INSERT OR REPLACE INTO products 
                (client_name, model_name, product_serial, part_number, 
                 customer_part_number, description, test_date, test_operator,
                 assembler, bench_serial, file_path, last_updated, extraction_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                client, model, serial,
                data.get('part_number', ''),
                data.get('customer_part_number', ''),
                data.get('description', ''),
                data.get('test_date', ''),
                data.get('test_operator', ''),
                data.get('assembler', ''),
                data.get('bench_serial', ''),
                file_path,
                datetime.now(),
                'success'
            ))
            
            # Delete old component serials
            cursor.execute(
                'DELETE FROM component_serials WHERE product_serial = ?', 
                (serial,)
            )
            
            # Insert component serials
            for item in data.get('other_serials', []):
                cursor.execute('''
                    INSERT INTO component_serials 
                    (product_serial, column_name, serial_value)
                    VALUES (?, ?, ?)
                ''', (serial, item['column'], item['value']))
            
            # Log success
            cursor.execute('''
                INSERT INTO extraction_log (file_path, status, message, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (file_path, 'success', f'Extracted manufacturing data', datetime.now()))
            
            self.conn.commit()
            
            return {
                'status': 'success',
                'message': f'Saved manufacturing data for {serial}'
            }
            
        except Exception as e:
            self.conn.rollback()
            
            # Log error
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO extraction_log (file_path, status, message, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (file_path, 'error', str(e)[:200], datetime.now()))
            self.conn.commit()
            
            return {
                'status': 'error',
                'error': str(e)[:200]
            }
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        cursor = self.conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM products')
        product_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM component_serials')  # CHANGED
        component_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT client_name) FROM products')
        client_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT status, COUNT(*) FROM extraction_log GROUP BY status')
        log_stats = dict(cursor.fetchall())
        
        return {
            'products': product_count,
            'component_serials': component_count,  # CHANGED
            'clients': client_count,
            'extraction_log': log_stats
        }
    
    def get_tool_descriptions(self) -> str:
        """Return tool descriptions for LLM to understand"""
        return """
Available Tools:

1. list_excel_files(max_files=100)
   Returns list of Excel files with metadata (client, model, serial, path)

2. read_excel_structure(file_path, sheet_name=None)
   Reads Excel file and returns column names and sample data
   Use this to understand the structure before extraction
   If sheet_name not provided, looks for "all data" sheet

3. extract_components_from_dataframe(file_path, extraction_plan)
   Extracts components based on your analysis
   extraction_plan should specify:
   - sheet_name: which sheet to read
   - component_name_column: column with component names
   - component_serial_column: column with serial numbers
   - component_type_column: column with type (optional)

4. save_to_database(client, model, serial, file_path, components)
   Saves extracted components to database

5. get_statistics()
   Returns current database statistics
        """
    
    def close(self):
        """Close database connection"""
        self.conn.close()