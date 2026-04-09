"""
Safe Tools for Manufacturing Data Extraction Agent
Controlled, read-only file access with database writing
"""

from typing import List, Dict, Optional
import pandas as pd
from pathlib import Path
import sqlite3
import os
import re
import fnmatch
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
        logger.info("[OK] Database initialized")
    
    def is_already_processed(self, file_path: str) -> bool:
        """Check if a file was already successfully extracted, or permanently failed, in a previous run"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM extraction_log WHERE file_path = ? AND status IN ('success', 'skipped', 'not_found')",
            (file_path,)
        )
        return cursor.fetchone()[0] > 0

    def mark_not_found(self, file_path: str):
        """Permanently record a file as not found so it is not retried on future runs"""
        cursor = self.conn.cursor()
        # Avoid duplicate not_found entries for the same path
        cursor.execute(
            "SELECT COUNT(*) FROM extraction_log WHERE file_path = ? AND status = 'not_found'",
            (file_path,)
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                'INSERT INTO extraction_log (file_path, status, message, timestamp) VALUES (?, ?, ?, ?)',
                (file_path, 'not_found', 'File not accessible at scan time', datetime.now())
            )
            self.conn.commit()

    def list_excel_files(self, max_files: int = None) -> List[Dict]:
        """
        Tool: List Excel files in directory structure
        RECURSIVE search for ALL TP*.xlsx files at any depth
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
        
        logger.info(f"Scanning client folder: {target_client} (recursive)")

        dir_count = 0
        patterns = ['TP*.xlsx', 'TP*.xls']
        seen_paths = set()  # Deduplicate by resolved absolute path

        for dirpath, dirnames, filenames in os.walk(str(client_path)):
            dir_count += 1
            if dir_count % 50 == 0:
                logger.info(f"   Scanning... {dir_count} folders visited, {count} files found so far")

            for filename in filenames:
                if not any(fnmatch.fnmatch(filename, p) for p in patterns):
                    continue

                excel_file = Path(dirpath) / filename

                # Get relative path from client folder
                try:
                    relative_path = excel_file.relative_to(client_path)
                except ValueError:
                    continue

                path_parts = relative_path.parts

                if len(path_parts) >= 2:
                    model_name = path_parts[0]
                    serial_name = path_parts[-2]
                else:
                    model_name = "Unknown"
                    serial_name = excel_file.parent.name

                # Skip template/blank files early — no point sending to LLM
                lower_name = filename.lower()
                if '_temp' in lower_name or lower_name.endswith('_temp.xlsx') or lower_name.endswith('_temp.xls'):
                    logger.debug(f"   [SKIP] Template file ignored: {filename}")
                    continue

                # Skip malformed filenames (e.g. TP-10003939-14-17_.xlsx — no serial after underscore)
                if not re.search(r'TP.+_\w+', filename):
                    logger.debug(f"   [SKIP] Malformed filename ignored: {filename}")
                    continue

                # Verify the file is actually accessible right now
                if not excel_file.exists():
                    logger.debug(f"   [GHOST] Indexed but not accessible, skipping: {excel_file}")
                    continue

                # Deduplicate by resolved absolute path
                resolved = str(excel_file.resolve())
                if resolved in seen_paths:
                    logger.debug(f"   [DUP] Duplicate path skipped: {filename}")
                    continue
                seen_paths.add(resolved)

                files.append({
                    'path': str(excel_file),
                    'client': target_client,
                    'model': model_name,
                    'serial': serial_name,
                    'filename': filename
                })
                count += 1

                if count >= max_files:
                    logger.info(f"[OK] Reached file limit: {max_files}")
                    return files
        
        logger.info(f"[OK] Found {len(files)} TP*.xlsx/xls files (recursive search)")
        return files
    
    def read_excel_structure(self, file_path: str, sheet_name: Optional[str] = None) -> Dict:
        """
        Tool: Read Excel file and return structure info
        Looks for 'all_data' sheet (case-insensitive)
        """
        try:
            if not os.path.exists(file_path):
                return {
                    'status': 'error',
                    'error': f'File not found (may have been moved or deleted): {file_path}'
                }

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
        """
        try:
            # Guard: verify file is still accessible before handing to pandas
            if not os.path.exists(file_path):
                return {
                    'status': 'error',
                    'error': f'File not found (may have been moved or deleted): {file_path}'
                }

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
                # DEFENSIVE: Handle lists (LLM might return a list instead of string)
                if isinstance(column_name, list):
                    # If it's a list, try each column until we find data
                    for col in column_name:
                        if isinstance(col, str) and col in df.columns:
                            for idx in range(min(10, len(df))):
                                val = df[col].iloc[idx]
                                if pd.notna(val) and str(val).strip() and str(val) not in ['nan', 'None', '']:
                                    return str(val).strip()
                    return ''  # No data found in any of the columns
                
                # Normal case: string column name
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
            
            # Extract other serial numbers - ULTRA DEFENSIVE
            try:
                other_serial_cols = extraction_plan.get('other_serial_columns', [])
                
                # Handle None
                if other_serial_cols is None:
                    other_serial_cols = []
                
                # Handle non-list (convert to list)
                if not isinstance(other_serial_cols, list):
                    other_serial_cols = [other_serial_cols]
                
                # Flatten nested lists recursively
                def flatten_list(nested_list):
                    flat = []
                    for item in nested_list:
                        if isinstance(item, list):
                            flat.extend(flatten_list(item))
                        elif isinstance(item, str):
                            flat.append(item)
                        # Skip anything that's not a string or list
                    return flat
                
                other_serial_cols = flatten_list(other_serial_cols)
                
                # Process each column
                for col in other_serial_cols:
                    if not isinstance(col, str):
                        continue  # Skip non-strings
                        
                    val = get_value(col)
                    if val:
                        # Make absolutely sure we're appending a dict
                        result['other_serials'].append({
                            'column': str(col),
                            'value': str(val)
                        })
                        
            except Exception as serial_error:
                # Log but don't fail the whole extraction
                logger.warning(f"   [WARNING] Error processing other_serial_columns: {serial_error}")
                result['other_serials'] = []
            
            return {
                'status': 'success',
                'data': result
            }
            
        except Exception as e:
            import traceback
            logger.error(f"Extraction error: {traceback.format_exc()}")
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
            # Skip blank/template records — serial '0', empty, or test date '00:00:00'
            serial_str = str(serial).strip()
            test_date_str = str(data.get('test_date', '')).strip()
            if serial_str in ('0', '', 'None', 'nan') or test_date_str == '00:00:00':
                logger.info(f"   [SKIP] Blank/template record skipped (serial={serial_str}, date={test_date_str})")
                cursor = self.conn.cursor()
                cursor.execute(
                    'INSERT INTO extraction_log (file_path, status, message, timestamp) VALUES (?, ?, ?, ?)',
                    (file_path, 'skipped', f'Blank/template record: serial={serial_str}', datetime.now())
                )
                self.conn.commit()
                return {'status': 'skipped', 'message': 'Blank/template record'}

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