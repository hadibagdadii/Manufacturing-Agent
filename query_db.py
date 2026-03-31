"""
Database Query Interface for Manufacturing Data
"""

import sqlite3
import pandas as pd
from config import DATABASE_PATH


class ManufacturingDB:
    """Easy-to-use interface for querying manufacturing data"""
    
    def __init__(self, db_path: str = DATABASE_PATH):
        self.conn = sqlite3.connect(db_path)
        print(f"✓ Connected to database: {db_path}\n")
        self.print_stats()
    
    def print_stats(self):
        """Print database statistics"""
        cursor = self.conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM products')
        products = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM components')
        components = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT client_name) FROM products')
        clients = cursor.fetchone()[0]
        
        print(f"Database Stats:")
        print(f"  • {products} products")
        print(f"  • {components} components")
        print(f"  • {clients} clients")
        print()
    
    def find_product(self, serial: str):
        """Find a product by serial number"""
        query = '''
            SELECT 
                p.client_name as Client,
                p.model_name as Model,
                p.product_serial as "Product Serial",
                c.component_name as "Component Name",
                c.component_serial as "Component Serial",
                c.component_type as Type
            FROM products p
            LEFT JOIN components c ON p.product_serial = c.product_serial
            WHERE p.product_serial LIKE ?
            ORDER BY c.component_name
        '''
        df = pd.read_sql_query(query, self.conn, params=(f'%{serial}%',))
        
        if df.empty:
            print(f"❌ No product found with serial containing '{serial}'")
        else:
            print(f"✓ Found product: {df['Product Serial'].iloc[0]}")
            print(f"\nClient: {df['Client'].iloc[0]}")
            print(f"Model: {df['Model'].iloc[0]}")
            print(f"\nComponents ({len(df)} total):")
            print(df[['Component Name', 'Component Serial', 'Type']].to_string(index=False))
        
        return df
    
    def find_component(self, component_serial: str):
        """Find which products use a specific component"""
        query = '''
            SELECT 
                p.client_name as Client,
                p.model_name as Model,
                p.product_serial as "Product Serial",
                c.component_name as "Component Name",
                c.component_serial as "Component Serial"
            FROM components c
            JOIN products p ON c.product_serial = p.product_serial
            WHERE c.component_serial LIKE ?
        '''
        df = pd.read_sql_query(query, self.conn, params=(f'%{component_serial}%',))
        
        if df.empty:
            print(f"❌ No component found with serial containing '{component_serial}'")
        else:
            print(f"✓ Found {len(df)} product(s) using this component:")
            print(df.to_string(index=False))
        
        return df
    
    def list_clients(self):
        """List all clients"""
        query = 'SELECT DISTINCT client_name FROM products ORDER BY client_name'
        df = pd.read_sql_query(query, self.conn)
        print("Clients in database:")
        for client in df['client_name']:
            print(f"  • {client}")
        return df
    
    def get_errors(self):
        """Show all extraction errors"""
        query = '''
            SELECT 
                file_path as "File Path",
                message as "Error",
                timestamp as "When"
            FROM extraction_log 
            WHERE status = 'error'
            ORDER BY timestamp DESC
        '''
        df = pd.read_sql_query(query, self.conn)
        
        if df.empty:
            print("✓ No errors logged!")
        else:
            print(f"⚠️  {len(df)} error(s) logged:")
            print(df.to_string(index=False))
        
        return df
    
    def close(self):
        """Close database connection"""
        self.conn.close()


def interactive_mode():
    """Interactive query interface"""
    db = ManufacturingDB()
    
    print("="*70)
    print("INTERACTIVE MODE - Type 'help' for commands, 'quit' to exit")
    print("="*70)
    print()
    
    while True:
        try:
            cmd = input("\nQuery> ").strip()
            
            if not cmd:
                continue
            
            if cmd.lower() in ['quit', 'exit', 'q']:
                print("Goodbye!")
                break
            
            if cmd.lower() == 'help':
                print("""
Available commands:
  product <serial>       - Find product by serial number
  component <serial>     - Find products using a component
  clients                - List all clients
  errors                 - Show extraction errors
  help                   - Show this help
  quit                   - Exit
                """)
                continue
            
            parts = cmd.split(maxsplit=1)
            command = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else None
            
            if command == 'product' and arg:
                db.find_product(arg)
            elif command == 'component' and arg:
                db.find_component(arg)
            elif command == 'clients':
                db.list_clients()
            elif command == 'errors':
                db.get_errors()
            else:
                print("❌ Unknown command. Type 'help' for usage.")
        
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
    
    db.close()


if __name__ == "__main__":
    interactive_mode()