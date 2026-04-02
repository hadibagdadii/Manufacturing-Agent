"""
Main execution script for Manufacturing Data Extraction Agent
"""

import sys
import requests
import logging
from agent_tools import SafeTools
from agent_core import ManufacturingDataAgent
from config import Q_DRIVE_PATH, DATABASE_PATH, MODEL_NAME, OLLAMA_BASE_URL

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_ollama():
    """Verify Ollama is running"""
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=2)
        if response.status_code == 200:
            data = response.json()
            models = data.get('models', [])
            model_names = [m.get('name', '') for m in models]
            
            print("[OK] Ollama is running")
            print(f"  Available models: {', '.join(model_names)}")
            
            # Check if the configured model is available
            from config import MODEL_NAME
            model_base = MODEL_NAME.split(':')[0]  # Extract base name (e.g., 'qwen2.5' from 'qwen2.5:7b')
            has_model = any(model_base in m for m in model_names)
            if not has_model:
                print(f"\n[WARNING] {MODEL_NAME} not found")
                print(f"   Install with: ollama pull {MODEL_NAME}")
                return False
            
            return True
        else:
            print("[ERROR] Ollama is not responding correctly")
            return False
    except requests.exceptions.RequestException:
        print("[ERROR] Ollama is not running")
        print("   Start it with: ollama serve")
        print("   Or download from: https://ollama.com")
        return False


def main():
    print("\n")
    print("="*70)
    print("    Lightweight Agentic Manufacturing Data Extractor")
    print("              100% Local - Ollama Powered")
    print("="*70)
    print()
    
    # Check Ollama
    if not check_ollama():
        sys.exit(1)
    
    print(f"\nConfiguration:")
    print(f"  Data path: {Q_DRIVE_PATH}")
    print(f"  Database: {DATABASE_PATH}")
    print(f"  Model: {MODEL_NAME}")
    
    # Initialize tools
    print("\n[*] Initializing tools...")
    try:
        tools = SafeTools(
            root_path=Q_DRIVE_PATH,
            db_path=DATABASE_PATH
        )
    except Exception as e:
        print(f"[ERROR] Error initializing tools: {e}")
        print(f"   Make sure the path exists: {Q_DRIVE_PATH}")
        sys.exit(1)
    
    # Create agent
    print("[*] Creating agent...")
    try:
        agent = ManufacturingDataAgent(
            tools=tools,
            model=MODEL_NAME,
            base_url=OLLAMA_BASE_URL
        )
    except Exception as e:
        print(f"[ERROR] Error creating agent: {e}")
        sys.exit(1)
    
    # Define task
    task = """
    Extract all manufacturing component data from Excel files.
    For each file:
    1. Identify the sheet with component data (usually "all data")
    2. Determine which columns contain component names and serial numbers
    3. Extract all components with their serial numbers
    4. Save to database
    
    Handle errors gracefully and continue processing other files.
    Adapt to different column names and file structures.
    """
    
    # Run agent
    print("\n" + "="*70)
    print("Starting extraction...")
    print("="*70)
    
    try:
        final_state = agent.run(task)
    except KeyboardInterrupt:
        print("\n\n[WARNING] Interrupted by user")
        stats = tools.get_statistics()
        print(f"\nPartial results saved:")
        print(f"  Products: {stats['products']}")
        print(f"  Component Serials: {stats['component_serials']}")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        tools.close()
    
    print(f"\n[OK] Extraction complete!")
    print(f"   Database saved to: {DATABASE_PATH}")
    print(f"\nNext steps:")
    print(f"  1. Query your data: python query_db.py")
    print(f"  2. Check for errors in the database extraction_log table")


if __name__ == "__main__":
    main()