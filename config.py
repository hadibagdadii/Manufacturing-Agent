"""
Configuration for Manufacturing Data Extraction Agent
Edit these settings before running
"""

# Path to your data (CHANGE THIS)
Q_DRIVE_PATH = "Q:/OPS/Production/Testdata"  # Production path
# Q_DRIVE_PATH = "./test_data"  # Test

# Database settings
DATABASE_PATH = "manufacturing_data.db"

# Ollama settings
OLLAMA_BASE_URL = "http://localhost:11434"
MODEL_NAME = "llama3.1:8b"

# Processing settings
MAX_FILES_PER_RUN = 1000  # Production limit (increase if needed)
BATCH_SIZE = 100

# Logging
LOG_LEVEL = "INFO"