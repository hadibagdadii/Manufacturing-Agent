# Manufacturing Data Extraction Agent

Automated component data extraction from Excel files using local AI.

## Quick Start

1. Install Ollama: https://ollama.com/download
2. Download model: `ollama pull llama3.1:8b`
3. Install Python dependencies: `pip install -r requirements.txt`
4. Edit `config.py` to set your data path
5. Run: `python run_agent.py`
6. Query: `python query_db.py`

## Configuration

Edit `config.py`:
- `Q_DRIVE_PATH`: Path to your Excel files
- `DATABASE_PATH`: Where to store the database
- `MODEL_NAME`: AI model to use

## Testing

For testing, use the included test_data directory:
1. Edit `config.py` and uncomment: `Q_DRIVE_PATH = "./test_data"`
2. Create test Excel files in test_data structure
3. Run the agent

## Usage
```bash
# Run extraction
python run_agent.py

# Query database
python query_db.py

# Then use commands like:
Query> product SN001
Query> component COMP123
Query> clients
```

## Troubleshooting

**Ollama not running:**
```bash
ollama serve
```

**Model not found:**
```bash
ollama pull llama3.1:8b
```