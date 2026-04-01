"""
Manufacturing Data Extraction Agent
Uses LangGraph for agentic workflow orchestration
"""

from langgraph.graph import StateGraph, END
try:
    from langchain_ollama import OllamaLLM as Ollama
except ImportError:
    from langchain_community.llms import Ollama
from typing import TypedDict, Annotated, List, Dict
import operator
import json
import re
import logging

logger = logging.getLogger(__name__)


class AgentState(TypedDict):
    """State that gets passed between agent steps"""
    task: str
    files_to_process: List[dict]
    current_file: dict
    file_structure: dict
    extraction_plan: dict
    components: dict  # Changed from List to dict
    errors: List[str]  # Removed the operator.add to prevent memory accumulation
    processed_count: int
    success_count: int
    error_count: int  # Track errors separately
    status: str


class ManufacturingDataAgent:
    """Lightweight agentic system for data extraction"""
    
    def __init__(self, tools, model: str = "llama3.1:8b", base_url: str = "http://localhost:11434"):
        self.tools = tools
        self.llm = Ollama(model=model, base_url=base_url)
        self.graph = self.build_graph()
    
    def build_graph(self) -> StateGraph:
        """Build the agent's decision flow"""
        
        workflow = StateGraph(AgentState)
        
        # Define agent steps
        workflow.add_node("plan", self.plan_step)
        workflow.add_node("get_files", self.get_files_step)
        workflow.add_node("analyze_file", self.analyze_file_step)
        workflow.add_node("extract_data", self.extract_data_step)
        workflow.add_node("save_data", self.save_data_step)
        workflow.add_node("handle_error", self.handle_error_step)
        
        # Define flow
        workflow.set_entry_point("plan")
        
        workflow.add_edge("plan", "get_files")
        workflow.add_edge("get_files", "analyze_file")
        
        # After analyzing, decide if we can extract
        workflow.add_conditional_edges(
            "analyze_file",
            self.should_extract_or_error,
            {
                "extract": "extract_data",
                "error": "handle_error",
                "next_file": "analyze_file",
                "done": END
            }
        )
        
        workflow.add_edge("extract_data", "save_data")
        
        # After saving, process next file or finish
        workflow.add_conditional_edges(
            "save_data",
            self.should_continue,
            {
                "continue": "analyze_file",
                "done": END
            }
        )
        
        workflow.add_conditional_edges(
            "handle_error",
            self.should_continue,
            {
                "continue": "analyze_file",
                "done": END
            }
        )
        
        return workflow.compile()
    
    def plan_step(self, state: AgentState) -> AgentState:
        """Initial planning step - SIMPLIFIED (no LLM call needed)"""
        logger.info("📋 Plan: Extract manufacturing data from TP*.xlsx files in 1.0_Lam folder")
        state['status'] = 'planning_complete'
        return state
    
    def get_files_step(self, state: AgentState) -> AgentState:
        """Get list of files to process"""
        logger.info("📁 AGENT: Discovering Excel files...")
        
        files = self.tools.list_excel_files(max_files=100)
        logger.info(f"✓ Found {len(files)} files to process")
        
        state['files_to_process'] = files
        state['processed_count'] = 0
        state['success_count'] = 0
        state['errors'] = []
        
        return state
    
    def analyze_file_step(self, state: AgentState) -> AgentState:
        """Analyze current file structure"""
        
        # Get next file to process
        if state['processed_count'] >= len(state['files_to_process']):
            state['status'] = 'all_done'
            return state
        
        current_file = state['files_to_process'][state['processed_count']]
        state['current_file'] = current_file
        
        logger.info(f"🔍 [{state['processed_count']+1}/{len(state['files_to_process'])}] {current_file['filename']}")
        
        # Read file structure
        structure = self.tools.read_excel_structure(current_file['path'])
        state['file_structure'] = structure
        
        if structure['status'] == 'error':
            logger.warning(f"   ❌ Could not read file: {structure.get('error')}")
            state['status'] = 'file_error'
            return state
        
        if structure['status'] == 'no_all_data_sheet':
            logger.warning(f"   ⚠️  No 'all data' sheet. Available: {structure['available_sheets']}")
            # Try first sheet
            first_sheet = structure['available_sheets'][0] if structure['available_sheets'] else None
            if first_sheet:
                logger.info(f"   🔄 Trying sheet: {first_sheet}")
                structure = self.tools.read_excel_structure(current_file['path'], sheet_name=first_sheet)
                state['file_structure'] = structure
                if structure['status'] == 'error':
                    state['status'] = 'file_error'
                    return state
            else:
                state['status'] = 'file_error'
                return state
        
        # DEBUG: Log what we're sending to LLM
        logger.info(f"   DEBUG - Columns found: {structure['columns'][:10]}...")  # First 10 columns
        logger.info(f"   DEBUG - Sample row 0: {structure['sample_data'][0] if structure['sample_data'] else 'No data'}")
        
        # Create a simpler, more direct prompt
        columns_str = ", ".join(structure['columns'][:20])  # First 20 columns
        
        prompt = f"""You are analyzing an Excel spreadsheet. Here are the column names:

{columns_str}

Look at these exact column names and return a JSON object with the following fields.
Use the EXACT column name from the list above, or null if not found:

- part_number_column: Column containing part number (look for "Part Number")
- serial_number_column: Column containing serial number (look for "Serial Number")  
- description_column: Column containing description (look for "Description")
- customer_part_number_column: Column with customer part number (look for "Customer part number")
- test_date_column: Column with test date (look for "Test date")
- test_operator_column: Column with test operator (look for "Test Operator")
- assembler_column: Column with assembler name (look for "Assembler")
- bench_serial_column: Column with bench serial (look for "Bench" and "S/N")
- other_serial_columns: Array of column names that contain "S/N" or "Serial"

Return ONLY this JSON, no explanation:
{{
  "sheet_name": "{structure['sheet_name']}",
  "part_number_column": "Part Number",
  "serial_number_column": "Serial Number",
  "description_column": "Description",
  "customer_part_number_column": "Customer part number",
  "test_date_column": "Test date",
  "test_operator_column": "Test Operator",
  "assembler_column": null,
  "bench_serial_column": "Bench I S/N",
  "other_serial_columns": ["PCB - S/N"]
}}
"""
        
        try:
            plan_str = self.llm.invoke(prompt)
            
            # Extract JSON from response (in case LLM adds extra text)
            json_match = re.search(r'\{[^}]+\}', plan_str, re.DOTALL)
            if json_match:
                plan_str = json_match.group()
            
            # Clean up potential markdown formatting
            plan_str = plan_str.replace('```json', '').replace('```', '').strip()
            extraction_plan = json.loads(plan_str)
            
            state['extraction_plan'] = extraction_plan
            state['status'] = 'ready_to_extract'
            
            logger.info(f"   ✓ Extraction plan received:")
            logger.info(f"      Part Number: {extraction_plan.get('part_number_column')}")
            logger.info(f"      Serial Number: {extraction_plan.get('serial_number_column')}")
            logger.info(f"      Test Operator: {extraction_plan.get('test_operator_column')}")
            
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"   ❌ Failed to parse plan: {e}")
            logger.debug(f"      LLM response: {plan_str[:200]}")
            state['status'] = 'plan_error'
        
        return state
    
    def extract_data_step(self, state: AgentState) -> AgentState:
        """Extract components based on plan"""
        
        result = self.tools.extract_manufacturing_data(
            file_path=state['current_file']['path'],
            extraction_plan=state['extraction_plan']
        )
        
        if result['status'] == 'success':
            state['components'] = result['data']
            state['status'] = 'extraction_complete'
            logger.info(f"   ✓ Extracted manufacturing data")
        else:
            # Truncate error message to prevent MemoryError
            error_msg = str(result.get('error', 'Unknown error'))[:200]
            logger.error(f"   ❌ Extraction failed: {error_msg}")
            state['status'] = 'extraction_error'
            state['errors'].append(f"{state['current_file']['filename']}: {error_msg}")
        
        return state
    
    def save_data_step(self, state: AgentState) -> AgentState:
        """Save extracted data to database"""
        
        # Use the serial number extracted from the Excel file
        product_serial = state['components'].get('serial_number', state['current_file']['serial'])
        
        result = self.tools.save_to_database(
            client=state['current_file']['client'],
            model=state['current_file']['model'],
            serial=product_serial,  # Use extracted serial number
            file_path=state['current_file']['path'],
            data=state['components']
        )
        
        if result['status'] == 'success':
            logger.info(f"   💾 Saved!")
            state['processed_count'] += 1
            state['success_count'] += 1
            state['status'] = 'saved'
        else:
            logger.error(f"   ❌ Save failed: {result.get('error')}")
            state['errors'].append(f"{state['current_file']['serial']}: {result.get('error')}")
            state['processed_count'] += 1
        
        return state
    
    def handle_error_step(self, state: AgentState) -> AgentState:
        """Handle errors intelligently"""
        
        # Just log error, don't accumulate in state (prevents MemoryError)
        error_msg = f"{state['current_file']['filename']}: {state['status']}"
        logger.warning(f"   ⚠️ Skipping: {error_msg}")
        
        # Move to next file
        state['processed_count'] += 1
        state['error_count'] = state.get('error_count', 0) + 1
        
        return state
    
    def should_extract_or_error(self, state: AgentState) -> str:
        """Decide next step after analysis"""
        if state['status'] == 'all_done':
            return 'done'
        elif state['status'] == 'ready_to_extract':
            return 'extract'
        elif state['status'] in ['file_error', 'plan_error', 'extraction_error']:
            return 'error'
        else:
            return 'next_file'
    
    def should_continue(self, state: AgentState) -> str:
        """Decide if we should process more files"""
        if state['processed_count'] >= len(state['files_to_process']):
            return 'done'
        else:
            return 'continue'
    
    def run(self, task: str) -> AgentState:
        """Execute the agent"""
        
        initial_state = AgentState(
            task=task,
            files_to_process=[],
            current_file={},
            file_structure={},
            extraction_plan={},
            components={},
            errors=[],
            processed_count=0,
            success_count=0,
            error_count=0,
            status='starting'
        )
        
        print("\n" + "="*70)
        print("🦞 LIGHTWEIGHT AGENTIC DATA EXTRACTOR")
        print("="*70)
        
        try:
            final_state = self.graph.invoke(initial_state)
        except Exception as e:
            logger.error(f"Agent error: {e}", exc_info=True)
            raise
        
        print("\n" + "="*70)
        print("EXTRACTION COMPLETE")
        print("="*70)
        print(f"✓ Successfully processed: {final_state['success_count']} files")
        print(f"❌ Errors: {final_state.get('error_count', 0)} files")
        
        # Show statistics
        stats = self.tools.get_statistics()
        print(f"\nDatabase Statistics:")
        print(f"  Products: {stats['products']}")
        print(f"  Component Serials: {stats['component_serials']}")
        print(f"  Clients: {stats['clients']}")
        
        return final_state