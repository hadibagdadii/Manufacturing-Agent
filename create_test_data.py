import pandas as pd

# Create test data
data = {
    'Component Name': ['PCB Board', 'Power Supply', 'Fan Assembly'],
    'Serial Number': ['PCB-001', 'PS-002', 'FAN-003'],
    'Type': ['PCB', 'Power', 'Cooling']
}

df = pd.DataFrame(data)

# Save to test directory
df.to_excel('test_data/TestClient/TestModel/SN001/test_data.xlsx', 
            sheet_name='all data', index=False)

print("✓ Test Excel file created at: test_data/TestClient/TestModel/SN001/test_data.xlsx")