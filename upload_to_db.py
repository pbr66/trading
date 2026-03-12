import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
# Map your local filenames to the exact TiDB table names
# You can change '.xlsx' to '.csv' if you download your weekly data as CSVs
FILE_TO_TABLE_MAP = {
    'nasdaq.xlsx': 'nasdaq', 
    'sp500.xlsx': 'sp500',
    'dow.xlsx': 'dow', 
    'dax.xlsx': 'dax', 
    'ftse.xlsx': 'ftse'
}

# --- 2. DATABASE CONNECTION ---
print("🔌 Connecting to TiDB...")
load_dotenv()
TIDB_URL = os.getenv("TIDB_URL")

if not TIDB_URL:
    print("❌ Error: TIDB_URL not found. Make sure your .env file is in the same folder.")
    exit(1)

# Create engine with SSL required by TiDB Serverless
engine = create_engine(
    TIDB_URL,
    connect_args={"ssl": {"fake_flag_to_enable_tls": True}}
)

# --- 3. UPLOAD FUNCTION ---
def upload_file_to_table(file_name, table_name):
    if not os.path.exists(file_name):
        print(f"⚠️  Skipping {table_name}: '{file_name}' not found in local folder.")
        return

    print(f"⏳ Reading local file '{file_name}'...")
    try:
        # Support both CSV and Excel
        if file_name.lower().endswith('.csv'):
            df = pd.read_csv(file_name, sep=None, engine='python', encoding='utf-8-sig')
        else:
            df = pd.read_excel(file_name)

        if df.empty:
            print(f"⚠️  Skipping {table_name}: File is empty.")
            return

        # Standardize column names (lowercase, strip whitespace)
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        df.columns = [str(c).lower().strip() for c in df.columns]

        # Log rows to be appended
        rows_to_upload = len(df)
        print(f"✅ Found {rows_to_upload} rows for {table_name}. Appending to database...")

        # Push to TiDB in chunks to avoid packet limits
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',   # <--- CRITICAL: Appends instead of overwriting
            index=False,
            chunksize=5000,       # Uploads 5,000 rows at a time safely
            method='multi'
        )
        print(f"🚀 SUCCESS: Appended {rows_to_upload} rows to '{table_name}'.\n")

    except Exception as e:
        print(f"❌ ERROR uploading {file_name} to {table_name}: {e}\n")

# --- 4. EXECUTION ---
if __name__ == "__main__":
    print("\n" + "="*50)
    print("📈 STARTING WEEKLY DATA UPLOAD")
    print("="*50 + "\n")
    
    # Optional safety prompt
    confirm = input("⚠️  Are your local files filtered to ONLY contain new data for this week? (y/n): ")
    if confirm.lower() != 'y':
        print("Upload cancelled. Please filter your local files to avoid uploading duplicate rows.")
        exit(0)
        
    print("\nStarting upload process...\n")
    for file_name, table_name in FILE_TO_TABLE_MAP.items():
        upload_file_to_table(file_name, table_name)
        
    print("🎉 ALL FINISHED! You can now check your Streamlit Dashboard.")