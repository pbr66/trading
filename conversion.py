import pandas as pd
import os

# The massive price files we want to speed up (we leave news.xlsx alone!)
files_to_convert = ['nasdaq.xlsx', 'sp500.xlsx', 'dow.xlsx', 'dax.xlsx', 'ftse.xlsx']

for file in files_to_convert:
    if os.path.exists(file):
        print(f"⏳ Reading {file} (this might take a minute)...")
        
        # Read all sheets in the Excel file just like the dashboard does
        all_sheets = pd.read_excel(file, sheet_name=None)
        
        # Drop any totally empty sheets
        valid_sheets = [sheet for sheet in all_sheets.values() if not sheet.empty]
        
        if valid_sheets:
            # Stitch them together seamlessly
            combined_df = pd.concat(valid_sheets, ignore_index=True)
            
            # Swap the extension to .csv
            csv_filename = file.replace('.xlsx', '.csv')
            
            # Save as CSV. 'utf-8-sig' matches the exact encoding your friend's script looks for
            combined_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"✅ Successfully created {csv_filename}!")
        else:
            print(f"⚠️ {file} had no data.")
    else:
        print(f"❌ Could not find {file} in this folder.")

print("\n🎉 All done! You can now use the CSVs.")