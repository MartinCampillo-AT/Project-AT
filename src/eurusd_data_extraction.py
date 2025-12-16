import pandas as pd
import os
import glob

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
BASE_PATH = "/Users/martincampillopereda/Desktop/ProjectAT/data"
COL_NAMES = ['date', 'time', 'open', 'high', 'low', 'close', 'volume']

def load_data_smart(base_path):
    print(f"🔍 Searching for files in: {base_path} ...")
    
    # 1. RECURSIVE SEARCH (THE BLOODHOUND)
    # Search for any CSV starting with "DAT_MT_EURUSD_M1_" in any subfolder
    # ** means "any folder depth"
    search_pattern = os.path.join(base_path, "**", "DAT_MT_EURUSD_M1_*.csv")
    
    # recursive=True allows searching inside subfolders
    all_files = glob.glob(search_pattern, recursive=True)
    
    # Sort to load 2011, then 2012, etc.
    all_files.sort()
    
    if not all_files:
        print("❌ CRITICAL ERROR: No matching CSV files found.")
        print("   -> Check that the 'data' folder is not empty.")
        print("   -> Check that files start with 'DAT_MT_EURUSD_M1_'")
        return None

    print(f"✅ Found {len(all_files)} suitable files.")
    
    # Show first and last to verify
    print("   -> First:", os.path.basename(all_files[0]))
    print("   -> Last: ", os.path.basename(all_files[-1]))
    
    # 2. LOADING AND MERGING
    all_dataframes = []
    
    for file_path in all_files:
        filename = os.path.basename(file_path)
        print(f"   Reading: {filename} ...")
        
        try:
            # HistData uses commas or semicolons. Trying comma first.
            df_temp = pd.read_csv(file_path, names=COL_NAMES, header=None)
            
            # Quick check to ensure we are not reading garbage
            if len(df_temp) < 10:
                print(f"   ⚠️ Warning: {filename} seems empty or very small.")
            
            all_dataframes.append(df_temp)
            
        except Exception as e:
            print(f"   ❌ Error reading {filename}: {e}")

    # 3. FINAL CONCATENATION
    if not all_dataframes:
        return None

    print("\nMerging everything into a giant DataFrame...")
    df_total = pd.concat(all_dataframes, ignore_index=True)
    
    # 4. DATE CLEANING
    print("Converting dates (this will take about 30-60 seconds)...")
    
    # Speed trick: Convert only if they are strings
    df_total['timestamp'] = pd.to_datetime(
        df_total['date'] + ' ' + df_total['time'], 
        format='%Y.%m.%d %H:%M'
    )
    
    df_total.set_index('timestamp', inplace=True)
    df_total.drop(columns=['date', 'time'], inplace=True)
    df_total.sort_index(inplace=True)
    
    # Remove duplicates
    df_total = df_total[~df_total.index.duplicated(keep='first')]
    
    print(f"✅ Success! Total minutes loaded: {len(df_total)}")
    return df_total

def create_resamples(df_m1):
    print("\n--- Creating Timeframes (Resampling) ---")
    
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    print("Generating 5 Minutes...")
    df_5m = df_m1.resample('5min').agg(agg_dict).dropna()
    
    print("Generating 1 Hour...")
    df_1h = df_m1.resample('1h').agg(agg_dict).dropna()
    
    print("Generating Daily...")
    df_1d = df_m1.resample('1D').agg(agg_dict).dropna()
    
    return df_5m, df_1h, df_1d

# --- EXECUTION ---
df_master_m1 = load_data_smart(BASE_PATH)

if df_master_m1 is not None:
    df_5m, df_1h, df_1d = create_resamples(df_master_m1)
    
    # Save
    print("\nSaving backup copies (.pkl)...")
    df_5m.to_pickle("EURUSD_5M.pkl")
    df_1h.to_pickle("EURUSD_1H.pkl")
    df_1d.to_pickle("EURUSD_1D.pkl")
    print("✅ All done. Files saved.")
    
    print("\nSample (1H):")
    print(df_1h.tail())