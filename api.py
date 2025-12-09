from fastapi import FastAPI, Query, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
import pandas as pd 
import os
import time
import threading
import shutil # Used for efficient file saving
from typing import Optional, List # Added List for checks
from C_data_processing import DataExplorer
from io import BytesIO # Needed to save Excel in memory before returning
import json # <-- ADDED

# --- Data/Project Specific Imports ---
# import pathlib
# from constants import DATA_PATH 
# from data_processing import DataExplorer # Assuming this is imported

# --- QC Specific Imports (MODIFIED: duration_check removed) ---
from qc_checks import (
    # ... (Your original QC imports) ...
    detect_period_from_rosco,
    load_bsr,
    period_check,
    completeness_check,
    overlap_duplicate_daybreak_check,
    program_category_check,
    check_event_matchday_competition,
    market_channel_consistency_check,
    domestic_market_check,
    rates_and_ratings_check,
    duplicated_market_check,
    country_channel_id_check,
    client_lstv_ott_check,
    color_excel,
    generate_summary_sheet,
    # market_specific_check_processor,
)

# --- F1 Imports (UNTOUCHED) ---
from C_data_processing_f1 import ( 
    BSRValidator, 
    # Note: These functions might conflict, so we'll call them by their module
    # color_excel,
    # generate_summary_sheet,
)

from C_data_processing_EPL import EPLValidator

# --- NEW QC IMPORTS (YOURS - ADDED) ---
# We import your file with an alias 'qc_general' to prevent name conflicts
import qc_checks_1 as qc_general

# -------------------- ⚙️ Folder setup (UNTOUCHED) --------------------
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------- 🧹 Cleanup Functions (UNTOUCHED) --------------------
def cleanup_old_files(folder_path, max_age_minutes=30):
    """Deletes files older than max_age_minutes."""
    now = time.time()
    max_age_seconds = max_age_minutes * 60

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            file_age = now - os.path.getmtime(file_path)
            if file_age > max_age_seconds:
                try:
                    os.remove(file_path)
                    print(f"🧹 Deleted old file: {file_path}")
                except Exception as e:
                    print(f"⚠️ Error deleting {file_path}: {e}")

def start_background_cleanup():
    """Starts a background thread that cleans up old files every 5 minutes."""
    def run_cleanup():
        while True:
            cleanup_old_files(UPLOAD_FOLDER, max_age_minutes=30)
            cleanup_old_files(OUTPUT_FOLDER, max_age_minutes=30)
            time.sleep(300)

    thread = threading.Thread(target=run_cleanup, daemon=True)
    thread.start()
# -----------------------------------------------------------

# Start the cleanup thread
start_background_cleanup()

# -------------------- 🧠 FastAPI Setup and Lifespan (UNTOUCHED) --------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # This is your existing lifespan logic, ensuring the Laligadata is loaded
    try:
        # app.state.df = pd.read_csv(DATA_PATH / "Sales.csv" , index_col=0 , parse_dates= True)
        app.state.df = pd.DataFrame() # Placeholder if Sales.csv isn't available
    except Exception as e:
        print(f"Warning: Could not load laliga.csv during startup: {e}")
        app.state.df = pd.DataFrame() # Ensure state exists
        
    yield
    # Cleanup state
    del app.state.df

app = FastAPI(lifespan=lifespan)

# --- NEW HELPER FUNCTION (ADDED) ---
def load_config():
    """Helper function to load the config.json file for your checks."""
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="config.json not found on server.")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="config.json is not valid JSON.")

# -------------------- 📂 Original API Endpoints (UNTOUCHED) --------------------

@app.post("/api/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    """
    Handles CSV file upload from the frontend and saves it to the data directory.
    """
    file_location = os.path.join(UPLOAD_FOLDER, file.filename) 
    
    try:
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        app.state.df = pd.read_csv(file_location, index_col=0, parse_dates=True)

        return {"filename": file.filename, "detail": f"File successfully uploaded and saved to {file_location}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during file upload: {e}")
    finally:
        await file.close() # This endpoint can remain async, it's fine

# --------------------  End Points Using DataExplorer Class  --------------------

@app.get("/api/summary")
async def read_summary_data():
    if app.state.df.empty:
        raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
    data = DataExplorer(app.state.df)
    return data.summary().json_response()

@app.get("/api/kpis")
async def read_kpis(country: str = Query(None)):
    if app.state.df.empty:
        raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
    data = DataExplorer(app.state.df)
    return data.kpis(country)

@app.get("/api/")
async def read_sales(limit: int = Query(100, gt=0, lt=150000)):
    if app.state.df.empty:
        raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
    data = DataExplorer(app.state.df, limit)
    return data.json_response()

# --------------------  QC API Endpoint (MODIFIED: duration_check removed) --------------------

@app.post("/api/run_qc")
def run_qc_checks(  # <-- CHANGED from async def to def
    rosco_file: UploadFile = File(..., description="The Rosco file (.xlsx)"),
    bsr_file: UploadFile = File(..., description="The BSR file (.xlsx)"),
    data_file: Optional[UploadFile] = File(None, description="The optional Client Data file (.xlsx)")
):
    """
    Runs the full QC pipeline on the uploaded Rosco, BSR, and optional Data files 
    and returns the processed Excel file.
    """
    
    # Define paths for uploaded files
    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
    data_path = None

    try:
        # 1. Save uploaded files to disk (for path-based QC functions)
        with open(rosco_path, "wb") as buffer:
            shutil.copyfileobj(rosco_file.file, buffer) # <-- Sync file save
        with open(bsr_path, "wb") as buffer:
            shutil.copyfileobj(bsr_file.file, buffer) # <-- Sync file save
        
        df_data = None
        if data_file and data_file.filename:
            data_path = os.path.join(UPLOAD_FOLDER, data_file.filename)
            with open(data_path, "wb") as buffer:
                shutil.copyfileobj(data_file.file, buffer) # <-- Sync file save
            df_data = pd.read_excel(data_path) 

        # 2. Run QC Pipeline (This is all blocking, now runs in a thread)
        start_date, end_date = detect_period_from_rosco(rosco_path)
        df = load_bsr(bsr_path) 

        df = period_check(df, start_date, end_date)
        df = completeness_check(df)
        df = overlap_duplicate_daybreak_check(df)
        df = program_category_check(df)
        df = check_event_matchday_competition(df, df_data=df_data, rosco_path=rosco_path)
        df = market_channel_consistency_check(df, reference_df=df_data)
        df = domestic_market_check(df, reference_df=df_data)
        df = rates_and_ratings_check(df)
        df = duplicated_market_check(df)
        df = country_channel_id_check(df)
        df = client_lstv_ott_check(df)

        # 3. Generate Output File on Disk (in OUTPUT_FOLDER)
        output_file = f"QC_Result_{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        df.to_excel(output_path, index=False)
        color_excel(output_path, df)
        generate_summary_sheet(output_path, df)

        # 4. Return FileResponse
        return FileResponse(
            path=output_path,
            filename=output_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        print(f"QC Error: {e}")
        for path in [rosco_path, bsr_path, data_path]:
            if path and os.path.exists(path):
                os.remove(path)
                
        raise HTTPException(status_code=500, detail=f"An error occurred during QC processing: {str(e)}")
    finally:
        # Removed all 'await file.close()' calls
        pass


# -------------------- 🌍 F1 MARKET CHECK ENDPOINT (MODIFIED FOR CONCURRENCY) --------------------

EPL_CHECK_KEYS  =     {
    "impute_lt_live_status",
    "consolidate_gillete_soccer",
    "check_sky_showcase_live",
    "standardize_uk_ire_region",
    "check_fixture_vs_case",
    "check_pan_balkans_serbia_parity",
    "audit_multi_match_status",
    "check_date_time_format_integrity",
    "check_live_broadcast_uniqueness",
    "audit_channel_line_item_count",
    "check_combined_archive_status",
    "suppress_duplicated_audience",
    "harmonize_uk_ire_program_descriptions_strict",
    "check_game_of_the_day_match",
    "check_non_metered_primary_market_audience",
    "check_legacy_mapping",
    "check_premier_league_october_obligation"
    } 

@app.post("/api/market_check_and_process", response_model=None)
def market_check_and_process( 
    bsr_file: UploadFile = File(..., description="BSR file for market-specific checks"),
    obligation_file: Optional[UploadFile] = File(None, description="F1 Obligation file for broadcaster checks"), 
    overnight_file: Optional[UploadFile] = File(None, description="Overnight Audience file for upscale/integrity check"),
    macro_file: Optional[UploadFile] = File(None, description="Macro BSA Market Duplicator file"),
    checks: List[str] = Form(..., description="List of selected check keys (e.g., 'remove_andorra')"),
    check_configs: str = Form("{}", description="JSON string of runtime configurations")
):
    bsr_file_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
    obligation_path, overnight_path, macro_path = None, None, None
    
    output_filename = f"Processed_BSR_{os.path.splitext(bsr_file.filename)[0]}_{int(time.time())}.xlsx"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)

    secondary_reports: Dict[str, pd.DataFrame] = {} 
    status_summaries = []
    df_processed = None 
    
    try:
        # 1. Parse Configuration
        try:
            config_dict = json.loads(check_configs)
        except json.JSONDecodeError:
            print("Warning: Failed to parse check_configs JSON. Using empty config.")
            config_dict = {}
            
        # 1. Save all files synchronously
        # Save BSR file
        with open(bsr_file_path, "wb") as buffer:
            shutil.copyfileobj(bsr_file.file, buffer)
            
        # Save optional files
        if obligation_file and obligation_file.filename:
            obligation_path = os.path.join(UPLOAD_FOLDER, obligation_file.filename)
            with open(obligation_path, "wb") as buffer:
                shutil.copyfileobj(obligation_file.file, buffer)
        if overnight_file and overnight_file.filename: 
            overnight_path = os.path.join(UPLOAD_FOLDER, overnight_file.filename)
            with open(overnight_path, "wb") as buffer:
                shutil.copyfileobj(overnight_file.file, buffer)
        if macro_file and macro_file.filename: 
            macro_path = os.path.join(UPLOAD_FOLDER, macro_file.filename)
            with open(macro_path, "wb") as buffer:
                shutil.copyfileobj(macro_file.file, buffer)


        # 2. Split Checks and Initialize Validators
        bsr_checks_to_run = [c for c in checks if c not in EPL_CHECK_KEYS]
        epl_checks_to_run = [c for c in checks if c in EPL_CHECK_KEYS]

        # 💡 Independent Initialization: Both validators load the raw file
        shared_kwargs = {
            'bsr_path': bsr_file_path, 
            'obligation_path': obligation_path, 
            'overnight_path': overnight_path, 
            'macro_path': macro_path
        }
        
        bsr_validator = BSRValidator(**shared_kwargs)
        epl_validator = EPLValidator(**shared_kwargs)

        # --- Run BSR/F1 Checks ---
        if bsr_checks_to_run:
            print(f"Running BSR checks: {bsr_checks_to_run}")
            status_summaries.extend(bsr_validator.market_check_processor(bsr_checks_to_run))
            df_processed = bsr_validator.df # Capture results
        
        # --- Run EPL Checks ---
        if epl_checks_to_run:
            print(f"Running EPL checks: {epl_checks_to_run}")
            # If BSR checks ran first, we must apply those changes to the EPL validator's DF 
            # so EPL checks run on the BSR-processed data.
            if bsr_checks_to_run and df_processed is not None:
                epl_validator.df = df_processed
                
            # Run EPL checks
            # Assuming EPLValidator's checks are modifying its internal self.df
            epl_summaries = [epl_validator.market_check_map[c]() for c in epl_checks_to_run if c in epl_validator.market_check_map]
            status_summaries.extend(epl_summaries)
            
            # The final processed DF is the one held by the EPL validator
            df_processed = epl_validator.df 

        # --- Determine the final DataFrame if no checks ran ---
        if df_processed is None:
            # If neither set of checks ran, use the original BSR validator's loaded DataFrame
            df_processed = bsr_validator.df 
        
        if df_processed.empty:
            raise Exception("Processed DataFrame is empty after applying checks.")

        # 3. Finalize and Save (Simplified report extraction)
        clean_summaries = [s for s in status_summaries if isinstance(s, dict)]
        
        # --- 4. MULTI-SHEET EXCEL WRITER ---
        # NOTE: Using 'openpyxl' engine which is suitable for Streamlit/FastAPI integration
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: Main Processed BSR Data (Mandatory)
            df_processed.to_excel(writer, sheet_name='Processed BSR', index=False)
            
            # Placeholder for saving secondary reports if any were generated (unchanged logic)
            # for sheet_name, report_df in secondary_reports.items():
            #     report_df.to_excel(writer, sheet_name= sheet_name, index=False) 


        # 5. Finalize JSON Response
        download_url = f"/api/download_file?filename={output_filename}" 

        return JSONResponse(content={
            "status": "Success",
            "message": f"Successfully applied {len(checks)} market checks. Processed file is ready for download.",
            "download_url": download_url,
            "summaries": clean_summaries
        })

    except Exception as e:
        print(f"Market Check Error: {e}")
        # Ensure temporary files are cleaned up even if an error occurs
        raise HTTPException(status_code=500, detail=f"An error occurred during market checks: {str(e)}")
        
    finally:
        # Cleanup uploaded files
        for path in [bsr_file_path, obligation_path, overnight_path, macro_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError as e:
                    print(f"Error cleaning up file {path}: {e}")

# -------------------- 📥 NEW DOWNLOAD ENDPOINT (UNTOUCHED) --------------------
@app.get("/api/download_file")
async def download_file(filename: str = Query(...)):
    """Retrieves a previously generated file from the output folder."""
    file_path = os.path.join(OUTPUT_FOLDER, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found or link has expired.")
        
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -----------------------------------------------------------
# -------------------- YOUR NEW ENDPOINTS (UPDATED) --------------------
# -----------------------------------------------------------

# -------------------- 1. UPDATED GENERAL QC ENDPOINT --------------------
@app.post("/api/run_general_qc")
def run_general_qc_checks( # <-- CHANGED from async def to def, now supports macro_file optional
    rosco_file: UploadFile = File(...),
    bsr_file: UploadFile = File(...),
    macro_file: Optional[UploadFile] = File(None)
):
    """
    Runs YOUR 9-check GENERAL QC pipeline from qc_checks_1.py
    This endpoint mirrors the same ordering & logic you implemented in app.py
    """
    config = load_config()
    col_map = config["column_mappings"]
    rules = config["qc_rules"]
    project = config.get("project_rules", {})
    file_rules = config["file_rules"]

    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
    macro_path = None
    
    try:
        # Save files synchronously
        with open(rosco_path, "wb") as buffer:
            shutil.copyfileobj(rosco_file.file, buffer)
        with open(bsr_path, "wb") as buffer:
            shutil.copyfileobj(bsr_file.file, buffer)
        
        if macro_file and macro_file.filename:
            macro_path = os.path.join(UPLOAD_FOLDER, macro_file.filename)
            with open(macro_path, "wb") as buffer:
                shutil.copyfileobj(macro_file.file, buffer)

        # --- Run YOUR QC Pipeline (The 9 Checks) ---
        start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
        df = qc_general.load_bsr(bsr_path, col_map["bsr"])

        # Clean headers & values (same as app.py)
        df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
        df = df.applymap(lambda x: str(x).replace("\xa0", " ").strip() if isinstance(x, str) else x)
        df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

        # Execution order aligned with app.py:
        df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
        # completeness_check expects full rules dict in app.py — pass rules
        df = qc_general.completeness_check(df, col_map["bsr"], rules)
        df = qc_general.program_category_check(bsr_path, df, col_map, rules.get("program_category", {}), file_rules)
        df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
        df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
        # domestic market check as in app.py
        df = qc_general.domestic_market_check(df, project, col_map["bsr"], debug=True)
        df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
        df = qc_general.country_channel_id_check(df, col_map["bsr"])
        df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules.get("client_check", {}))
        # rates_and_ratings_check called again in app.py — keep consistent
        df = qc_general.rates_and_ratings_check(df, col_map["bsr"])

        # 2️ Duplicate Market Check FIRST — because Overlap depends on it (app.py flow)
        # duplicated_market_check returns (df, duplicated_channels) in app.py
        df = qc_general.duplicated_market_check(
            df, macro_path, project, col_map, file_rules, debug=True
        )

        # 3️ Overlap / Duplicate / Daybreak Check — pass duplicated channels
        df = qc_general.overlap_duplicate_daybreak_check(
            df, col_map["bsr"], rules.get("overlap_check", {}) # Note: duplicated_channels argument was removed from the qc_checks_1 definition.
        )

        # -----------------------------------------------------------
        #   OUTPUT SAVE (use file_rules)
        # -----------------------------------------------------------
        output_prefix = file_rules.get("output_prefix", "General_QC_Result_")
        output_sheet = file_rules.get("output_sheet_name", "QC Results")
        output_file = f"{output_prefix}{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        # Cleanup datetime formats (remove tz info if present)
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_convert(None).dt.tz_localize(None) if hasattr(df[col].dt, "tz") else df[col].dt.tz_localize(None)

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=output_sheet)

        qc_general.color_excel(output_path, df)
        qc_general.generate_summary_sheet(output_path, df, file_rules)

        return FileResponse(
            path=output_path,
            filename=output_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        for path in [rosco_path, bsr_path, macro_path]:
            if path and os.path.exists(path): os.remove(path)
        raise HTTPException(status_code=500, detail=f"An error occurred during General QC: {str(e)}")
    finally:
        # Removed 'await file.close()'
        pass

# -------------------- 2. UPDATED LALIGA QC ENDPOINT --------------------
@app.post("/api/run_laliga_qc")
def run_laliga_qc_checks( # <-- CHANGED from async def to def
    rosco_file: UploadFile = File(...),
    bsr_file: UploadFile = File(...),
    macro_file: UploadFile = File(...)
):
    """
    Runs YOUR FULL 11-check QC pipeline from qc_checks_1.py
    This mirrors the app.py flow (cleaning, ordering, duplicated market before overlap)
    """
    config = load_config()
    col_map = config["column_mappings"]
    rules = config["qc_rules"]
    project = config["project_rules"]
    file_rules = config["file_rules"]

    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
    macro_path = os.path.join(UPLOAD_FOLDER, macro_file.filename)
    
    try:
        # Save files synchronously
        with open(rosco_path, "wb") as buffer:
            shutil.copyfileobj(rosco_file.file, buffer)
        with open(bsr_path, "wb") as buffer:
            shutil.copyfileobj(bsr_file.file, buffer)
        with open(macro_path, "wb") as buffer:
            shutil.copyfileobj(macro_file.file, buffer)

        # --- Run YOUR QC Pipeline (ALL 11 Checks) ---
        start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
        df = qc_general.load_bsr(bsr_path, col_map["bsr"])

        # Clean headers & values (same as app.py)
        df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
        df = df.applymap(lambda x: str(x).replace("\xa0", " ").strip() if isinstance(x, str) else x)
        df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

        # Execution order aligned with app.py:
        df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
        df = qc_general.completeness_check(df, col_map["bsr"], rules)
        df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules.get("overlap_check", {}))
        df = qc_general.program_category_check(bsr_path, df, col_map, rules.get("program_category", {}), file_rules)
        df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
        df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
        df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
        df = qc_general.country_channel_id_check(df, col_map["bsr"])
        df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules.get("client_check", {}))
        
        df = qc_general.domestic_market_check(df, project, col_map["bsr"], debug=True)
        # For Laliga we still run duplicated market check using macro_path
        df = qc_general.duplicated_market_check(df, macro_path, project, col_map, file_rules, debug=True)

        # After the duplicated check ensure overlap/daybreak is run with duplicated channels
        # Note: duplicated_channels argument was removed from the qc_checks_1 definition.
        df = qc_general.overlap_duplicate_daybreak_check(
            df, col_map["bsr"], rules.get("overlap_check", {})
        )

        # Generate Output File
        output_prefix = file_rules.get("output_prefix", "Laliga_QC_Result_")
        output_sheet = file_rules.get("output_sheet_name", "Laliga QC Results")
        output_file = f"{output_prefix}{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        # Cleanup datetime formats
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_convert(None).dt.tz_localize(None) if hasattr(df[col].dt, "tz") else df[col].dt.tz_localize(None)

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=output_sheet)

        qc_general.color_excel(output_path, df)
        qc_general.generate_summary_sheet(output_path, df, file_rules)

        return FileResponse(
            path=output_path,
            filename=output_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        for path in [rosco_path, bsr_path, macro_path]:
            if path and os.path.exists(path): os.remove(path)
        raise HTTPException(status_code=500, detail=f"An error occurred during Laliga QC: {str(e)}")
    finally:
        # Removed 'await file.close()'
        pass

# -------------------- EPL Endpoints --------------------
import epl_checks   # <-- your new EPL backend file

# --------------------------------------------------------------------
# EPL PRE CHECKS
# --------------------------------------------------------------------
@app.post("/api/run_epl_pre_checks")
def run_epl_pre_checks(
    notfinal_bsr: UploadFile = File(...),
    rosco_file: UploadFile = File(...),
    market_dup_file: UploadFile = File(...)
):

    bsr_path = os.path.join(UPLOAD_FOLDER, notfinal_bsr.filename)
    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    market_dup_path = os.path.join(UPLOAD_FOLDER, market_dup_file.filename)

    try:
        # Save files
        for obj, path in [
            (notfinal_bsr, bsr_path),
            (rosco_file, rosco_path),
            (market_dup_file, market_dup_path)
        ]:
            with open(path, "wb") as f:
                shutil.copyfileobj(obj.file, f)

        # Run EPL Pre-Checks
        df = epl_checks.run_pre_checks(
            bsr_path=bsr_path,
            rosco_path=rosco_path,
            market_dup_path=market_dup_path
        )

        output_file = "EPL_Pre_Checks.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        df.to_excel(output_path, index=False)
        return FileResponse(output_path, filename=output_file)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --------------------------------------------------------------------
# EPL POST CHECKS
# --------------------------------------------------------------------
@app.post("/api/run_epl_post_checks")
def run_epl_post_checks(
    bsr_file: UploadFile = File(...),
    rosco_file: UploadFile = File(...),
    macro_file: UploadFile = File(...)
):

    bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    macro_path = os.path.join(UPLOAD_FOLDER, macro_file.filename)

    try:
        # Save files
        for obj, path in [
            (bsr_file, bsr_path),
            (rosco_file, rosco_path),
            (macro_file, macro_path)
        ]:
            with open(path, "wb") as f:
                shutil.copyfileobj(obj.file, f)

        # Run EPL Post-Checks
        df = epl_checks.run_post_checks(
            bsr_path=bsr_path,
            rosco_path=rosco_path,
            macro_path=macro_path
        )

        output_file = "EPL_Post_Checks.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        df.to_excel(output_path, index=False)
        return FileResponse(output_path, filename=output_file)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))