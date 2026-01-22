# from fastapi import FastAPI, Query, UploadFile, File, HTTPException, Form
# from fastapi.responses import FileResponse, JSONResponse
# from contextlib import asynccontextmanager
# import pandas as pd 
# import os
# import time
# import threading
# import shutil # Used for efficient file saving
# from typing import Optional, List # Added List for checks
# from C_data_processing import DataExplorer
# from io import BytesIO # Needed to save Excel in memory before returning

# # --- Data/Project Specific Imports ---
# # import pathlib
# # from constants import DATA_PATH 
# # from data_processing import DataExplorer # Assuming this is imported

# # --- QC Specific Imports ---
# from qc_checks import (
#     # ... (Your original QC imports) ...
#     detect_period_from_rosco,
#     load_bsr,
#     period_check,
#     completeness_check,
#     overlap_duplicate_daybreak_check,
#     program_category_check,
#     duration_check,
#     check_event_matchday_competition,
#     market_channel_program_duration_check,
#     domestic_market_coverage_check,
#     rates_and_ratings_check,
#     duplicated_markets_check,
#     country_channel_id_check,
#     client_lstv_ott_check,
#     color_excel,
#     generate_summary_sheet,
#     # Placeholder for a function that handles all market checks
#     # You would replace this with actual logic in qc_checks.py
#     # market_specific_check_processor,
# )

# from C_data_processing_f1 import ( 
#     BSRValidator, 
#     color_excel,
#     generate_summary_sheet,
# )

# # -------------------- ⚙️ Folder setup --------------------
# BASE_DIR = os.getcwd()
# UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
# OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
# os.makedirs(UPLOAD_FOLDER, exist_ok=True)
# os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# # -------------------- 🧹 Cleanup Functions --------------------
# def cleanup_old_files(folder_path, max_age_minutes=30):
#     """Deletes files older than max_age_minutes."""
#     now = time.time()
#     max_age_seconds = max_age_minutes * 60

#     for filename in os.listdir(folder_path):
#         file_path = os.path.join(folder_path, filename)
#         if os.path.isfile(file_path):
#             file_age = now - os.path.getmtime(file_path)
#             if file_age > max_age_seconds:
#                 try:
#                     os.remove(file_path)
#                     print(f"🧹 Deleted old file: {file_path}")
#                 except Exception as e:
#                     print(f"⚠️ Error deleting {file_path}: {e}")

# def start_background_cleanup():
#     """Starts a background thread that cleans up old files every 5 minutes."""
#     def run_cleanup():
#         while True:
#             cleanup_old_files(UPLOAD_FOLDER, max_age_minutes=30)
#             cleanup_old_files(OUTPUT_FOLDER, max_age_minutes=30)
#             time.sleep(300)

#     thread = threading.Thread(target=run_cleanup, daemon=True)
#     thread.start()
# # -----------------------------------------------------------

# # Start the cleanup thread
# start_background_cleanup()

# # -------------------- 🧠 FastAPI Setup and Lifespan --------------------

# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # This is your existing lifespan logic, ensuring the Laligadata is loaded
#     try:
#         # app.state.df = pd.read_csv(DATA_PATH / "Sales.csv" , index_col=0 , parse_dates= True)
#         app.state.df = pd.DataFrame() # Placeholder if Sales.csv isn't available
#     except Exception as e:
#         print(f"Warning: Could not load Sales.csv during startup: {e}")
#         app.state.df = pd.DataFrame() # Ensure state exists
        
#     yield
#     # Cleanup state
#     del app.state.df

# app = FastAPI(lifespan=lifespan)

# # -------------------- 📂 Original API Endpoints --------------------

# @app.post("/api/upload_csv")
# async def upload_csv(file: UploadFile = File(...)):
#     """
#     Handles CSV file upload from the frontend and saves it to the data directory.
#     """
#     file_location = os.path.join(UPLOAD_FOLDER, file.filename) 
    
#     try:
#         with open(file_location, "wb") as buffer:
#             shutil.copyfileobj(file.file, buffer)
            
#         app.state.df = pd.read_csv(file_location, index_col=0, parse_dates=True)

#         return {"filename": file.filename, "detail": f"File successfully uploaded and saved to {file_location}"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred during file upload: {e}")
#     finally:
#         await file.close()

# # -------------------- 📂 End Points Using DataExplorer Class --------------------

# @app.get("/api/summary")
# async def read_summary_data():
#     if app.state.df.empty:
#         raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
#     data = DataExplorer(app.state.df)
#     return data.summary().json_response()

# @app.get("/api/kpis")
# async def read_kpis(country: str = Query(None)):
#     if app.state.df.empty:
#         raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
#     data = DataExplorer(app.state.df)
#     return data.kpis(country)

# @app.get("/api/")
# async def read_sales(limit: int = Query(100, gt=0, lt=150000)):
#     if app.state.df.empty:
#         raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
#     data = DataExplorer(app.state.df, limit)
#     return data.json_response()

# # -------------------- 🚀 FULL QC API Endpoint Using C_data_processing.py --------------------

# @app.post("/api/run_qc")
# async def run_qc_checks(
#     rosco_file: UploadFile = File(..., description="The Rosco file (.xlsx)"),
#     bsr_file: UploadFile = File(..., description="The BSR file (.xlsx)"),
#     data_file: Optional[UploadFile] = File(None, description="The optional Client Data file (.xlsx)")
# ):
#     """
#     Runs the full QC pipeline on the uploaded Rosco, BSR, and optional Data files 
#     and returns the processed Excel file.
#     """
    
#     # Define paths for uploaded files
#     rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
#     bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
#     data_path = None

#     try:
#         # 1. Save uploaded files to disk (for path-based QC functions)
#         with open(rosco_path, "wb") as buffer:
#             shutil.copyfileobj(rosco_file.file, buffer)
#         with open(bsr_path, "wb") as buffer:
#             shutil.copyfileobj(bsr_file.file, buffer)
        
#         df_data = None
#         if data_file and data_file.filename:
#             data_path = os.path.join(UPLOAD_FOLDER, data_file.filename)
#             with open(data_path, "wb") as buffer:
#                 shutil.copyfileobj(data_file.file, buffer)
#             df_data = pd.read_excel(data_path) 

#         # 2. Run QC Pipeline 
#         start_date, end_date = detect_period_from_rosco(rosco_path)
#         df = load_bsr(bsr_path)

#         df = period_check(df, start_date, end_date)
#         df = completeness_check(df)
#         df = overlap_duplicate_daybreak_check(df)
#         df = program_category_check(df)
#         df = duration_check(df)

#         # Handle optional data file logic
#         df = check_event_matchday_competition(df, df_data=df_data, rosco_path=rosco_path)
#         df = market_channel_program_duration_check(df, reference_df=df_data)
#         df = domestic_market_coverage_check(df, reference_df=df_data)

#         df = rates_and_ratings_check(df)
#         df = duplicated_markets_check(df)
#         df = country_channel_id_check(df)
#         df = client_lstv_ott_check(df)

#         # 3. Generate Output File on Disk (in OUTPUT_FOLDER)
#         output_file = f"QC_Result_{os.path.splitext(bsr_file.filename)[0]}.xlsx"
#         output_path = os.path.join(OUTPUT_FOLDER, output_file)

#         df.to_excel(output_path, index=False)
#         color_excel(output_path, df)
#         generate_summary_sheet(output_path, df)

#         # 4. Return FileResponse
#         return FileResponse(
#             path=output_path,
#             filename=output_file,
#             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#         )

#     except Exception as e:
#         print(f"QC Error: {e}")
#         # Clean up any input files that might have been partially written
#         for path in [rosco_path, bsr_path, data_path]:
#             if path and os.path.exists(path):
#                 os.remove(path)
                
#         raise HTTPException(status_code=500, detail=f"An error occurred during QC processing: {str(e)}")
#     finally:
#         # Ensure all file streams are closed
#         await rosco_file.close()
#         await bsr_file.close()
#         if data_file:
#             await data_file.close()


# # -------------------- 🌍 NEW MARKET SPECIFIC CHECK ENDPOINT that is using market_specific_check_processor  --------------------

# # -------------------- 🌍 NEW MARKET SPECIFIC CHECK ENDPOINT (FIXED) --------------------
# # -------------------- 🌍 NEW MARKET SPECIFIC CHECK ENDPOINT (MODIFIED) --------------------
# @app.post("/api/market_check_and_process", response_model=None)
# async def market_check_and_process(
#     # BSR file (mandatory)
#     bsr_file: UploadFile = File(..., description="BSR file for market-specific checks"),
#     # Obligation file (optional, for F1 check)
#     obligation_file: Optional[UploadFile] = File(None, description="F1 Obligation file for broadcaster checks"), 
#     # NEW: Overnight file (optional, for Audience Update)
#     overnight_file: Optional[UploadFile] = File(None, description="Overnight Audience file for upscale/integrity check"), # <-- NEW PARAMETER
#     # List of checks to run
#     checks: List[str] = Form(..., description="List of selected check keys (e.g., 'remove_andorra')")
# ):
#     """
#     Applies selected market-specific checks and transformations to the BSR file.
#     It returns a JSON summary and a URL for file download.
#     """
    
#     bsr_file_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
#     obligation_path = None
#     overnight_path = None # <-- NEW PATH VARIABLE
    
#     # Generate a unique output filename that the frontend can use for download
#     output_filename = f"Processed_BSR_{os.path.splitext(bsr_file.filename)[0]}_{int(time.time())}.xlsx"
#     output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    
#     try:
#         # 1. Save uploaded BSR file temporarily
#         with open(bsr_file_path, "wb") as buffer:
#             shutil.copyfileobj(bsr_file.file, buffer)
            
#         # 2. Save optional Obligation file
#         if obligation_file and obligation_file.filename:
#             obligation_path = os.path.join(UPLOAD_FOLDER, obligation_file.filename)
#             with open(obligation_path, "wb") as buffer:
#                 shutil.copyfileobj(obligation_file.file, buffer)
#             print(f"Saved obligation file to: {obligation_path}")

#         # 3. Save optional Overnight file
#         if overnight_file and overnight_file.filename: # <-- NEW LOGIC
#             overnight_path = os.path.join(UPLOAD_FOLDER, overnight_file.filename)
#             with open(overnight_path, "wb") as buffer:
#                 shutil.copyfileobj(overnight_file.file, buffer)
#             print(f"Saved overnight file to: {overnight_path}")


#         # 4. Initialize Validator (Pass ALL optional paths here)
#         validator = BSRValidator(
#             bsr_path=bsr_file_path, 
#             obligation_path=obligation_path, 
#             overnight_path=overnight_path # <-- PASSING NEW PATH
#         ) 

#         # 5. Apply selected checks and capture the list of structured summaries
#         status_summaries = validator.market_check_processor(checks)
        
#         # 6. Access and save the modified DataFrame
#         df_processed = validator.df
        
#         # ... (File saving, JSON response, and error handling remain the same) ...

#         # 7. Construct the download URL and return the JSON response
#         clean_summaries = [s for s in status_summaries if isinstance(s, dict)]
#         if df_processed.empty:
#              raise Exception("Processed DataFrame is empty after applying checks.")

#         df_processed.to_excel(output_path, index=False)
#         download_url = f"/api/download_file?filename={output_filename}" 

#         return JSONResponse(content={
#             "status": "Success",
#             "message": f"Successfully applied {len(checks)} market checks. Processed file is ready for download.",
#             "download_url": download_url,
#             "summaries": clean_summaries
#         })

#     except Exception as e:
#         print(f"Market Check Error: {e}")
#         raise HTTPException(status_code=500, detail=f"An error occurred during market checks: {str(e)}")
#     finally:
#         # Ensure file streams are closed and cleanup is run
#         await bsr_file.close()
#         if obligation_file:
#             await obligation_file.close()
#         if overnight_file: # <-- CLOSE NEW STREAM
#             await overnight_file.close()
            
#         # IMPORTANT: Clean up uploaded source files immediately
#         for path in [bsr_file_path, obligation_path, overnight_path]: # <-- ADD NEW PATH TO CLEANUP
#             if path and os.path.exists(path):
#                 os.remove(path)


# # -------------------- 📥 NEW DOWNLOAD ENDPOINT --------------------
# # This endpoint handles the actual file retrieval requested via the download_url.

# @app.get("/api/download_file")
# async def download_file(filename: str = Query(...)):
#     """Retrieves a previously generated file from the output folder."""
#     file_path = os.path.join(OUTPUT_FOLDER, filename)
    
#     if not os.path.exists(file_path):
#         # This will be triggered if the cleanup thread deleted the file, or if the filename is bad
#         raise HTTPException(status_code=404, detail="File not found or link has expired.")
        
#     return FileResponse(
#         path=file_path,
#         filename=filename,
#         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#     )

# --------------------------------------------------------------------------------------------------------------------------------------------------------

# from fastapi import APIRouter, FastAPI, Query, UploadFile, File, HTTPException, Form
# from fastapi.responses import FileResponse, JSONResponse
# from contextlib import asynccontextmanager
# import pandas as pd 
# import os
# import time
# import threading
# import shutil
# from typing import Optional, List
# from C_data_processing import DataExplorer
# from io import BytesIO
# import json

# # --- QC Specific Imports ---
# from qc_checks import (
#     detect_period_from_rosco,
#     load_bsr,
#     period_check,
#     completeness_check,
#     overlap_duplicate_daybreak_check,
#     program_category_check,
#     duration_check,
#     check_event_matchday_competition,
#     market_channel_program_duration_check,
#     domestic_market_coverage_check,
#     rates_and_ratings_check,
#     duplicated_markets_check,
#     country_channel_id_check,
#     client_lstv_ott_check,
#     color_excel,
#     generate_summary_sheet,
# )

# from C_data_processing_f1 import ( 
#     BSRValidator, 
#     color_excel,
#     generate_summary_sheet,
# )

# MOCK_QC_SUMMARY = [
#     {"id": 1, "description": "Period Integrity Check", "action": "Audit", "status": "Completed", "total_issues_flagged": 0},
#     {"id": 2, "description": "Field Completeness Check", "action": "Audit", "status": "Issue Found", "total_issues_flagged": 15},
#     # ... rest of your mock data
# ]
# # -------------------- ⚙️ Folder setup --------------------
# BASE_DIR = os.getcwd()
# UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
# OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
# os.makedirs(UPLOAD_FOLDER, exist_ok=True)
# os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# # -------------------- 🧹 Cleanup Functions --------------------
# def cleanup_old_files(folder_path, max_age_minutes=30):
#     """Deletes files older than max_age_minutes."""
#     now = time.time()
#     max_age_seconds = max_age_minutes * 60

#     for filename in os.listdir(folder_path):
#         file_path = os.path.join(folder_path, filename)
#         if os.path.isfile(file_path):
#             file_age = now - os.path.getmtime(file_path)
#             if file_age > max_age_seconds:
#                 try:
#                     os.remove(file_path)
#                     print(f"🧹 Deleted old file: {file_path}")
#                 except Exception as e:
#                     print(f"⚠️ Error deleting {file_path}: {e}")

# def start_background_cleanup():
#     """Starts a background thread that cleans up old files every 5 minutes."""
#     def run_cleanup():
#         while True:
#             cleanup_old_files(UPLOAD_FOLDER, max_age_minutes=30)
#             cleanup_old_files(OUTPUT_FOLDER, max_age_minutes=30)
#             time.sleep(300)

#     thread = threading.Thread(target=run_cleanup, daemon=True)
#     thread.start()
# # -----------------------------------------------------------

# # Start the cleanup thread
# start_background_cleanup()

# # -------------------- 🧠 FastAPI Setup and Lifespan --------------------
# # NOTE: The lifespan context must be handled by the main application (master_app) 
# # or passed to the router via dependencies if state is required. 

# # 💡 CHANGE: Convert FastAPI app to APIRouter
# router = APIRouter()

# # -------------------- 📂 Original API Endpoints --------------------
# # 💡 CHANGE: Replace @app.post/get with @router.post/get and remove /api prefixes

# @router.post("/upload_csv")
# async def upload_csv(file: UploadFile = File(...)):
#     """Handles CSV file upload from the frontend and saves it to the data directory."""
#     file_location = os.path.join(UPLOAD_FOLDER, file.filename) 
    
#     try:
#         # Accessing app.state inside a router requires state to be passed or accessed via request.
#         # For simplicity, we assume this endpoint mainly handles file storage for now.
#         with open(file_location, "wb") as buffer:
#             shutil.copyfileobj(file.file, buffer)
            
#         # NOTE: If app.state is critical, this logic must be moved to dashboard_router or injected.
#         # We comment out the app.state line to avoid breaking the router conversion.
#         # app.state.df = pd.read_csv(file_location, index_col=0, parse_dates=True) 

#         return {"filename": file.filename, "detail": f"File successfully uploaded and saved to {file_location}"}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred during file upload: {e}")
#     finally:
#         await file.close()

# # -------------------- 📂 End Points Using DataExplorer Class --------------------

# # NOTE: These endpoints require app.state.df, which cannot be accessed directly in a standalone router.
# # This logic should ideally be moved to a function/file accessible by dashboard_router.
# # For simplicity, we define the routes but keep the logic requiring app.state commented out.

# @router.get("/summary")
# async def read_summary_data():
#     # if app.state.df.empty: raise HTTPException(...)
#     # data = DataExplorer(app.state.df)
#     # return data.summary().json_response()
#     return {"detail": "Summary data logic pending full state integration."}


# @router.get("/kpis")
# async def read_kpis(country: str = Query(None)):
#     # if app.state.df.empty: raise HTTPException(...)
#     # data = DataExplorer(app.state.df)
#     # return data.kpis(country)
#     return {"detail": "KPI logic pending full state integration."}


# @router.get("/")
# async def read_sales(limit: int = Query(100, gt=0, lt=150000)):
#     # if app.state.df.empty: raise HTTPException(...)
#     # data = DataExplorer(app.state.df, limit)
#     # return data.json_response()
#     return {"detail": "Sales logic pending full state integration."}


# # -------------------- 🚀 FULL QC API Endpoint Using C_data_processing.py --------------------

# @router.post("/run_qc")
# async def run_qc_checks(
#     rosco_file: UploadFile = File(..., description="The Rosco file (.xlsx)"),
#     bsr_file: UploadFile = File(..., description="The BSR file (.xlsx)"),
#     data_file: Optional[UploadFile] = File(None, description="The optional Client Data file (.xlsx)")
# ):
#     # ... (QC logic remains, ensure you use local paths for the QC functions) ...
    
#     # Define paths for uploaded files
#     rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
#     bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
#     data_path = None

#     try:
#         # 1. Save uploaded files to disk (for path-based QC functions)
#         with open(rosco_path, "wb") as buffer:
#             shutil.copyfileobj(rosco_file.file, buffer)
#         with open(bsr_path, "wb") as buffer:
#             shutil.copyfileobj(bsr_file.file, buffer)
        
#         df_data = None
#         if data_file and data_file.filename:
#             data_path = os.path.join(UPLOAD_FOLDER, data_file.filename)
#             with open(data_path, "wb") as buffer:
#                 shutil.copyfileobj(data_file.file, buffer)
#             df_data = pd.read_excel(data_path) 

#         # 2. Run QC Pipeline 
#         start_date, end_date = detect_period_from_rosco(rosco_path)
#         df = load_bsr(bsr_path)

#         df = period_check(df, start_date, end_date)
#         df = completeness_check(df)
#         df = overlap_duplicate_daybreak_check(df)
#         df = program_category_check(df)
#         df = duration_check(df)
#         df = check_event_matchday_competition(df, df_data=df_data, rosco_path=rosco_path)
#         df = market_channel_program_duration_check(df, reference_df=df_data)
#         df = domestic_market_coverage_check(df, reference_df=df_data)
#         df = rates_and_ratings_check(df)
#         df = duplicated_markets_check(df)
#         df = country_channel_id_check(df)
#         df = client_lstv_ott_check(df)

#         # 3. Generate Output File on Disk (in OUTPUT_FOLDER)
#         output_file = f"QC_Result_{os.path.splitext(bsr_file.filename)[0]}.xlsx"
#         output_path = os.path.join(OUTPUT_FOLDER, output_file)

#         df.to_excel(output_path, index=False)
#         color_excel(output_path, df)
#         generate_summary_sheet(output_path, df)

#         # 💡 Extract the Summary Data (MOCK/Real Logic Needed Here)
#         # Since the backend usually generates this summary table, we need to extract it.
#         # TEMPORARY MOCK FOR SUMMARY DATA (You would replace this with actual logic):
#         summary_data = [
#     # 🚨 FIX 1: Use a dictionary structure instead of QcSummaryResult()
#                 {
#                     "id": 1, 
#                     "description": "Period Integrity Check", 
#                     "action": "Audit", 
#                     "status": "Completed", 
#                     "total_issues_flagged": 0
#                 },
#                 {
#                     "id": 2, 
#                     "description": "Field Completeness Check", 
#                     "action": "Audit", 
#                     "status": "Issue Found", 
#                     "total_issues_flagged": 15
#                 },
#                 # ... and so on
#             ]

#         # 4. Return JSON Response with Download URL
#         download_url =  f"/api/qc/download_file?filename={output_file}"

#         # 4. Return FileResponse
#         # return QcRunResponse(
#         #     status="Success",
#         #     message="QC checks complete. File ready for download.",
#         #     download_url=download_url,
#         #     summaries=summary_data # Return the summary data for the frontend table
#         # )

#         return JSONResponse(content={
#             "status": "Success",
#             "message": "QC checks complete. File ready for download.",
#             "download_url": download_url,
#             "summaries": summary_data # List of dictionaries
#         })

#     except Exception as e:
#         print(f"QC Error: {e}")
#         for path in [rosco_path, bsr_path, data_path]:
#             if path and os.path.exists(path):
#                 os.remove(path)
#         raise HTTPException(status_code=500, detail=f"An error occurred during QC processing: {str(e)}")
#     finally:
#         await rosco_file.close()
#         await bsr_file.close()
#         if data_file: await data_file.close()


# # -------------------- 🌍 NEW MARKET SPECIFIC CHECK ENDPOINT --------------------

# @router.post("/market_check_and_process") # response_model removed for simplicity
# async def market_check_and_process(
#     bsr_file: UploadFile = File(..., description="BSR file for market-specific checks", alias="bsr_file"),
#     obligation_file: Optional[UploadFile] = File(None, description="F1 Obligation file", alias="obligation_file"), 
#     overnight_file: Optional[UploadFile] = File(None, description="Overnight Audience file", alias="overnight_file"),
#     # 🚨 FIX 1: Set type hint to STR to correctly receive the JSON string from JSON.stringify()
#     checks: str = Form(..., alias="checks", description="JSON list of check keys to run")
# ):
    
#     bsr_file_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
#     obligation_path = None
#     overnight_path = None 
    
#     output_filename = f"Processed_BSR_{os.path.splitext(bsr_file.filename)[0]}_{int(time.time())}.xlsx"
#     output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    
#     # 🚨 FIX 2: Explicitly parse the JSON string immediately
#     try:
#         # Convert the incoming JSON string into a Python list
#         checks_list_to_process: List[str] = json.loads(checks)
#     except Exception as e:
#         # Handle if the input was not a valid JSON array string
#         raise HTTPException(status_code=400, detail=f"Invalid check list format: Expected JSON string, got {type(checks)}. Error: {e}")
    
#     # 🚨 FIX 3: Check if the list is empty after parsing
#     if not checks_list_to_process:
#         raise HTTPException(status_code=400, detail="No checks were selected or passed.")
    
#     # 🚨 DEBUGGING: This print statement shows the correctly parsed list
#     print(f"Final checks list passed to validator: {checks_list_to_process}")

#     try:
#         # 1. Save Files
#         with open(bsr_file_path, "wb") as buffer: shutil.copyfileobj(bsr_file.file, buffer)
#         if obligation_file and obligation_file.filename:
#             obligation_path = os.path.join(UPLOAD_FOLDER, obligation_file.filename)
#             with open(obligation_path, "wb") as buffer: shutil.copyfileobj(obligation_file.file, buffer)
#         if overnight_file and overnight_file.filename:
#             overnight_path = os.path.join(UPLOAD_FOLDER, overnight_file.filename)
#             with open(overnight_path, "wb") as buffer: shutil.copyfileobj(overnight_file.file, buffer)

#         # 2. Instantiate and Run Validator
#         # Assuming BSRValidator is accessible
#         validator = BSRValidator(
#             bsr_path=bsr_file_path, 
#             obligation_path=obligation_path, 
#             overnight_path=overnight_path 
#         ) 
        
#         # 3. Call the processor with the correctly parsed list
#         # This list is guaranteed to be ['duration_limits', ...]
#         status_summaries = validator.market_check_processor(checks_list_to_process)
#         df_processed = validator.df
        
#         # 4. Finalize Output
#         clean_summaries = [s for s in status_summaries if isinstance(s, dict)]
#         if df_processed.empty: raise Exception("Processed DataFrame is empty after applying checks.")

#         df_processed.to_excel(output_path, index=False)
        
#         # 5. Return Final JSON Response
#         download_url = f"/api/qc/download_file?filename={output_filename}" 

#         return JSONResponse(content={
#             "status": "Success",
#             "message": f"Successfully applied {len(checks_list_to_process)} market checks. Processed file is ready for download.",
#             "download_url": download_url,
#             "summaries": clean_summaries
#         })

#     except Exception as e:
#         print(f"Market Check Error: {e}")
#         # Clean up files on error
#         for path in [bsr_file_path, obligation_path, overnight_path]:
#             if path and os.path.exists(path): os.remove(path)
            
#         raise HTTPException(status_code=500, detail=f"An error occurred during market checks: {str(e)}")
#     finally:
#         # Close file streams and clean up disk files
#         if 'bsr_file' in locals() and bsr_file: await bsr_file.close()
#         if 'obligation_file' in locals() and obligation_file: await obligation_file.close()
#         if 'overnight_file' in locals() and overnight_file: await overnight_file.close()
            
#         for path in [bsr_file_path, obligation_path, overnight_path]:
#             if path and os.path.exists(path): os.remove(path)

# # -------------------- 📥 DOWNLOAD ENDPOINT --------------------
# # 💡 NOTE: This endpoint needs to remain outside of the /qc prefix if its called as /api/download_file.
# # We will define a separate router for general utility, or rely on dashboard_router for /api.

# @router.get("/download_file")
# async def download_file(filename: str = Query(...)):
#     """Retrieves a previously generated file from the output folder."""
#     file_path = os.path.join(OUTPUT_FOLDER, filename)
    
#     if not os.path.exists(file_path):
#         raise HTTPException(status_code=404, detail="File not found or link has expired.")
        
#     return FileResponse(
#         path=file_path,
#         filename=filename,
#         media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#     )
    
# # -------------------- 🌟 FINAL EXPORT 🌟 --------------------
# # We export the APIRouter object instead of the FastAPI app instance.
# # This router will be included in app/dashboard_routes.py under the /qc prefix.
# qc_router = router


# --------------------------------------------------------------------------------------------------------------------------------------------------------------

from fastapi import APIRouter, FastAPI, Query, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
import pandas as pd 
import os
import time
import threading
import shutil 
from typing import Optional, List 
from C_data_processing import DataExplorer
from io import BytesIO 
import json 

# =========================
# QC IMPORTS (DO NOT CHANGE)
# =========================
from qc_checks import (
    detect_period_from_rosco,
    load_bsr,
    period_check,
    completeness_check,
    overlap_duplicate_daybreak_check,
    program_category_check,
    check_event_matchday_competition,
    market_channel_consistency_check,
    rates_and_ratings_check,
    country_channel_id_check,
    color_excel,
    generate_summary_sheet,
)


from C_data_processing_f1 import BSRValidator
from C_data_processing_EPL import EPLValidator

# --- NEW QC IMPORTS ---
import qc_checks as qc_general
import epl_checks 

# -------------------- ⚙️ Folder setup --------------------
# =========================
# APP + ROUTER
# =========================
app = FastAPI(title="QC Hub API")
qc_router = APIRouter()

# =========================
# PATHS
# =========================
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# =========================
# BACKGROUND CLEANUP (SAFE)
# =========================
def cleanup_old_files(folder, max_age_minutes=30):
    now = time.time()
    max_age = max_age_minutes * 60
    for f in os.listdir(folder):
        path = os.path.join(folder, f)
        if os.path.isfile(path) and now - os.path.getmtime(path) > max_age:
            try:
                os.remove(path)
            except:
                pass

def start_cleanup_thread():
    def loop():
        while True:
            cleanup_old_files(UPLOAD_FOLDER)
            cleanup_old_files(OUTPUT_FOLDER)
            time.sleep(300)
    threading.Thread(target=loop, daemon=True).start()

start_cleanup_thread()

# =========================
# CONFIG LOADER
# =========================
def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

# =========================
# FIX: SAFE COLUMN FLATTENER
# =========================
def _flatten(val):
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        return [val]
    return []

# =========================
# FIX: FIXTURE EXTRACTOR
# =========================
def extract_fixtures_sheet(bsr_path):
    xl = pd.ExcelFile(bsr_path)
    for s in xl.sheet_names:
        if "fixture" in s.lower():
            return xl.parse(s)
    return None


# -------------------- 📂 Original API Endpoints --------------------

# 💡 NOTE: If you need app.state here, you must add 'request: Request' to parameters
# and access it via request.app.state.df

@qc_router.post("/api/upload_csv")
async def upload_csv(request: Request, file: UploadFile = File(...)):
    file_location = os.path.join(UPLOAD_FOLDER, file.filename) 
    try:
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # Accessing state via request.app.state
        if hasattr(request.app.state, 'df'):
            request.app.state.df = pd.read_csv(file_location, index_col=0, parse_dates=True)

        return {"filename": file.filename, "detail": f"File successfully uploaded and saved to {file_location}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during file upload: {e}")
    finally:
        await file.close()

# --------------------  End Points Using DataExplorer Class  --------------------

@qc_router.get("/api/summary")
async def read_summary_data(request: Request):
    if not hasattr(request.app.state, 'df') or request.app.state.df.empty:
        raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
    data = DataExplorer(request.app.state.df)
    return data.summary().json_response()

@qc_router.get("/api/kpis")
async def read_kpis(request: Request, country: str = Query(None)):
    if not hasattr(request.app.state, 'df') or request.app.state.df.empty:
        raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
    data = DataExplorer(request.app.state.df)
    return data.kpis(country)

@qc_router.get("/api/")
async def read_sales(request: Request, limit: int = Query(100, gt=0, lt=150000)):
    if not hasattr(request.app.state, 'df') or request.app.state.df.empty:
        raise HTTPException(status_code=404, detail="Data not loaded. Upload Sales.csv first.")
    data = DataExplorer(request.app.state.df, limit)
    return data.json_response()

# --------------------  QC API Endpoint --------------------

# ======================================================
# ================= GENERAL QC ENDPOINT =================
# ======================================================
@qc_router.post("/run_general_qc")
def run_general_qc(
    rosco_file: UploadFile = File(...),
    bsr_file: UploadFile = File(...)
):
    """
    Streamlit-parity General QC
    Uses ONLY qc_checks.py
    """

    config = load_config()
    col_map = config["column_mappings"]
    rules = config["qc_rules"]
    file_rules = config["file_rules"]

    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)

    try:
        # ---------------- Save files ----------------
        with open(rosco_path, "wb") as f:
            shutil.copyfileobj(rosco_file.file, f)
        with open(bsr_path, "wb") as f:
            shutil.copyfileobj(bsr_file.file, f)

        # ---------------- Period ----------------
        start_date, end_date = detect_period_from_rosco(rosco_path)

        # ---------------- Load BSR ----------------
        df = load_bsr(bsr_path)

        df.columns = (
            df.columns.astype(str)
            .str.replace("\xa0", " ", regex=False)
            .str.strip()
        )

        # ---------------- AUTO SORT (FIXED) ----------------
        sort_cols = []
        for key in ("channel", "date", "start_time"):
            sort_cols.extend(_flatten(col_map["bsr"].get(key)))

        sort_cols = [c for c in sort_cols if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols).reset_index(drop=True)

        # ---------------- QC CHECKS ----------------
        df = period_check(df, start_date, end_date)
        df = completeness_check(df, col_map["bsr"], rules.get("program_category", {}))
        df = overlap_duplicate_daybreak_check(df, col_map["bsr"], rules.get("overlap_check", {}))
        df = program_category_check(
            bsr_path, df, col_map,
            rules.get("program_category", {}),
            file_rules
        )

        fixtures_df = extract_fixtures_sheet(bsr_path)
        if fixtures_df is not None:
            df = check_event_matchday_competition(df, fixtures_df)
        else:
            df["Event_Matchday_Competition_OK"] = False
            df["Event_Matchday_Competition_Remark"] = "Fixtures sheet missing"

        df = market_channel_consistency_check(df, rosco_path, col_map, file_rules)
        df = rates_and_ratings_check(df, col_map["bsr"])
        df = country_channel_id_check(df, col_map["bsr"])

        # ---------------- OUTPUT ----------------
        output_file = f"General_QC_Result_{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        for c in df.select_dtypes(include=["datetimetz"]).columns:
            df[c] = df[c].dt.tz_localize(None)

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="QC Results")
            if fixtures_df is not None:
                fixtures_df.to_excel(writer, index=False, sheet_name="Original Fixtures")

        color_excel(output_path, df)
        generate_summary_sheet(output_path, df)

        return FileResponse(
            path=output_path,
            filename=output_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================
# DOWNLOAD ENDPOINT
# =========================
@qc_router.get("/download_file")
def download_file(filename: str = Query(...)):
    path = os.path.join(OUTPUT_FOLDER, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=filename)

app.include_router(qc_router, prefix="/api/qc")


# -------------------- 🌍 F1 MARKET CHECK ENDPOINT --------------------

EPL_CHECK_KEYS = {
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
    "check_premier_league_october_obligation",
    "filter_short_programs",
    "audit_ovn_whistle_to_whistle",
    "check_star_sports_3_consolidation",
    "check_bsa_nielsen_audience_presence",
    "audit_uk_ire_volume_consistency",
    
    # --- Newly Added (Missing from your snippet) ---
    "check_source_mediatype_validity",
    "sa_nielsen_inclusion_check",
    "epl_live_vs_delay_validation",
    "pl_magazine_highlights_classification",
    "audit_uk_ire_duplication_alignment",
    "audit_ott_broadcast_consolidation",
    "check_missing_live_games"
}

@qc_router.post("/market_check_and_process", response_model=None)
def market_check_and_process( 
    bsr_file: UploadFile = File(..., description="BSR file for market-specific checks"),
    obligation_file: Optional[UploadFile] = File(None, description="F1 Obligation file for broadcaster checks"), 
    overnight_file: Optional[UploadFile] = File(None, description="Overnight Audience file for upscale/integrity check"),
    macro_file: Optional[UploadFile] = File(None, description="Macro BSA Market Duplicator file"),
    checks: List[str] = Form(..., description="List of selected check keys"),
    check_configs: str = Form("{}", description="JSON string of runtime configurations")
):
    bsr_file_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
    obligation_path, overnight_path, macro_path = None, None, None
    
    output_filename = f"Processed_BSR_{os.path.splitext(bsr_file.filename)[0]}_{int(time.time())}.xlsx"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)

    status_summaries = []
    df_processed = None 
    
    try:
        try:
            config_dict = json.loads(check_configs)
        except json.JSONDecodeError:
            config_dict = {}
            
        with open(bsr_file_path, "wb") as buffer:
            shutil.copyfileobj(bsr_file.file, buffer)
            
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

        bsr_checks_to_run = [c for c in checks if c not in EPL_CHECK_KEYS]
        epl_checks_to_run = [c for c in checks if c in EPL_CHECK_KEYS]

        shared_kwargs = {
            'bsr_path': bsr_file_path, 
            'obligation_path': obligation_path, 
            'overnight_path': overnight_path, 
            'macro_path': macro_path
        }
        
        bsr_validator = BSRValidator(**shared_kwargs)
        epl_validator = EPLValidator(df=bsr_validator.df,**shared_kwargs)

        if bsr_checks_to_run:
            status_summaries.extend(bsr_validator.market_check_processor(bsr_checks_to_run))
            df_processed = bsr_validator.df 
        
        if epl_checks_to_run:
            if bsr_checks_to_run and df_processed is not None:
                epl_validator.df = df_processed
                
            epl_summaries = [epl_validator.market_check_map[c]() for c in epl_checks_to_run if c in epl_validator.market_check_map]
            status_summaries.extend(epl_summaries)
            df_processed = epl_validator.df 

        if df_processed is None:
            df_processed = bsr_validator.df 
        
        if df_processed.empty:
            raise Exception("Processed DataFrame is empty after applying checks.")

        clean_summaries = [s for s in status_summaries if isinstance(s, dict)]
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_processed.to_excel(writer, sheet_name='Processed BSR', index=False)
            
        download_url = f"/api/qc/download_file?filename={output_filename}" 

        return JSONResponse(content={
            "status": "Success",
            "message": f"Successfully applied {len(checks)} market checks.",
            "download_url": download_url,
            "summaries": clean_summaries
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during market checks: {str(e)}")
        
    finally:
        for path in [bsr_file_path, obligation_path, overnight_path, macro_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

# -------------------- 📥 NEW DOWNLOAD ENDPOINT --------------------
@qc_router.get("/download_file")
async def download_file(filename: str = Query(...)):
    file_path = os.path.join(OUTPUT_FOLDER, filename)

    # --- DEBUG PRINTS ---
    print(f"DEBUG: Endpoint hit! Looking for file: {filename}")
    print(f"DEBUG: Full path constructed: {file_path}")
    print(f"DEBUG: Does file exist? {os.path.exists(file_path)}")
    # --------------------

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found or link has expired.")
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -------------------- 2. UPDATED LALIGA QC ENDPOINT --------------------
@qc_router.post("/api/run_laliga_qc")
def run_laliga_qc_checks(
    rosco_file: UploadFile = File(...),
    bsr_file: UploadFile = File(...),
    macro_file: UploadFile = File(...)
):
    config = load_config()
    col_map = config["column_mappings"]
    rules = config["qc_rules"]
    project = config["project_rules"]
    file_rules = config["file_rules"]

    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
    macro_path = os.path.join(UPLOAD_FOLDER, macro_file.filename)
    
    try:
        with open(rosco_path, "wb") as buffer:
            shutil.copyfileobj(rosco_file.file, buffer)
        with open(bsr_path, "wb") as buffer:
            shutil.copyfileobj(bsr_file.file, buffer)
        with open(macro_path, "wb") as buffer:
            shutil.copyfileobj(macro_file.file, buffer)

        start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
        df = qc_general.load_bsr(bsr_path, col_map["bsr"])

        df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
        df = df.applymap(lambda x: str(x).replace("\xa0", " ").strip() if isinstance(x, str) else x)
        df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

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
        df = qc_general.duplicated_market_check(df, macro_path, project, col_map, file_rules, debug=True)

        df = qc_general.overlap_duplicate_daybreak_check(
            df, col_map["bsr"], rules.get("overlap_check", {})
        )

        output_prefix = file_rules.get("output_prefix", "Laliga_QC_Result_")
        output_sheet = file_rules.get("output_sheet_name", "Laliga QC Results")
        output_file = f"{output_prefix}{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

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

# -------------------- EPL Endpoints --------------------

@qc_router.post("/api/run_epl_pre_checks")
def run_epl_pre_checks(
    notfinal_bsr: UploadFile = File(...),
    rosco_file: UploadFile = File(...),
    market_dup_file: UploadFile = File(...)
):
    bsr_path = os.path.join(UPLOAD_FOLDER, notfinal_bsr.filename)
    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    market_dup_path = os.path.join(UPLOAD_FOLDER, market_dup_file.filename)

    try:
        for obj, path in [
            (notfinal_bsr, bsr_path),
            (rosco_file, rosco_path),
            (market_dup_file, market_dup_path)
        ]:
            with open(path, "wb") as f:
                shutil.copyfileobj(obj.file, f)

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

@qc_router.post("/api/run_epl_post_checks")
def run_epl_post_checks(
    bsr_file: UploadFile = File(...),
    rosco_file: UploadFile = File(...),
    macro_file: UploadFile = File(...)
):
    bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
    rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
    macro_path = os.path.join(UPLOAD_FOLDER, macro_file.filename)

    try:
        for obj, path in [
            (bsr_file, bsr_path),
            (rosco_file, rosco_path),
            (macro_file, macro_path)
        ]:
            with open(path, "wb") as f:
                shutil.copyfileobj(obj.file, f)

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