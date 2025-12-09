import os
import shutil
import json
import pandas as pd
from typing import Dict, List, Optional, Tuple , Any
from fastapi import UploadFile, HTTPException

class FileService:
    """Handles file system operations: Saving uploads and cleaning up."""
    
    def __init__(self, upload_folder: str):
        self.upload_folder = upload_folder

    def save_upload(self, file_obj: Optional[UploadFile]) -> Optional[str]:
        """Saves a single UploadFile to disk and returns the path."""
        if not file_obj or not file_obj.filename:
            return None
        
        file_path = os.path.join(self.upload_folder, file_obj.filename)
        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file_obj.file, buffer)
            print(f"DEBUG: Saved file to {file_path}", flush=True)
            return file_path
        except Exception as e:
            print(f"ERROR: Failed to save {file_obj.filename}: {e}", flush=True)
            raise HTTPException(status_code=500, detail=f"Failed to save file {file_obj.filename}")

    def cleanup_files(self, paths: List[Optional[str]]):
        """Safely deletes a list of file paths."""
        for path in paths:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

class ConfigService:
    """Handles parsing of configuration data."""
    
    @staticmethod
    def parse_check_configs(json_str: str) -> Dict[str, Any]:
        """Parses JSON string into a dictionary safely."""
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"WARNING: JSON Parse Error: {e}", flush=True)
            # Return empty dict or raise error depending on strictness required
            return {}

class ReportService:
    """Handles extraction of reports and Excel generation."""
    
    @staticmethod
    def extract_secondary_reports(summaries: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        """Extracts specific dataframes (Channel Summary, SA Defect) from validator summaries."""
        reports = {}
        for summary in summaries:
            details = summary.get('details', {})
            
            # 1. Channel Count Report
            if details.get('channel_count_report_data'):
                reports['Channel Summary'] = pd.DataFrame.from_records(details['channel_count_report_data'])
                
            # 2. SA Defect Report
            if isinstance(details.get('sa_defect_report_df'), pd.DataFrame):
                reports['SA Defect Report'] = details['sa_defect_report_df']
                
        return reports

    @staticmethod
    def save_multi_sheet_excel(df_main: pd.DataFrame, secondary_reports: Dict[str, pd.DataFrame], output_path: str):
        """Writes the main dataframe and any secondary reports to a multi-sheet Excel file."""
        try:
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                # Sheet 1: Main Data
                df_main.to_excel(writer, sheet_name='Processed BSR', index=False)
                
                # Secondary Sheets
                for sheet_name, report_df in secondary_reports.items():
                    if not report_df.empty:
                        report_df.to_excel(writer, sheet_name=sheet_name, index=False)
        except Exception as e:
            print(f"ERROR: Excel Generation Failed: {e}", flush=True)
            raise HTTPException(status_code=500, detail=f"Failed to generate Excel report: {str(e)}")