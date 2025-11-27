import pandas as pd
import re
from typing import List ,Dict,Any, Set
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows
from io import BytesIO 
from pandas.api.types import is_object_dtype, is_categorical_dtype, CategoricalDtype 
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta
import numpy as np
from datetime import timedelta


# --- Constants ---
DATE_FORMAT = "%Y-%m-%d"
GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
HEADER_FILL = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")


class EPLValidator:
    """
    Handles loading, validating, and processing of BSR data.
    The dependency on the Rosco file has been removed.
    
    """
    # --- AUDIENCE CHECK CLASS CONSTANTS ---
    OVERNIGHT_SHEET = "DATA"
    OVERNIGHT_AUDIENCE_COL = 'Audience'
    BSR_TARGET_COL_RAW = 'Aud Metered (000s) 3+'
    GP_FILTER_COL = 'Grand Prix'
    GP_FILTER_VALUE = '15_Dutch GP'
    
    # Canonical Column Names
    COUNTRY_COLUMN = 'Market' 
    CHANNEL_COLUMN = 'TV-Channel'
    DATE_COLUMN = 'Date'
    SESSION_COMPETITION_COLUMN = 'Competition'

    def __init__(self, bsr_path: str, obligation_path: str = None, overnight_path: str = None, macro_path: str = None):
        # self.df = df        
        self.bsr_path = bsr_path
        self.df = self._load_bsr()
        # New: Store the obligation path, but don't load the full DF yet
        self.obligation_path = obligation_path
        self.full_obligation_df = None # Will store the entire obligation sheet
        # NEW: Store the overnight path
        self.overnight_path = overnight_path # <-- STORED HERE
        self.macro_path = macro_path
        # 🚨 NEW: Load and store the duplication rules DataFrame
        self.dup_rules_df = self._load_and_filter_macro_rules()

        # ✅ FIX: Load the DataFrame immediately using the path
        # try:
        #     self.df = pd.read_excel(self.bsr_path)
        # except Exception as e:
        #     raise ValueError(f"Failed to load BSR file at {self.bsr_path}: {e}")

        # Initialize other attributes
        self.full_obligation_df = None 
        
        # Load duplication rules (Assuming _load_and_filter_macro_rules is defined in your class)
        try:
            self.dup_rules_df = self._load_and_filter_macro_rules()
        except Exception:
            # Handle case where macro path might not be set or valid
            self.dup_rules_df = pd.DataFrame()
        
        # Dictionary to map market check keys to internal methods (to be implemented)
        self.market_check_map = {
        "impute_lt_live_status": self._impute_lt_live_status,
        "consolidate_gillete_soccer": self._consolidate_gillette_soccer_programs,
        "check_sky_showcase_live": self._check_sky_showcase_live_status,
        "standardize_uk_ire_region": self._standardize_uk_ire_region,
        "check_fixture_vs_case" : self._check_fixture_vs_case,
        "check_pan_balkans_serbia_parity" : self._check_pan_balkans_serbia_parity,
        "audit_multi_match_status" : self._audit_multi_match_status,
        "check_date_time_format_integrity" : self._check_date_time_format_integrity,
        "check_live_broadcast_uniqueness" : self._check_live_broadcast_uniqueness,
        "audit_channel_line_item_count" : self._audit_channel_line_item_count,
        "check_combined_archive_status": self._check_combined_archive_status,
        "suppress_duplicated_audience" : self._suppress_duplicated_audience,
        "filter_short_programs": self._filter_short_programs
        # Future EPL checks would be added here
    }

    def _load_and_filter_macro_rules(self):
        """Loads, filters, and standardizes the macro duplication rules file."""
        if not self.macro_path:
            return None
            
        MACRO_SHEET_NAME = "Data Core"
        MACRO_HEADER_INDEX = 1 
        SEARCH_TERM = "Formula 1"
        REQUIRED_RULE_COLS = ['Orig Market', 'Dup Market', 'Dup Channel', 'Projects'] # Include Projects for filtering

        try:
            df_macro = pd.read_excel(self.macro_path, sheet_name=MACRO_SHEET_NAME, header=MACRO_HEADER_INDEX)
            df_macro.columns = [str(c).strip() for c in df_macro.columns]

            # 1. Filter by Project (Formula 1)
            filtered_df = df_macro[
                df_macro['Projects'].astype(str).str.contains(SEARCH_TERM, case=False, na=False)
            ].copy()
            
            # 2. Select and clean required columns
            df_dup_rules = filtered_df[REQUIRED_RULE_COLS].copy()

            # Ensure key columns are clean strings (strip, upper case)
            for col in ['Orig Market', 'Dup Market', 'Dup Channel']:
                if col in df_dup_rules.columns:
                    df_dup_rules[col] = df_dup_rules[col].astype(str).str.strip().str.upper()
                    
            # We only need 'Orig Market', 'Dup Market', 'Dup Channel' for the validation check
            return df_dup_rules[['Orig Market', 'Dup Market', 'Dup Channel']].drop_duplicates()
        
        except Exception as e:
            print(f"Error loading duplication rules from macro file: {e}")
            return None

    # --- Private Loading/Parsing Methods (from old qc_checks.py) ---
    def _load_overnight_data(self):
        """
        Loads, standardizes, filters, and prepares the overnight audience file 
        for merging with the BSR data. The Grand Prix filter is applied immediately 
        after initial column mapping for maximum efficiency.
        """
        # Complex rule defined locally for clarity
        DATE_SWAP_RULES = {
            pd.to_datetime('2025-08-30'): pd.to_datetime('2025-07-05'),
            pd.to_datetime('2025-08-31'): pd.to_datetime('2025-07-06'),
            pd.to_datetime('2025-07-06'): pd.to_datetime('2025-07-06') 
        }

        if not self.overnight_path:
            return None
            
        try:
            OVERNIGHT_COLS_RAW = ['Country', 'Channel', 'Date', 'Session', 'Grand Prix', self.OVERNIGHT_AUDIENCE_COL]
            
            # Load data using raw column names
            df_overnight = pd.read_excel(self.overnight_path, sheet_name=self.OVERNIGHT_SHEET, header=0, usecols=OVERNIGHT_COLS_RAW)
            df_overnight.columns = [str(c).strip() for c in df_overnight.columns]
            
            # --- Initial Renaming (Country -> Market, Channel -> TV-Channel) ---
            if 'Country' in df_overnight.columns:
                df_overnight = df_overnight.rename(columns={'Country': self.COUNTRY_COLUMN}, errors='ignore')
            if 'Channel' in df_overnight.columns:
                df_overnight = df_overnight.rename(columns={'Channel': self.CHANNEL_COLUMN}, errors='ignore')
            
            # --- CRITICAL FILTERING STEP (STEP B) ---
            # Apply the Grand Prix filter immediately after renaming columns
            if self.GP_FILTER_COL in df_overnight.columns:
                df_overnight = df_overnight[df_overnight[self.GP_FILTER_COL] == self.GP_FILTER_VALUE].copy()
            
            # ⭐ NEW PRINT STATEMENT ⭐
            print("\n--- OVERNIGHT DF STATE (Post-GP Filter, Pre-Transformation) ---")
            print(f"Rows after filtering '{self.GP_FILTER_VALUE}': {len(df_overnight)}")
            print(f"Columns (Raw): {df_overnight.columns.tolist()}")
            print("---------------------------------------------------------------")
            
            if df_overnight.empty:
                print(f"Warning: Overnight data is empty after filtering for '{self.GP_FILTER_VALUE}'.")
                return None

            # --- Standardize and Clean ---
            
            # Standardize String Columns (using the BSR's names)
            for col in [self.COUNTRY_COLUMN, self.CHANNEL_COLUMN, 'Session', self.GP_FILTER_COL]:
                if col in df_overnight.columns:
                    df_overnight[col] = df_overnight[col].astype(str).str.strip().str.upper()

            if self.DATE_COLUMN in df_overnight.columns:
                df_overnight[self.DATE_COLUMN] = pd.to_datetime(df_overnight[self.DATE_COLUMN], errors='coerce')

            # --- STEP A: APPLY DATE SWAP LOGIC ---
            for original_date, target_date in DATE_SWAP_RULES.items():
                if self.DATE_COLUMN in df_overnight.columns:
                    df_overnight.loc[df_overnight[self.DATE_COLUMN] == original_date, self.DATE_COLUMN] = target_date

            # --- STEP C & D: FORCE SESSION ALIGNMENT & Rename ---
            TARGET_DATE_QUALIFYING = pd.to_datetime('2025-07-05')
            TARGET_DATE_RACE = pd.to_datetime('2025-07-06')
            SESSION_COL_NAME = 'Session'
            
            if self.DATE_COLUMN in df_overnight.columns and SESSION_COL_NAME in df_overnight.columns:
                df_overnight.loc[df_overnight[self.DATE_COLUMN] == TARGET_DATE_QUALIFYING, SESSION_COL_NAME] = 'QUALIFYING'
                df_overnight.loc[df_overnight[self.DATE_COLUMN] == TARGET_DATE_RACE, SESSION_COL_NAME] = 'RACE'

            df_overnight = df_overnight.rename(columns={'Session': self.SESSION_COMPETITION_COLUMN}, errors='ignore')
            df_overnight[self.OVERNIGHT_AUDIENCE_COL] = pd.to_numeric(df_overnight[self.OVERNIGHT_AUDIENCE_COL], errors='coerce')

            FINAL_COLS = [self.COUNTRY_COLUMN, self.CHANNEL_COLUMN, self.DATE_COLUMN, self.SESSION_COMPETITION_COLUMN, self.OVERNIGHT_AUDIENCE_COL]
            return df_overnight[FINAL_COLS]
            
        except Exception as e:
            print(f"Error loading and preparing overnight file: {e}")
            return None

    def _update_audience_from_overnight(self) -> Dict[str, Any]:
        """
        Compares BSR audience with Max Overnight Audience, updating the BSR value if 
        the overnight audience is higher, and explicitly flagging the status of every row.
        """
        initial_rows = len(self.df)
        
        # --- CONSTANTS ---
        OVERNIGHT_AUDIENCE_COL = self.OVERNIGHT_AUDIENCE_COL
        BSR_TARGET_COL_RAW = self.BSR_TARGET_COL_RAW 
        QC_FLAG_COL = 'QC_Audience_Update_Status' # NEW Status Flag Column
        
        # Canonical Column Names
        COUNTRY_COLUMN = self.COUNTRY_COLUMN      
        CHANNEL_COLUMN = self.CHANNEL_COLUMN      
        DATE_COLUMN = self.DATE_COLUMN            
        SESSION_COMPETITION_COLUMN = self.SESSION_COMPETITION_COLUMN 
        
        FINAL_MERGE_ON_COLS = [COUNTRY_COLUMN, CHANNEL_COLUMN, DATE_COLUMN, SESSION_COMPETITION_COLUMN]
        
        # 1. Load and Prepare Overnight data (Assumed correct)
        df_overnight = self._load_overnight_data()

        if df_overnight is None or BSR_TARGET_COL_RAW not in self.df.columns:
            return {"check_key": "update_audience_from_overnight", "status": "Skipped", "action": "Audience Update", "description": "Skipped: Missing Overnight file or target BSR column.", "details": {"rows_updated": 0}}
        
        # 2. Prepare BSR for merging (Standardize keys)
        self.df[BSR_TARGET_COL_RAW] = pd.to_numeric(self.df[BSR_TARGET_COL_RAW], errors='coerce')
        
        # Apply standardization to BSR columns
        for col in [COUNTRY_COLUMN, CHANNEL_COLUMN, SESSION_COMPETITION_COLUMN]:
            if col in self.df.columns:
                self.df.loc[:, col] = self.df[col].astype(str).str.strip().str.upper()
        if DATE_COLUMN in self.df.columns:
            self.df.loc[:, DATE_COLUMN] = pd.to_datetime(self.df[DATE_COLUMN], errors='coerce')
            
        # --- 3. AGGREGATE OVERNIGHT DATA (Get max audience per key) ---
        df_overnight_max = df_overnight.groupby(FINAL_MERGE_ON_COLS, dropna=False)[OVERNIGHT_AUDIENCE_COL].max().reset_index()
        df_overnight_max = df_overnight_max.rename(columns={OVERNIGHT_AUDIENCE_COL: 'Max_Overnight_Audience'})

        # 4. MERGE AND COMPARE
        merged_df = self.df.merge(
            df_overnight_max, 
            on=FINAL_MERGE_ON_COLS, 
            how='left' 
        )
        
        # Initialize the new status column in the merged DataFrame
        merged_df[QC_FLAG_COL] = 'No Match Found' # Default state

        # Scale BSR audience to absolute numbers (multiplying by 1000)
        temp_bsr_abs = merged_df[BSR_TARGET_COL_RAW] * 1000.0

        # Mask A: Rows where a match was found (Max_Overnight_Audience is NOT NaN)
        match_found_mask = merged_df['Max_Overnight_Audience'].notna()
        
        # Mask B: Rows updated (Max_Overnight_Audience > BSR_ABS)
        update_mask = match_found_mask & \
                    (merged_df['Max_Overnight_Audience'] > temp_bsr_abs) & \
                    (merged_df[BSR_TARGET_COL_RAW].notna())

        # --- 5. Apply Status Flags ---
        
        # Status 2: OK (Match found, but BSR was already higher or equal)
        # This is the residual mask: Match found AND NOT updated.
        ok_mask = match_found_mask & (~update_mask)
        merged_df.loc[ok_mask, QC_FLAG_COL] = 'OK - BSR Value Retained'
        
        # Status 1: UPDATED (The highest priority flag)
        merged_df.loc[update_mask, QC_FLAG_COL] = 'UPDATED - Scaled from Overnight Max'

        # 6. Perform the value update
        rows_updated = update_mask.sum()
        
        if rows_updated > 0:
            updated_value_in_thousands = merged_df.loc[update_mask, 'Max_Overnight_Audience'] / 1000.0
            
            # Write the new audience value to the BSR's target column
            self.df.loc[update_mask[update_mask].index, BSR_TARGET_COL_RAW] = updated_value_in_thousands 
        
        # --- 7. Finalize (Copy new columns back to self.df) ---
        self.df[QC_FLAG_COL] = merged_df[QC_FLAG_COL]

        return {
            "check_key": "update_audience_from_overnight",
            "status": "Completed" if rows_updated == 0 else "Flagged",
            "action": "Audience Update",
            "description": f"Updated BSR audience rows by overriding {rows_updated} values with higher Max Overnight data.",
            "details": {
                "rows_updated": int(rows_updated),
                "rows_not_matched": int(ok_mask.sum()),
                "rows_skipped": int((merged_df[QC_FLAG_COL] == 'No Match Found').sum()),
                "total_rows_processed": int(initial_rows)
            }
        }

    # New Private Method to load the full obligation sheet once
    def _load_full_obligation_data(self) -> pd.DataFrame:
        """
        Loads the F1 Obligation sheet and filters it to include ONLY the '15_Dutch GP' 
        event data, storing the filtered DataFrame in self.full_obligation_df.
        """
        if self.full_obligation_df is not None:
            return self.full_obligation_df

        if not self.obligation_path:
            return pd.DataFrame()
            
        TARGET_GP = '15_Dutch GP' # <-- Define the target GP here
        
        try:
            # Load the entire obligation sheet
            df_obl = pd.read_excel(
                self.obligation_path, 
                sheet_name="F1 - Broadcaster Obligations",
            )
            df_obl.columns = [str(c).strip() for c in df_obl.columns]
            
            # --- CRITICAL FILTERING STEP ---
            # Filter the loaded DataFrame for the specific GP
            df_obl_filtered = df_obl[df_obl.get('GP') == TARGET_GP].copy()

            print(f"Obligation data loaded and filtered for: {TARGET_GP}. Rows found: {len(df_obl_filtered)}")
            
            # Store and return the filtered DataFrame
            self.full_obligation_df = df_obl_filtered
            return df_obl_filtered
            
        except FileNotFoundError:
            print(f"Error: Obligation file not found at {self.obligation_path}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error loading/filtering obligation sheet: {e}")
            return pd.DataFrame()

    def _detect_header_row(self, sheet_name=0):
        """
        Detects the header row index by scanning the first 200 rows 
        of the specified sheet for key column names.
        
        Args:
            sheet_name: The name or index of the Excel sheet to read. Defaults to the first sheet (0).
        """
        # Read a sample of the specified sheet
        df_sample = pd.read_excel(
            self.bsr_path, 
            sheet_name=sheet_name, 
            header=None, 
            nrows=200
        )
        
        for i, row in df_sample.iterrows():
            # Convert row to a single, space-separated, lowercase string for detection
            # Use fillna('') to handle rows that might be mostly empty
            row_str = " ".join(row.fillna('').astype(str).tolist()).lower()

            # First set of keywords (common BSR columns)
            if all(k in row_str for k in ["region", "market", "broadcaster"]):
                return i
            
            # Second set of keywords (common date/time columns)
            if "date" in row_str and ("utc" in row_str or "gmt" in row_str):
                return i
                
        raise ValueError(f"Could not detect header row in '{sheet_name}' sheet of BSR file.")

    def _load_bsr(self):
        # Define the specific sheet name based on your example
        sheet_name_to_load = "Worksheet" 

        # Detect the header row on the specified sheet
        header_row = self._detect_header_row(sheet_name=sheet_name_to_load)

        # Load the full data using the detected header row and sheet name
        df = pd.read_excel(
            self.bsr_path, 
            sheet_name=sheet_name_to_load,  # Use the specific sheet name
            header=header_row               # Use the dynamically detected header row
        )
        
        # Ensure column names are clean
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # --- Methods for Market Specific Checks (Placeholder Implementation) ---
    def market_check_processor(self, checks: List[str]) -> List[Dict[str, Any]]:
        # ... (Method contents remain unchanged - assumed correct)
        status_summaries = [] 
        
        for check_key in checks:
            if check_key in self.market_check_map:
                try:
                    result = self.market_check_map[check_key]()
                    if result:
                        status_summaries.append(result)
                    print(f"Applied custom check: {check_key}")
                except Exception as e:
                    status_summaries.append({
                        "check_key": check_key,
                        "status": "Failed",
                        "action": "Error during execution",
                        "description": f"Check failed due to internal error: {str(e)}",
                        "details": {"error": str(e)}
                    })
                    print(f"Error applying check {check_key}: {e}")
            else:
                print(f"Warning: Unknown check key received: {check_key}")
                
        return status_summaries

    def _impute_lt_live_status(self) -> Dict[str, Any]:
        """
        If 'L/T' is found in the Combined column, classifies the program as Live 
        and adds a flag indicating the recommended status, without modifying 
        the original 'Type of program' column.
        """
        initial_rows = len(self.df)
        
        # The output column for the recommended status/flag
        FLAG_COLUMN = 'QC_Recommended_Program_Type' 
        
        # 1. Initialization and Checks
        self.df[FLAG_COLUMN] = 'Current Status OK'
        REQUIRED_COLS = ['Combined', 'Type of program']
        
        if not all(col in self.df.columns for col in REQUIRED_COLS):
            return {
                "check_key": "impute_lt_live_status", "status": "Skipped",
                "action": "L/T Live Imputation", 
                "description": "Skipped: Missing required BSR columns.",
                "details": {"rows_flagged": 0}
            }

        # Prepare normalized columns
        combined_norm = self.df['Combined'].astype(str).str.upper()
        type_of_program_norm = self.df['Type of program'].astype(str).str.lower()

        # 2. Identify Target Rows for Classification
        
        # Mask A: Rows where 'L/T' keyword is present (case-insensitive regex search)
        is_lt_present_mask = combined_norm.str.contains(r'L/T', na=False)
        
        # 3. Apply Classification and Flag
        
        # Apply the recommended status to all rows where 'L/T' is present
        self.df.loc[is_lt_present_mask, FLAG_COLUMN] = 'Recommended: Live'

        # Now, audit the rows that already had a non-live status but should be Live.
        
        # Anomalous Mask: L/T is present AND the original status is NOT 'Live'
        is_not_live_mask = type_of_program_norm != 'live'
        anomalous_flag_mask = is_lt_present_mask & is_not_live_mask
        
        # Update the flag column for the specific anomaly (overwriting "Recommended: Live")
        self.df.loc[anomalous_flag_mask, FLAG_COLUMN] = 'ANOMALY: Should be Live (L/T Present)'
        
        rows_flagged_anomaly = anomalous_flag_mask.sum()
        
        return {
            "check_key": "impute_lt_live_status",
            "status": "Flagged" if rows_flagged_anomaly > 0 else "Completed",
            "action": "L/T Live Imputation & Anomaly Check", 
            "description": f"Audited L/T status. Flagged {rows_flagged_anomaly} rows where 'L/T' was present but 'Type of program' was not 'Live'.",
            "details": {
                "rows_flagged_anomaly": int(rows_flagged_anomaly),
                "total_lt_programs": int(is_lt_present_mask.sum())
            }
        }

    def _consolidate_gillette_soccer_programs(self) -> Dict[str, Any]:
        """
        Identifies sequential 'Gillete Soccer' programs where the gap between the 
        End Time of the first and the Start Time of the second is 30 minutes or less.
        The second, later row is flagged for consolidation with a reference to the preceding row.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Consolidate_Gillete_Soccer'
        KEYWORD = 'GILLETE SOCCER'
        MAX_GAP_MINUTES = 30
        
        # Define required columns for time, content, and grouping
        REQUIRED_COLS = ['Combined', 'Date', 'Start', 'End', 'Market', 'TV-Channel']
        if not all(col in self.df.columns for col in REQUIRED_COLS):
            return {
                "check_key": "consolidate_gillete_soccer", "status": "Skipped",
                "action": "Program Consolidation Check", 
                "description": "Skipped: Missing required BSR columns.",
                "details": {"rows_flagged": 0}
            }

        self.df[FLAG_COLUMN] = 'OK'
        
        # 2. Prepare Timestamps (using safe parsing logic)
        try:
            # Create robust datetime objects. Assume 'Date' represents the start date of the program.
            date_key = self.df['Date'].astype(str).str[:10]
            self.df['Start_DT'] = pd.to_datetime(date_key + ' ' + self.df['Start'].astype(str), errors='coerce')
            
            # Adjust End_DT for programs that cross midnight.
            end_times = self.df['End'].astype(str)
            base_end_dt = pd.to_datetime(date_key + ' ' + end_times, errors='coerce')
            rollover_mask = (base_end_dt < self.df['Start_DT']) & base_end_dt.notna()
            base_end_dt.loc[rollover_mask] += timedelta(days=1)
            self.df['End_DT'] = base_end_dt
            
        except Exception as e:
            return {
                "check_key": "consolidate_gillete_soccer", "status": "Failed",
                "action": "Program Consolidation Check", 
                "description": f"Failed to parse Date/Time columns: {e}",
                "details": {"rows_flagged": 0}
            }

        # 3. Filter and Sort Candidates
        
        # Filter for candidates that are NOT missing time data
        gillete_mask = self.df['Combined'].astype(str).str.upper().str.contains(KEYWORD, na=False)
        df_candidates = self.df[gillete_mask & self.df['Start_DT'].notna() & self.df['End_DT'].notna()].copy()
        
        # Preserve original index for final flagging
        df_candidates['Original_Index'] = df_candidates.index
        
        # Grouping by Market and Channel only
        GROUP_COLS = ['Market', 'TV-Channel']
        df_candidates = df_candidates.sort_values(by=GROUP_COLS + ['Start_DT'])

        # 4. Perform Sequential Gap Check (Now grouping only by Market and Channel)
        
        # Dictionary to store complex flags: {original_index_to_flag: message}
        complex_flags = {}
        
        for _, group in df_candidates.groupby(GROUP_COLS):
            # Calculate the gap in minutes
            time_gap_minutes = (group['Start_DT'] - group['End_DT'].shift(1)) / timedelta(minutes=1)
            
            # Get the original index of the PRECEDING row (Row A)
            preceding_original_indices = group['Original_Index'].shift(1)
            
            # Identify the second row in the sequence where gap is valid
            consolidation_mask = (time_gap_minutes <= MAX_GAP_MINUTES) & (time_gap_minutes >= 0)
            
            # Filter down to the indices that meet the consolidation criteria
            indices_to_flag_now = group[consolidation_mask]['Original_Index']
            preceding_indices_now = preceding_original_indices[consolidation_mask]

            # Construct the detailed flag message for each flagged row
            for idx_to_flag, idx_preceding in zip(indices_to_flag_now, preceding_indices_now):
                
                # Look up the Start Time of the preceding row (Row A) using its original index
                # Assuming 'Start' column is a readable string/object
                preceding_start_time = self.df.loc[idx_preceding, 'Start']
                
                # Construct the descriptive flag message
                flag_message = (f"Consolidate with program starting at {preceding_start_time} "
                                f"(Original Index: {idx_preceding}, Gap <= {MAX_GAP_MINUTES}min)")
                
                complex_flags[idx_to_flag] = flag_message

        rows_flagged = len(complex_flags)
        
        # 5. Apply Flag to Original DataFrame (Uses the dictionary mapping)
        if rows_flagged > 0:
            # Create a pandas Series from the dictionary {original_index: message}
            flag_series = pd.Series(complex_flags)
            
            # Apply the messages directly to the original DataFrame using .loc
            self.df.loc[flag_series.index, FLAG_COLUMN] = flag_series
            
            # Final cleanup of temporary columns in self.df
            self.df.drop(columns=['Start_DT', 'End_DT'], inplace=True, errors='ignore')

        return {
            "check_key": "consolidate_gillete_soccer",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Program Consolidation Check", 
            "description": f"Flagged {rows_flagged} sequential 'Gillete Soccer' rows for consolidation (gap <= 30 min).",
            "details": {
                "rows_flagged": int(rows_flagged),
                "max_gap_minutes": MAX_GAP_MINUTES
            }
        }

    def _check_sky_showcase_live_status(self) -> Dict[str, Any]:
        """
        Implements a zero-tolerance check: flags any program on 'Sky Showcase' 
        in the UK/United Kingdom market that is incorrectly labeled 'Live', 
        as this channel is designated for Repeat/Delayed content only.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Sky_Showcase_Live_Flag'
        
        # Define specific target parameters using robust strings/regex
        TARGET_MARKET_REGEX = r'UNITED KINGDOM|UK' # Broadening to include full name and Ireland (common data area)
        TARGET_CHANNEL_KEYWORD = 'SKY SHOWCASE' # Keyword in the TV-Channel name
        FORBIDDEN_STATUS = 'LIVE'
        
        REQUIRED_COLS = ['Market', 'TV-Channel', 'Type of program']
        if not all(col in self.df.columns for col in REQUIRED_COLS):
            return {
                "check_key": "check_sky_showcase_live", "status": "Skipped",
                "action": "Zero-Tolerance Live Check", 
                "description": "Skipped: Missing required BSR columns.",
                "details": {"rows_flagged": 0}
            }

        self.df[FLAG_COLUMN] = 'OK'

        # 1. Normalize columns for reliable filtering
        market_norm = self.df['Market'].astype(str).str.strip().str.upper()
        channel_norm = self.df['TV-Channel'].astype(str).str.strip().str.upper()
        type_norm = self.df['Type of program'].astype(str).str.strip().str.upper()

        # 2. Identify the target rows (Robust Market AND Channel Identification)
        
        # Mask 1: Identify UK/Ireland market variants
        market_match_mask = market_norm.str.contains(TARGET_MARKET_REGEX, regex=True, na=False)
        
        # Mask 2: Identify Sky Showcase variants (uses str.contains to catch "Sky Showcase DE")
        channel_match_mask = channel_norm.str.contains(TARGET_CHANNEL_KEYWORD, na=False)
        
        target_rows_mask = market_match_mask & channel_match_mask
        
        # 3. Identify the error condition (Target row AND status is 'LIVE')
        error_mask = target_rows_mask & (type_norm == FORBIDDEN_STATUS)
        
        rows_flagged = error_mask.sum()
        
        # 4. Apply Flag to Original DataFrame
        if rows_flagged > 0:
            
            # Construct the flag message
            flag_message = f"INTEGRITY ERROR: Designated repeat channel ({TARGET_CHANNEL_KEYWORD} variant) is incorrectly marked '{FORBIDDEN_STATUS}'."
            
            # Apply flag only to rows currently marked OK
            rows_to_flag = error_mask & (self.df[FLAG_COLUMN] == 'OK')
            
            self.df.loc[rows_to_flag, FLAG_COLUMN] = flag_message

        # 5. Final Summary
        return {
            "check_key": "check_sky_showcase_live",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Zero-Tolerance Live Check", 
            "description": f"Flagged {rows_flagged} rows on Sky Showcase variants that were incorrectly tagged as 'Live'.",
            "details": {
                "rows_flagged": int(rows_flagged),
                "target_channel_keyword": TARGET_CHANNEL_KEYWORD,
                "forbidden_status": FORBIDDEN_STATUS
            }
        }

    def _standardize_uk_ire_region(self) -> Dict[str, Any]:
        """
        Flags rows in the UK and Ireland markets where the 'Region' column is incorrectly 
        designated (i.e., not 'Europe'), detecting inconsistent market spellings like 'U.K'.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Region_Standardization_Flag'
        
        # Define the markets that need to be within the 'Europe' region
        TARGET_MARKETS = ['UNITED KINGDOM', 'UK', 'IRELAND']
        CANONICAL_REGION = 'Europe'
        
        REQUIRED_COLS = ['Market', 'Region']
        # ... (Skipped initial checks for brevity) ...

        self.df[FLAG_COLUMN] = 'OK'

        # 1. Prepare normalized Market column
        market_norm = self.df['Market'].astype(str).str.strip().str.upper()
        
        # --- CRITICAL FIX: Aggressively remove periods/punctuation from Market name ---
        # This ensures 'U.K' becomes 'UK' for comparison.
        market_norm = market_norm.str.replace(r'[^A-Z\s]', '', regex=True).str.strip() 
        
        # 2. Create the mask for all rows in the target markets
        target_market_mask = market_norm.isin(TARGET_MARKETS)
        
        # 3. Identify rows where the Region column is currently INCORRECT (not Europe)
        region_norm = self.df['Region'].astype(str).str.strip().str.title() 
        
        # Final mask identifies the inconsistency
        inconsistency_mask = target_market_mask & (region_norm != CANONICAL_REGION)
        
        rows_flagged = inconsistency_mask.sum()
        
        # 4. Apply Flag
        if rows_flagged > 0:
            
            # Action 1: Flag the row for auditing purposes
            flag_message = f"Region INCONSISTENCY: Market should be '{CANONICAL_REGION}', but current value is non-standard."
            
            # Apply flag only to rows currently marked OK
            rows_to_flag = inconsistency_mask & (self.df[FLAG_COLUMN] == 'OK')
            
            self.df.loc[rows_to_flag, FLAG_COLUMN] = flag_message

        # 5. Final Summary
        return {
            "check_key": "standardize_uk_ire_region",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Region Standardization", 
            "description": f"Flagged {rows_flagged} UK/Ireland rows with regional designation inconsistencies.",
            "details": {
                "rows_flagged": int(rows_flagged),
                "target_markets": TARGET_MARKETS,
                "canonical_region": CANONICAL_REGION
            }
        }

    def _check_fixture_vs_case(self) -> Dict[str, Any]:
        """
        Checks the 'Phase / Fixture / Episode Desc.' column for incorrect casing of 
        the separator 'VS' (must be lowercase 'vs') and flags violating rows.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Fixture_Vs_Case_Flag'
        TARGET_COL = 'Phase / Fixture / Episode Desc.'
        
        # Target the specific market for the check (UK Market only)
        TARGET_MARKET = 'United Kingdom'
        
        REQUIRED_COLS = ['Market', TARGET_COL]
        if not all(col in self.df.columns for col in REQUIRED_COLS):
            return {
                "check_key": "check_fixture_vs_case", "status": "Skipped",
                "action": "Fixture Case Check", 
                "description": "Skipped: Missing required BSR columns.",
                "details": {"rows_flagged": 0}
            }

        self.df[FLAG_COLUMN] = 'OK'
        
        # 1. Normalize market column for filtering
        market_norm = self.df['Market'].astype(str).str.strip().str.upper()
        uk_market_mask = market_norm == TARGET_MARKET.upper()
        
        # 2. Identify the error condition: Uppercase 'VS' variants
        
        # We must look for any instance of 'VS' that is NOT entirely lowercase 'vs'
        # The safest way is to find non-lowercase instances of V/S surrounded by spaces, or check if the title contains V or S.
        
        # Mask A: Rows in the target market
        
        # Mask B: Rows that contain the uppercase form of 'VS' (case-sensitive check for the error)
        # We look for "VS", "Vs", or "V.S." (space-sensitive) in the column content.
        # Note: We must ensure we don't flag words that start with V or S.
        
        # Check 1: Find rows that contain 'VS' or 'Vs' (must be applied to the column content)
        target_content = self.df[TARGET_COL].astype(str).str.strip()
        
        # Use regex to find "VS" or "Vs" surrounded by non-word boundaries or spaces
        # We will use the simple regex pattern (VS or Vs) within word boundaries or spaces
        
        # A robust way to check for the improper casing:
        # 1. Standardize the whole column to lowercase.
        # 2. Check the difference between the original and the standardized column where the substring 'vs' is present.
        
        # Filter only UK market rows for analysis
        target_content_uk = target_content[uk_market_mask].copy()
        
        # Check 1: Does the content contain any form of 'VS' (case-insensitive)?
        vs_present_mask = target_content_uk.str.contains(r'VS', case=False, na=False)
        
        # Check 2: Does the content contain the invalid uppercase form?
        # We must specifically check for V or S being capitalized in the context of 'vs'.
        
        # To check for improper capitalization, we look for 'VS' or 'Vs' in the original content
        improper_case_mask = target_content_uk.str.contains(r'(VS|Vs|V\s+S)', case=True, na=False)
        
        # Final error mask: In the UK market, VS is present, AND the casing is improper (non-lowercase)
        error_mask = uk_market_mask & improper_case_mask.reindex(self.df.index).fillna(False)
        
        rows_flagged = error_mask.sum()
        
        # 3. Apply Flag
        if rows_flagged > 0:
            
            flag_message = "CASE INTEGRITY ERROR: Fixture must use only lowercase 'vs'. Uppercase ('VS' or 'Vs') found."
            
            # Apply flag only to rows currently marked OK
            rows_to_flag = error_mask & (self.df[FLAG_COLUMN] == 'OK')
            
            self.df.loc[rows_to_flag, FLAG_COLUMN] = flag_message

        # 4. Final Summary
        return {
            "check_key": "check_fixture_vs_case",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Fixture Case Check", 
            "description": f"Flagged {rows_flagged} UK rows where the fixture delimiter was incorrectly capitalized (must be 'vs').",
            "details": {
                "rows_flagged": int(rows_flagged),
                "target_market": TARGET_MARKET
            }
        }

    def _check_pan_balkans_serbia_parity(self) -> Dict[str, Any]:
        """
        Checks if the total program row count for 'Pan Balkans' is strictly equal to 
        the total program row count for 'Serbia', enforcing structural parity post-modeling.
        Correctly handles hyphens/spaces in market names.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Balkans_Serbia_Parity_Flag'
        
        # Define Target Markets (Use the clean form without spaces/hyphens)
        TARGET_MARKET_A_CLEAN = 'PANBALKANS' # Pan-Balkans
        TARGET_MARKET_B_CLEAN = 'SERBIA' # Serbia
        
        REQUIRED_COLS = ['Market']
        if not all(col in self.df.columns for col in REQUIRED_COLS):
            return {
                "check_key": "check_pan_balkans_serbia_parity", "status": "Skipped",
                "action": "Market Parity Check", 
                "description": "Skipped: Missing required BSR 'Market' column.",
                "details": {"parity_match": "False", "rows_flagged": 0}
            }

        self.df[FLAG_COLUMN] = 'OK'
        
        # 1. Normalize market column: Upper, strip, and remove all hyphens and spaces
        market_norm = self.df['Market'].astype(str).str.upper().str.strip()
        market_norm = market_norm.str.replace(r'[\s\-]+', '', regex=True) # <-- FIX: Removes spaces and hyphens

        # 2. Count Total Programs in each target market
        
        # Count rows in Market A (Matching against the clean constant)
        count_a = (market_norm == TARGET_MARKET_A_CLEAN).sum()
        
        # Count rows in Market B
        count_b = (market_norm == TARGET_MARKET_B_CLEAN).sum()
        
        # 3. Perform Parity Check
        is_parity_match = (count_a > 0) and (count_a == count_b)
        
        rows_flagged = 0
        
        if not is_parity_match:
            
            # Apply the mismatch mask based on the normalized values
            mismatch_mask = (market_norm == TARGET_MARKET_A_CLEAN) | (market_norm == TARGET_MARKET_B_CLEAN)
            
            # Apply the flag (using the assumed clean original names for the message)
            flag_message = (f"PARITY ERROR: Program count mismatch between Pan Balkans ({count_a} rows) "
                            f"and Serbia ({count_b} rows). Counts must be identical.")
            
            rows_to_flag = mismatch_mask & (self.df[FLAG_COLUMN] == 'OK')
            
            self.df.loc[rows_to_flag, FLAG_COLUMN] = flag_message
            rows_flagged = rows_to_flag.sum()


        # 4. Final Summary
        return {
            "check_key": "check_pan_balkans_serbia_parity",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Market Parity Check", 
            "description": f"Audited program count parity. Counts: Pan Balkans ({count_a}), Serbia ({count_b}).",
            "details": {
                "rows_flagged": int(rows_flagged),
                "pan_balkans_count": int(count_a),
                "serbia_count": int(count_b),
                "parity_match": str(is_parity_match)
            }
        }

    def _audit_multi_match_status(self) -> Dict[str, Any]:
        """
        Audits rows containing 'Goal Rush' or 'Konferenz/Conference' (Multi-Match content). 
        Flags rows if the 'Phase / Fixture / Episode Desc.' column does not contain 
        the 'Multi-Match' classification.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Multi_Match_Audit_Flag'
        
        # Define keywords and target columns
        MULTI_MATCH_KEYWORDS = ['GOAL RUSH', 'KONFERENZ', 'CONFERENCE']
        DESCRIPTION_COL = 'Program Description'
        FIXTURE_DESC_COL = 'Phase / Fixture / Episode Desc.'
        
        # Define the expected classification tag that MUST be present in the fixture description
        EXPECTED_FIXTURE_TAG = 'MULTI-MATCH' 
        
        REQUIRED_COLS = [DESCRIPTION_COL, FIXTURE_DESC_COL]
        if not all(col in self.df.columns for col in REQUIRED_COLS):
            return {
                "check_key": "audit_multi_match", "status": "Skipped",
                "action": "Multi-Match Audit", 
                "description": "Skipped: Missing required BSR columns.",
                "details": {"rows_flagged": 0}
            }

        self.df[FLAG_COLUMN] = 'OK'

        # 1. Prepare normalized columns
        description_norm = self.df[DESCRIPTION_COL].astype(str).str.upper()
        fixture_desc_norm = self.df[FIXTURE_DESC_COL].astype(str).str.upper()
        
        # 2. Identify the primary target: rows containing Multi-Match keywords in description
        match_keyword_pattern = '|'.join([re.escape(k) for k in MULTI_MATCH_KEYWORDS])
        target_mask = description_norm.str.contains(match_keyword_pattern, na=False)

        # 3. Define Error Mask: Target identified AND Fixture Description is missing the tag
        
        # Check if the expected tag (MULTI-MATCH) is present in the fixture description
        fixture_tag_present_mask = fixture_desc_norm.str.contains(EXPECTED_FIXTURE_TAG, na=False)
        
        # The error is when the target is TRUE, but the tag is FALSE
        error_mask = target_mask & (~fixture_tag_present_mask)

        rows_flagged = error_mask.sum()
        
        # 4. Apply Flag (No data change required, just flagging)
        if error_mask.any():
            
            flag_message = f"FIXTURE TAG MISSING: Description contains Multi-Match content (Goal Rush/Konferenz), but '{FIXTURE_DESC_COL}' does not contain the required tag '{EXPECTED_FIXTURE_TAG}'."
            
            # Apply flag only to rows currently marked OK
            rows_to_flag = error_mask & (self.df[FLAG_COLUMN] == 'OK')
            
            self.df.loc[rows_to_flag, FLAG_COLUMN] = flag_message
            rows_flagged = (self.df[FLAG_COLUMN] != 'OK').sum() # Re-count rows that were actually flagged


        # 5. Final Summary
        return {
            "check_key": "audit_multi_match",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Multi-Match Audit", 
            "description": f"Audited Multi-Match content. Flagged {rows_flagged} rows missing the required '{EXPECTED_FIXTURE_TAG}' tag.",
            "details": {
                "rows_processed": int(initial_rows),
                "rows_flagged": int(rows_flagged),
                "target_keywords": MULTI_MATCH_KEYWORDS
            }
        }
    
    def _check_date_time_format_integrity(self) -> Dict[str, Any]:
        """
        Audits specific date and time columns to check for data type inconsistencies 
        (e.g., numeric entries, invalid formats) that prevent UTC format conversion.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_DateTime_Format_Flag'
        
        # Define all six columns that need to be checked
        DATE_TIME_COLS_TO_CHECK = [
            'Date (UTC/GMT)', 'Date', 
            'Start (UTC)', 'End (UTC)', 
            'Start', 'End'
        ]
        
        self.df[FLAG_COLUMN] = 'OK'
        
        # 1. Check for required column existence
        if not all(col in self.df.columns for col in DATE_TIME_COLS_TO_CHECK):
            missing = list(set(DATE_TIME_COLS_TO_CHECK) - set(self.df.columns))
            return {
                "check_key": "check_datetime_format", "status": "Skipped",
                "action": "Date/Time Integrity Check", 
                "description": f"Skipped: Missing required columns: {missing}",
                "details": {"rows_flagged": 0}
            }

        # --- Data Type Check Loop ---
        
        total_flagged_rows = 0
        
        for col in DATE_TIME_COLS_TO_CHECK:
            
            # Determine if the column contains Date/DateTime or Time/Duration data
            if 'Start' in col or 'End' in col or 'Duration' in col:
                # For Time/Duration columns, check if they can be converted to timedelta
                # We must convert the column to string first to handle numeric entries like '11232'.
                try:
                    # pandas' to_timedelta can handle HH:MM:SS or large numbers (Excel format)
                    parsed_series = pd.to_timedelta(self.df[col].astype(str), errors='coerce')
                    error_mask = parsed_series.isna()
                except Exception:
                    # Fallback check if to_timedelta raises unexpected error
                    error_mask = self.df[col].astype(str).str.contains(r'[A-Za-z]', na=False) # Check for letters
            else:
                # For Date columns, check if they can be converted to datetime
                try:
                    # pandas' to_datetime can handle various date formats
                    parsed_series = pd.to_datetime(self.df[col], errors='coerce')
                    error_mask = parsed_series.isna()
                except Exception:
                    # Fallback check for general corruption
                    error_mask = self.df[col].astype(str).str.contains(r'[A-Za-z]', na=False) 
            
            
            # 2. Apply Flag to the BSR
            if error_mask.any():
                
                flag_message = f"FORMAT ERROR: '{col}' contains invalid or non-standard entries (e.g., numeric IDs, text). Requires manual cleanup."
                
                # Identify rows that failed the current check AND were not already flagged
                rows_to_flag = error_mask & (self.df[FLAG_COLUMN] == 'OK')
                
                self.df.loc[rows_to_flag, FLAG_COLUMN] = flag_message
                total_flagged_rows += rows_to_flag.sum()


        # 3. Final Summary
        return {
            "check_key": "check_datetime_format",
            "status": "Flagged" if total_flagged_rows > 0 else "Completed",
            "action": "Date/Time Integrity Check", 
            "description": f"Audited {len(DATE_TIME_COLS_TO_CHECK)} date/time columns. Flagged {total_flagged_rows} entries with invalid formatting.",
            "details": {
                "rows_flagged": int(total_flagged_rows),
                "columns_checked": DATE_TIME_COLS_TO_CHECK
            }
        }
    #Need to check the logic once agin 
    def _check_live_broadcast_uniqueness(self) -> Dict[str, Any]:
        """
        Implements a Channel Capacity Check: Ensures no two LIVE programs overlap 
        on the same Market/Channel ID/Time Slot, as a single channel cannot carry 
        multiple simultaneous feeds.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Channel_Capacity_Conflict_Flag' # Renamed flag for clarity
        
        # --- Define the Grouping Key for Channel Capacity ---
        # We rely on the time slot: Market, Channel ID, Start_DT, End_DT
        # NOTE: Since the time windows (Start/End) are the variables being checked, 
        # we cannot use them in the fixed GROUPING_KEY_COLS for the groupby operation.
        
        # We use a reduced, minimal grouping key to bring all simultaneous broadcasts together.
        CAPACITY_GROUPING_KEY = ['Market', 'Channel ID', 'Date (UTC/GMT)','Combined']
        
        LIVE_PROGRAM_TYPE = 'LIVE'
        
        REQUIRED_COLS = ['Market', 'Channel ID', 'Type of program', 'Date (UTC/GMT)', 'Start (UTC)', 'End (UTC)', 'Home Team', 'Away Team']
        if not all(col in self.df.columns for col in REQUIRED_COLS):
            return {"check_key": "check_live_broadcast_uniqueness", "status": "Skipped", "action": "Live Overlap Check", "description": "Skipped: Missing required BSR columns.", "details": {"rows_flagged": 0}}

        self.df[FLAG_COLUMN] = 'OK'
        
        # --- 1. Prepare Data and Timestamps (Handling midnight rollover) ---
        try:
            date_key = self.df['Date (UTC/GMT)'].astype(str).str[:10]
            self.df['Start_DT'] = pd.to_datetime(date_key + ' ' + self.df['Start (UTC)'].astype(str), errors='coerce')
            base_end_dt = pd.to_datetime(date_key + ' ' + self.df['End (UTC)'].astype(str), errors='coerce')
            rollover_mask = (base_end_dt < self.df['Start_DT']) & base_end_dt.notna()
            base_end_dt.loc[rollover_mask] += timedelta(days=1)
            self.df['End_DT'] = base_end_dt
            
        except Exception as e:
            self.df.drop(columns=['Start_DT', 'End_DT'], inplace=True, errors='ignore')
            return {"check_key": "check_live_broadcast_uniqueness", "status": "Failed", "action": "Live Overlap Check", "description": f"Failed to parse Date/Time columns: {e}", "details": {"rows_flagged": 0}}
            
        # Standardize grouping columns
        for col in ['Market', 'Channel ID']:
            self.df[col] = self.df[col].astype(str).str.strip().str.upper().str.replace(r'[^A-Z0-9\s\.\-]', '', regex=True).fillna('NAN')

        live_mask = self.df['Type of program'].astype(str).str.upper().str.strip() == LIVE_PROGRAM_TYPE
        
        df_live_candidates = self.df[live_mask].copy()
        
        # --- 2. Overlap Detection Logic (Grouping by Capacity Slot) ---
        
        conflict_details = {} 
        
        # Sort by the CAPACITY key and Start_DT
        df_live_candidates = df_live_candidates.sort_values(by=CAPACITY_GROUPING_KEY + ['Start_DT'])
        
        # Group by the CAPACITY SLOT (Market, Channel ID, Date)
        for key_tuple, group in df_live_candidates.groupby(CAPACITY_GROUPING_KEY):
            if len(group) < 2:
                continue
                
            lagged_end_dt = group['End_DT'].shift(1)
            overlap_start_mask = group['Start_DT'] < lagged_end_dt
            
            # Check if any overlap exists in this specific group
            if overlap_start_mask.any():
                
                current_overlap_indices = group[overlap_start_mask].index.tolist()
                preceding_overlap_indices = group[overlap_start_mask].shift(1).index.dropna().astype(int)

                all_conflict_indices = set(current_overlap_indices).union(set(preceding_overlap_indices))
                
                # Format the detailed conflict message for this group
                conflict_log = []
                
                for idx in sorted(list(all_conflict_indices)):
                    row = self.df.loc[idx]
                    
                    # Retrieve all relevant info for diagnosis
                    team_info = f"{row['Home Team']} vs {row['Away Team']}"
                    
                    log_entry = (f"Index {idx} | Fixture: {team_info} | "
                                f"Times: {row['Start (UTC)']} - {row['End (UTC)']}")
                    conflict_log.append(log_entry)
                
                # Key tuple contains: (Market, Channel ID, Date)
                key_id = "|".join([str(k) for k in key_tuple]) 
                
                conflict_message = f"CAPACITY CONFLICT: Channel Slot ({key_id}) has overlapping LIVE feeds. Conflicting slots: " + " || ".join(conflict_log)

                # Apply the SAME detailed message to ALL rows involved in this specific conflict group
                for idx in all_conflict_indices:
                    conflict_details[idx] = conflict_message
        

        # --- 3. Apply Flag to Original DataFrame ---
        rows_flagged = len(conflict_details)
        
        if rows_flagged > 0:
            flag_series = pd.Series(conflict_details)
            
            self.df.loc[flag_series.index, FLAG_COLUMN] = flag_series

        # Final cleanup of temporary columns in self.df
        self.df.drop(columns=['Start_DT', 'End_DT'], inplace=True, errors='ignore')

        # 4. Final Summary
        return {
            "check_key": "check_live_broadcast_uniqueness",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Channel Capacity Check", 
            "description": f"Flagged {rows_flagged} rows involved in a simultaneous live broadcast conflict.",
            "details": {
                "rows_flagged": int(rows_flagged),
                "uniqueness_key_components": CAPACITY_GROUPING_KEY
            }
        }
    
    def _audit_channel_line_item_count(self) -> Dict[str, Any]:
        """
        Calculates the total number of line items (programs) for each unique TV-Channel 
        in the BSR and returns this summary as a separate DataFrame for reporting.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Channel_Count_Audit_Flag'
        
        CHANNEL_COL = 'TV-Channel'
        
        if CHANNEL_COL not in self.df.columns:
            return {
                "check_key": "audit_channel_line_item_count", "status": "Skipped",
                "action": "Deliverable Count Audit", 
                "description": "Skipped: Missing required BSR 'TV-Channel' column.",
                "details": {"report_generated": False}
            }

        # Initialize the flag column for audit (optional, but good practice)
        self.df[FLAG_COLUMN] = 'OK' 

        # 1. Normalize and Calculate Counts
        
        # Normalize channel names for accurate grouping (UPPER/strip)
        channel_norm = self.df[CHANNEL_COL].astype(str).str.strip().str.upper()
        
        # Calculate the current line item count for each unique channel
        channel_counts_df = channel_norm.value_counts().reset_index()
        channel_counts_df.columns = ['TV-Channel_Norm', 'Program_Count']
        
        # Sort for better readability in the final report
        channel_counts_df = channel_counts_df.sort_values(by='Program_Count', ascending=False)
        
        # 2. Final Summary
        return {
            "check_key": "audit_channel_line_item_count",
            "status": "Completed",
            "action": "Deliverable Count Audit", 
            "description": f"Generated line item count summary for {len(channel_counts_df)} unique channels.",
            "details": {
                "total_channels": int(len(channel_counts_df)),
                "report_generated": True,
                # CRITICAL: Return the DataFrame itself for saving to a separate tab
                "channel_count_report_df": channel_counts_df.to_dict('records')
            }
        }

    def _check_combined_archive_status(self) -> Dict[str, Any]:
        """
        Audits the 'Combined' column for the keyword 'archive' and flags rows 
        as potential archival content requiring removal or review.
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Archive_Status_Flag'
        KEYWORD = 'ARCHIVE'
        COMBINED_COL = 'Combined'
        
        # Check for required column
        if COMBINED_COL not in self.df.columns:
            return {
                "check_key": "check_combined_archive_status", "status": "Skipped",
                "action": "Keyword Audit", 
                "description": "Skipped: Missing required BSR 'Combined' column.",
                "details": {"rows_flagged": 0}
            }

        self.df[FLAG_COLUMN] = 'OK'

        # 1. Normalize the Combined column for case-insensitive search
        combined_norm = self.df[COMBINED_COL].astype(str).str.upper()
        
        # 2. Identify rows containing the keyword
        archive_mask = combined_norm.str.contains(KEYWORD, na=False)
        
        rows_flagged = archive_mask.sum()
        
        # 3. Apply Flag to Original DataFrame
        if rows_flagged > 0:
            
            flag_message = f"ARCHIVAL CONTENT FLAG: Keyword '{KEYWORD}' found in Combined column. Requires review/removal."
            
            # Apply flag only to rows currently marked OK
            rows_to_flag = archive_mask & (self.df[FLAG_COLUMN] == 'OK')
            
            self.df.loc[rows_to_flag, FLAG_COLUMN] = flag_message

        # 4. Final Summary
        return {
            "check_key": "check_combined_archive_status",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Keyword Audit", 
            "description": f"Flagged {rows_flagged} rows containing the '{KEYWORD}' keyword in the Combined column.",
            "details": {
                "rows_flagged": int(rows_flagged),
                "target_keyword": KEYWORD
            }
        }

    def _suppress_duplicated_audience(self) -> Dict[str, Any]:
        """
        Audits the BSR to flag any row where the 'Source' column indicates a duplication 
        origin, but EITHER the Modeled Audience or the Metered Audience column contains 
        a positive, non-zero value (an anomaly).
        """
        initial_rows = len(self.df)
        FLAG_COLUMN = 'QC_Audience_Suppression_Flag'
        SOURCE_COL = 'Source'
        KEYWORD = 'DUPLICATED FROM BSA'
        
        # Define BOTH audience columns that must be zero
        TARGET_AUDIENCE_COLS = [
            'Aud. Estimates [\'000s]', 
            'Aud Metered (000s) 3+'
        ]
        
        # Check for required columns
        REQUIRED_COLS = TARGET_AUDIENCE_COLS + [SOURCE_COL]
        if not all(col in self.df.columns for col in REQUIRED_COLS):
            return {
                "check_key": "suppress_duplicated_audience", "status": "Skipped",
                "action": "Audience Suppression Audit", 
                "description": "Skipped: Missing required BSR columns.",
                "details": {"rows_flagged": 0}
            }

        self.df[FLAG_COLUMN] = 'OK'

        # 1. Identify rows that are duplication sources
        source_norm = self.df[SOURCE_COL].astype(str).str.upper()
        suppression_mask = source_norm.str.contains(KEYWORD, na=False)
        
        # 2. Identify the anomaly: Is there ANY positive audience value?
        
        # Check if ANY of the two target columns are greater than zero
        # Use fillna(0) to treat NaNs as zero, ensuring a safe comparison
        audience_check_df = self.df[TARGET_AUDIENCE_COLS].fillna(0)
        
        # Create a mask that is TRUE if AT LEAST ONE of the two columns is positive
        any_audience_positive_mask = (audience_check_df > 0).any(axis=1)
        
        # Final Error Mask: Duplication Source AND Any Positive Audience
        error_mask = suppression_mask & any_audience_positive_mask
        
        rows_flagged = error_mask.sum()
        
        # 3. Apply Flag to Original DataFrame (No Value Update)
        if rows_flagged > 0:
            
            flag_message = f"SUPPRESSION ANOMALY: Source indicates duplication origin ('{KEYWORD}'), but Audience is POSITIVE in Aud. Estimates or Aud Metered."
            
            # Apply flag only to rows currently marked OK
            rows_to_flag = error_mask & (self.df[FLAG_COLUMN] == 'OK')
            
            self.df.loc[rows_to_flag, FLAG_COLUMN] = flag_message

        # 4. Final Summary
        return {
            "check_key": "suppress_duplicated_audience",
            "status": "Flagged" if rows_flagged > 0 else "Completed",
            "action": "Audience Suppression Audit", 
            "description": f"Flagged {rows_flagged} duplication source rows containing positive audience values (should be zero).",
            "details": {
                "rows_flagged": int(rows_flagged),
                "target_columns_checked": TARGET_AUDIENCE_COLS
            }
        }
    
    def _filter_short_programs(self):
        """
        Removes programs where duration <5 minutes except Austria and New Zealand.
        Stores removed rows in: self.short_programs_df
        """
        MIN_DURATION = 5
        EXEMPT = ["AUSTRIA", "NEW ZEALAND"]

        df = self.df.copy()

        # Normalize markets
        df["Market_norm"] = df["Market"].astype(str).str.upper()

        # Parse start + end with date included
        df["Date_only"] = pd.to_datetime(df["Date"], errors="coerce").dt.date.astype(str)

        df["Start_DT"] = pd.to_datetime(
            df["Date_only"] + " " + df["Start (UTC)"].astype(str),
            errors="coerce"
        )
        df["End_DT_raw"] = pd.to_datetime(
            df["Date_only"] + " " + df["End (UTC)"].astype(str),
            errors="coerce"
        )

        # Handle past-midnight rollover
        rollover = df["End_DT_raw"] < df["Start_DT"]
        df.loc[rollover, "End_DT_raw"] += pd.Timedelta(days=1)

        # Compute duration in minutes
        df["Duration_Min"] = (df["End_DT_raw"] - df["Start_DT"]).dt.total_seconds() / 60

        remove_mask = (df["Duration_Min"] < MIN_DURATION) & (~df["Market_norm"].isin(EXEMPT))

        removed_df = df[remove_mask].copy()
        keep_df = df[~remove_mask].copy()

        # Clean temp cols
        for col in ["Market_norm", "Start_DT", "End_DT_raw", "Duration_Min", "Date_only"]:
            removed_df.drop(columns=col, inplace=True, errors="ignore")
            keep_df.drop(columns=col, inplace=True, errors="ignore")

        self.short_programs_df = removed_df
        self.df = keep_df

        return {
            "check_key": "filter_short_programs",
            "status": "Flagged" if len(removed_df) else "Completed",
            "description": f"{len(removed_df)} short programs removed (<5 min)",
            "details": {}
        }



# ----------------------------- ⚙️ Utility Functions (kept standalone) -----------------------------



    def color_excel(output_path, df):
        """Applies green/red coloring based on QC_OK columns."""
        wb = load_workbook(output_path)
        ws = wb.active
        headers = [cell.value for cell in ws[1]]
        col_map = {name: idx+1 for idx, name in enumerate(headers)}

        qc_columns = [col for col in df.columns if col.endswith("_OK")]

        for col_name in qc_columns:
            if col_name in col_map:
                col_idx = col_map[col_name]
                for row in range(2, ws.max_row + 1):
                    cell = ws.cell(row=row, column=col_idx)
                    val = cell.value
                    if val in [True, "True"]:
                        cell.fill = GREEN_FILL
                    elif val in [False, "False"]:
                        cell.fill = RED_FILL

        wb.save(output_path)


    def generate_summary_sheet(output_path, df):
        """Generates a summary sheet with pass/fail counts for QC checks."""
        wb = load_workbook(output_path)
        if "Summary" in wb.sheetnames: del wb["Summary"]
        ws = wb.create_sheet("Summary")

        qc_columns = [col for col in df.columns if "_OK" in col]
        summary_data = []
        for col in qc_columns:
            total = len(df)
            passed = df[col].sum() if df[col].dtype==bool else sum(df[col]=="True")
            summary_data.append([col, total, passed, total - passed])

        summary_df = pd.DataFrame(summary_data, columns=["Check", "Total", "Passed", "Failed"])
        for r in dataframe_to_rows(summary_df, index=False, header=True):
            ws.append(r)
        wb.save(output_path)

