import pandas as pd
import numpy as np
from datetime import datetime

class BSAValidator:
    def __init__(self, rosco_path, bsa_report_path, bsr_df):
        self.rosco_path = rosco_path
        self.bsa_report_path = bsa_report_path
        self.df = bsr_df  # This is the main BSR data
        self.results = []
        self.missing_in_rosco = []

    def run_consistency_check(self):
        """
        Main execution method for the BSA Channel Consistency Check.
        """
        # 1. Extract Reference Channels from Weekly BSA Reporting GSheet
        # We look for common sheets like 'BSA_Channel_List' or 'Aura Channels'
        ref_channels = self._extract_reference_channels()
        
        # 2. Extract ROSCO Channels
        rosco_channels = self._extract_rosco_channels()
        
        # 3. Find channels in Reference but missing from ROSCO
        self.missing_in_rosco = [c for c in ref_channels if c not in rosco_channels]

        # 4. Flag missing schedules in BSR for all reference channels
        # Ensure BSR date is datetime
        if 'Date' in self.df.columns:
            self.df['Date'] = pd.to_datetime(self.df['Date'])
            all_days = self.df['Date'].unique()
            
            flag_results = []
            for day in all_days:
                # Get channels that actually have data on this day
                daily_data = self.df[self.df['Date'] == day]
                scheduled_channels = set(daily_data['TV Channel'].astype(str).str.strip().str.lower().unique())
                
                for bsa_ch in ref_channels:
                    if bsa_ch not in scheduled_channels:
                        # Determine if this is a critical channel (present in ROSCO) or just a reference warning
                        is_in_rosco = bsa_ch in rosco_channels
                        flag_results.append({
                            "Date": pd.to_datetime(day).strftime('%Y-%m-%d'),
                            "Channel": bsa_ch.upper(),
                            "Status": "Missing Schedule" if is_in_rosco else "Reference Missing",
                            "Severity": "CRITICAL" if is_in_rosco else "WARNING",
                            "Remark": "Exclusive BSA channel with no logs for this day" if is_in_rosco else "Channel in BSA list but not in ROSCO"
                        })
            
            self.results = pd.DataFrame(flag_results)
        
        return self.results, self.missing_in_rosco

    def _extract_reference_channels(self):
        """Helper to combine channels from all relevant sheets in the BSA GSheet."""
        all_sheets = pd.read_excel(self.bsa_report_path, sheet_name=None)
        ref_set = set()
        
        # Common column names in your uploaded snippets: 'Channel Name', 'TV-Channel', 'Channel'
        target_cols = ['Channel Name', 'TV-Channel', 'Channel', 'Channels']
        
        for sheet_name, df in all_sheets.items():
            for col in target_cols:
                if col in df.columns:
                    channels = df[col].dropna().astype(str).str.strip().str.lower().unique()
                    ref_set.update(channels)
        return ref_set

    def _extract_rosco_channels(self):
        """Helper to get channels from ROSCO."""
        rosco_df = pd.read_excel(self.rosco_path)
        # Assuming standard 'TV Channel' column in ROSCO
        if 'TV Channel' in rosco_df.columns:
            return set(rosco_df['TV Channel'].astype(str).str.strip().str.lower().unique())
        return set()