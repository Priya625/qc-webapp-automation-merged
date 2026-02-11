import pandas as pd

class BSAValidator:
    def __init__(self, rosco_path, bsa_report_path, bsr_df):
        self.rosco_path = rosco_path
        self.bsa_report_path = bsa_report_path
        self.df = bsr_df
        self.flags_df = pd.DataFrame()

    def run_comparison(self):
        # 1. Load Reference Channels from BSA GSheet
        # We assume sheets might be named 'BSA_Channel_List' or 'Aura Channels'
        bsa_ref = pd.read_excel(self.bsa_report_path, sheet_name=None)
        ref_channels = set()
        for sheet in bsa_ref.values():
            # Try to find a channel column dynamically
            col = next((c for c in sheet.columns if 'channel' in c.lower()), None)
            if col:
                ref_channels.update(sheet[col].astype(str).str.strip().str.lower().unique())

        # 2. Load ROSCO Channels
        rosco_df = pd.read_excel(self.rosco_path)
        rosco_channels = set(rosco_df['TV Channel'].astype(str).str.strip().str.lower().unique())

        # 3. Check for Schedule Gaps in BSR
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        all_days = sorted(self.df['Date'].unique())
        
        gaps = []
        for day in all_days:
            daily_channels = set(self.df[self.df['Date'] == day]['TV Channel'].astype(str).str.strip().str.lower())
            
            for channel in ref_channels:
                if channel not in daily_channels:
                    is_in_rosco = channel in rosco_channels
                    gaps.append({
                        "Date": day.strftime('%Y-%m-%d'),
                        "Channel": channel.upper(),
                        "In_Rosco": "Yes" if is_in_rosco else "No",
                        "Issue": "No Schedule Found",
                        "Severity": "Critical" if is_in_rosco else "Warning"
                    })
        
        self.flags_df = pd.DataFrame(gaps)
        return self.flags_df