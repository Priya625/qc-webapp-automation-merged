import pandas as pd


class BSAValidator:

    # ---------------------------------------------------------
    # INIT
    # ---------------------------------------------------------
    def __init__(self, rosco_file, bsa_report_file, bsr_df):
        self.rosco_file = rosco_file
        self.bsa_report_file = bsa_report_file
        self.bsr_df = bsr_df

    # ---------------------------------------------------------
    # NORMALIZATION
    # ---------------------------------------------------------
    @staticmethod
    def normalize(name):
        if pd.isna(name):
            return ""
        return (
            str(name)
            .lower()
            .replace("(", " ")
            .replace(")", " ")
            .replace("-", " ")
            .replace("_", " ")
            .replace(".", " ")
            .strip()
        )

    # ---------------------------------------------------------
    # MAIN FUNCTION
    # ---------------------------------------------------------
    def run_comparison(self):

        # =====================================================
        # 1️⃣ LOAD ALL SHEETS
        # =====================================================
        all_sheets = pd.read_excel(self.bsa_report_file, sheet_name=None)

        raw_df = all_sheets.get("Raw Data")
        bsa_list_df = all_sheets.get("BSA Channel List")
        aura_df = all_sheets.get("Aura Channels")
        flow_status_df = all_sheets.get("Channel Flow Status (J Column)")

        if raw_df is None:
            raise ValueError("Raw Data sheet missing in BSA file")

        # =====================================================
        # 2️⃣ EXTRACT BSA CHANNEL LIST
        # =====================================================
        bsa_channels = set()

        if bsa_list_df is not None:
            col = next(
                (c for c in bsa_list_df.columns if "channel" in c.lower()),
                None,
            )
            if col:
                bsa_channels = set(
                    bsa_list_df[col]
                    .dropna()
                    .astype(str)
                    .map(self.normalize)
                )

        # =====================================================
        # 3️⃣ EXTRACT AURA CHANNELS (COLUMN E)
        # =====================================================
        aura_channels = set()
        if aura_df is not None:
            if len(aura_df.columns) >= 5:
                aura_channels = set(
                    aura_df.iloc[:, 4]
                    .dropna()
                    .astype(str)
                    .map(self.normalize)
                )

        # =====================================================
        # 4️⃣ EXTRACT ROSCO CHANNELS
        # =====================================================
        rosco_df = pd.read_excel(self.rosco_file)
        rosco_channels = set(
            rosco_df.iloc[:, 0]
            .dropna()
            .astype(str)
            .map(self.normalize)
        )

        # =====================================================
        # 5️⃣ EXTRACT CHANNEL FLOW STATUS
        # =====================================================
        status_map = {}

        if flow_status_df is not None:
            flow_status_df["Channel Key"] = flow_status_df["Channel Key"].map(self.normalize)

            latest_week = flow_status_df["Week End Date"].max()

            latest_df = flow_status_df[
                flow_status_df["Week End Date"] == latest_week
            ]

            for _, row in latest_df.iterrows():
                status_map[row["Channel Key"]] = row["Final Status"]

        # =====================================================
        # 6️⃣ DAILY SCHEDULE CHECK FROM BSR FILE
        # =====================================================
        self.bsr_df["Date"] = pd.to_datetime(self.bsr_df["Date"])
        all_days = sorted(self.bsr_df["Date"].unique())

        self.bsr_df["TV Channel"] = self.bsr_df["TV Channel"].map(self.normalize)

        # =====================================================
        # 7️⃣ BUILD RESULT TABLE
        # =====================================================
        records = []

        for channel in bsa_channels:

            in_rosco = channel in rosco_channels
            in_aura = channel in aura_channels
            status = status_map.get(channel, "No Status Found")

            # Daily gap check
            for day in all_days:
                day_channels = set(
                    self.bsr_df[self.bsr_df["Date"] == day]["TV Channel"]
                )

                no_schedule = channel not in day_channels

                records.append(
                    {
                        "Date": day.strftime("%Y-%m-%d"),
                        "Channel": channel.upper(),
                        "In_ROSCO": "Yes" if in_rosco else "No",
                        "In_Aura": "Yes" if in_aura else "No",
                        "Channel_Status": status,
                        "Schedule_Present": "No" if no_schedule else "Yes",
                        "Severity": self.get_severity(
                            in_rosco, in_aura, no_schedule
                        ),
                    }
                )

        return pd.DataFrame(records)

    # ---------------------------------------------------------
    # SEVERITY LOGIC
    # ---------------------------------------------------------
    @staticmethod
    def get_severity(in_rosco, in_aura, no_schedule):

        if no_schedule and in_rosco:
            return "Critical – ROSCO Channel Missing Schedule"

        if no_schedule and in_aura:
            return "High – Aura Channel Missing Schedule"

        if no_schedule:
            return "Medium – BSA Channel Missing"

        return "OK"