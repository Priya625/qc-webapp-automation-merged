import pandas as pd
import numpy as np
import os

class SerieAValidator:
    def __init__(self, df, duplicator_path=None, infront_path=None):
        # We use .copy() to ensure we don't modify the original dataframe unexpectedly
        self.df = df.copy()
        self.duplicator_path = duplicator_path
        self.infront_path = infront_path
        self.results_log = []

    def market_check_processor(self, active_checks):
        """Dispatches the selected checks to their respective functions."""
        
        # This map connects the keys in Streamlit to the functions below
        check_map = {
            "check_missing_duplicator_data": self.check_missing_duplicator_data,
            "compare_audience_trends": self.compare_audience_trends,
            "consolidation_check": self.consolidation_check,
            "filter_irrelevant_data": self.filter_irrelevant_data,
            "exclude_pre_post_programs": self.exclude_pre_post_programs,
            "remove_identical_broadcasts": self.remove_identical_broadcasts,
            "upload_issue_audit": self.upload_issue_audit
        }

        for check_key in active_checks:
            if check_key in check_map:
                try:
                    check_map[check_key]()
                except Exception as e:
                    self.results_log.append({
                        "check_key": check_key,
                        "status": "Error",
                        "description": f"Function failed: {str(e)}"
                    })
        
        return self.results_log

    # --- S.NO 1: Market Duplicator Check ---
    def check_missing_duplicator_data(self):
        """
        Ensures that markets defined as 'Duplicated Markets'
        have at least one broadcast line in final output.
        """

        check_key = "check_missing_duplicator_data"

        # -----------------------------
        # 1. Validate reference file
        # -----------------------------
        if not self.duplicator_path or not os.path.exists(self.duplicator_path):
            self.results_log.append({
                "check_key": check_key,
                "status": "Error",
                "description": "Duplicated markets reference file not found."
            })
            return

        # -----------------------------
        # 2. Read duplicated markets sheet
        # -----------------------------
        try:
            dup_df = pd.read_excel(self.duplicator_path, sheet_name=0)
        except Exception as e:
            self.results_log.append({
                "check_key": check_key,
                "status": "Error",
                "description": f"Failed to read duplicated markets sheet: {str(e)}"
            })
            return

        # Normalize column names
        dup_df.columns = dup_df.columns.str.strip().str.lower()
        self.df.columns = self.df.columns.str.strip().str.lower()

        required_dup_cols = {"market", "channel"}
        required_main_cols = {"market", "channel"}

        if not required_dup_cols.issubset(dup_df.columns):
            self.results_log.append({
                "check_key": check_key,
                "status": "Error",
                "description": "Duplicated markets sheet missing Market / Channel columns."
            })
            return

        if not required_main_cols.issubset(self.df.columns):
            self.results_log.append({
                "check_key": check_key,
                "status": "Error",
                "description": "Main dataset missing Market / Channel columns."
            })
            return

        # -----------------------------
        # 3. Expected duplicated outputs
        # -----------------------------
        expected_pairs = (
            dup_df[["market", "channel"]]
            .dropna()
            .drop_duplicates()
        )

        # -----------------------------
        # 4. Actual output presence
        # -----------------------------
        actual_pairs = (
            self.df[["market", "channel"]]
            .dropna()
            .drop_duplicates()
        )

        # -----------------------------
        # 5. Identify missing duplicated markets
        # -----------------------------
        merged = expected_pairs.merge(
            actual_pairs,
            on=["market", "channel"],
            how="left",
            indicator=True
        )

        missing_df = merged[merged["_merge"] == "left_only"]

        # -----------------------------
        # 6. Log results
        # -----------------------------
        if missing_df.empty:
            self.results_log.append({
                "check_key": check_key,
                "status": "Success",
                "description": "All duplicated markets have broadcast output."
            })
        else:
            examples = missing_df.head(5).to_dict(orient="records")

            self.results_log.append({
                "check_key": check_key,
                "status": "Warning",
                "description": (
                    f"{len(missing_df)} duplicated market/channel combinations "
                    f"have no broadcast output. Examples: {examples}"
                )
            })

    # --- S.NO 2: Audience Trend Analysis ---
    def compare_audience_trends(self):
        check_key = "compare_audience_trends"

        required_cols = {
            "season",
            "market",
            "channel",
            "mat_country_id",
            "channel_id",
            "start_time",
            "audience"
        }

        if not required_cols.issubset(self.df.columns):
            self.results_log.append({
                "check_key": check_key,
                "status": "Error",
                "description": (
                    "Required columns missing for season-level audience trend check. "
                    "Expected MAT Country ID, Channel ID, Start Time, Audience."
                )
            })
            return

        # Ensure datetime
        self.df["start_time"] = pd.to_datetime(self.df["start_time"], errors="coerce")

        # -------------------------------------------------
        # 1. Define BC line using MAT Country + Channel + Time
        # -------------------------------------------------
        self.df["bc_line_key"] = (
            self.df["mat_country_id"].astype(str) + "_" +
            self.df["channel_id"].astype(str) + "_" +
            self.df["start_time"].astype(str)
        )

        # -------------------------------------------------
        # 2. Aggregate at Season level
        # -------------------------------------------------
        season_summary = (
            self.df
            .groupby(["season", "market", "channel"])
            .agg(
                total_audience=("audience", "sum"),
                bc_lines=("bc_line_key", "nunique")
            )
            .reset_index()
        )

        # -------------------------------------------------
        # 3. Pivot Last vs Current Season
        # -------------------------------------------------
        pivot = season_summary.pivot_table(
            index=["market", "channel"],
            columns="season",
            values=["total_audience", "bc_lines"]
        )

        if pivot.shape[1] < 4:
            self.results_log.append({
                "check_key": check_key,
                "status": "Warning",
                "description": "Insufficient season data to compare audience trends."
            })
            return

        pivot.columns = ["_".join(map(str, col)) for col in pivot.columns]
        pivot = pivot.dropna()

        # -------------------------------------------------
        # 4. Compute percentage changes
        # -------------------------------------------------
        pivot["audience_change_pct"] = (
            (pivot.iloc[:, 0] - pivot.iloc[:, 2]) /
            pivot.iloc[:, 2].replace(0, np.nan)
        ).abs() * 100

        pivot["bc_line_change_pct"] = (
            (pivot.iloc[:, 1] - pivot.iloc[:, 3]) /
            pivot.iloc[:, 3].replace(0, np.nan)
        ).abs() * 100

        # -------------------------------------------------
        # 5. Flag illogical movements
        # -------------------------------------------------
        flagged = pivot[
            (pivot["audience_change_pct"] >= 30) &
            (pivot["bc_line_change_pct"] <= 10)
        ]

        if flagged.empty:
            self.results_log.append({
                "check_key": check_key,
                "status": "Success",
                "description": "Season-level audience trends align with BC line movement."
            })
        else:
            examples = flagged.reset_index().head(5).to_dict(orient="records")
            self.results_log.append({
                "check_key": check_key,
                "status": "Warning",
                "description": (
                    f"{len(flagged)} market/channel combinations show "
                    f"audience variance not supported by BC line change. "
                    f"Examples: {examples}"
                )
            })

    # --- S.NO 3: Consolidation Check ---
    def consolidation_check(self):

        check_key = "consolidation_check"

        required = {"market", "channel", "program_title", "start_time"}
        missing = required - set(self.df.columns)

        if missing:
            self.results_log.append({
                "check_key": check_key,
                "status": "Info",
                "description": (
                    "Consolidation check skipped. "
                    f"Missing columns: {sorted(missing)}"
                )
            })
            return

        self.df["start_time"] = pd.to_datetime(self.df["start_time"], errors="coerce")

        grouped = (
            self.df
            .groupby(["market", "channel", "program_title"])
            .size()
            .reset_index(name="line_count")
        )

        splits = grouped[grouped["line_count"] > 1]

        if splits.empty:
            self.results_log.append({
                "check_key": check_key,
                "status": "Success",
                "description": "No programs appear split across multiple lines."
            })
        else:
            self.results_log.append({
                "check_key": check_key,
                "status": "Warning",
                "description": (
                    f"{len(splits)} programs appear split and may need consolidation. "
                    f"Examples: {splits.head(5).to_dict(orient='records')}"
                )
            })

    # --- S.NO 4: Irrelevant Data Filter ---
    def filter_irrelevant_data(self):
        check_key = "filter_irrelevant_data"

        if not self.infront_path:
            self.results_log.append({
                "check_key": check_key,
                "status": "Warning",
                "description": "Infront reference not provided."
            })
            return

        ref = pd.read_excel(self.infront_path)
        ref.columns = ref.columns.str.lower()

        if "start_date" not in ref.columns or "end_date" not in ref.columns:
            self.results_log.append({
                "check_key": check_key,
                "status": "Error",
                "description": "Monitoring range missing in Infront reference."
            })
            return

        start, end = ref["start_date"].min(), ref["end_date"].max()

        self.df["start_time"] = pd.to_datetime(self.df["start_time"], errors="coerce")
        mask = (self.df["start_time"] < start) | (self.df["start_time"] > end)

        removed = mask.sum()
        self.df = self.df[~mask]

        self.results_log.append({
            "check_key": check_key,
            "status": "Success",
            "description": f"Removed {removed} lines outside monitoring range."
        })

    # --- S.NO 5: Pre & Post Programs Exclusion ---
    def exclude_pre_post_programs(self):
        # Initial logic to remove rows containing Pre/Post keywords in 'Combined' column
        if 'Combined' in self.df.columns:
            mask = self.df['Combined'].str.contains('PRE|POST|P.MATCH|P-MATCH', case=False, na=False)
            removed_count = mask.sum()
            self.df = self.df[~mask]
            self.results_log.append({"check_key": "exclude_pre_post_programs", "status": "Success", "description": f"Excluded {removed_count} Pre/Post lines."})
        else:
            self.results_log.append({"check_key": "exclude_pre_post_programs", "status": "Warning", "description": "Column 'Combined' not found."})

    # --- S.NO 6: Duplication Check (Identical Lines) ---
    def remove_identical_broadcasts(self):
        check_key = "remove_identical_broadcasts"

        required_cols = {
            "market", "channel", "program_title",
            "start_time", "duration", "source"
        }
        if not required_cols.issubset(self.df.columns):
            self.results_log.append({
                "check_key": check_key,
                "status": "Error",
                "description": "Required columns missing for duplication check."
            })
            return

        before = len(self.df)

        self.df["norm_channel"] = self.df["channel"].str.lower().str.strip()
        self.df["norm_title"] = self.df["program_title"].str.lower().str.strip()
        self.df["start_time"] = pd.to_datetime(self.df["start_time"], errors="coerce")

        self.df = self.df.sort_values(by=["source"])

        self.df = self.df.drop_duplicates(
            subset=["market", "norm_channel", "norm_title", "start_time", "duration"],
            keep="first"
        )

        removed = before - len(self.df)

        self.results_log.append({
            "check_key": check_key,
            "status": "Success",
            "description": f"Removed {removed} duplicate broadcast lines."
        })


    # --- S.NO 7: Upload Issues Audit ---
    def upload_issue_audit(self):
        self.results_log.append({"check_key": "upload_issue_audit", "status": "Initialized", "description": "Waiting for logic implementation 1"})