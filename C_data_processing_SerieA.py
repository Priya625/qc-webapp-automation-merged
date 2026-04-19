import pandas as pd
import numpy as np
import os

import pandas as pd

class SerieAValidator:

    def __init__(self, df, duplicator_path=None, infront_path=None):

        self.df = df.copy()

        # Normalize BSR columns
        self.df.columns = (
            self.df.columns.astype(str)
            .str.strip().str.lower()
            .str.replace(" ", "_", regex=False)
        )

        self.results_log = []

        # ---------------- LOAD DUPLICATOR ----------------
        self.dup_df = None
        if duplicator_path:
            try:
                excel_file = pd.ExcelFile(duplicator_path)

                # Normalize all sheet names
                sheet_map = {s.lower().strip(): s for s in excel_file.sheet_names}

                if "data core" in sheet_map:
                    correct_sheet_name = sheet_map["data core"]
                    self.dup_df = pd.read_excel(duplicator_path, sheet_name=correct_sheet_name)
                    
                    self.results_log.append({
                        "check": "Duplicator Load",
                        "status": "Success",
                        "description": f"Loaded sheet: {correct_sheet_name}"
                    })

                else:
                    self.results_log.append({
                        "check": "Duplicator Load",
                        "status": "Error",
                        "description": f"'Data Core' sheet not found. Available sheets: {excel_file.sheet_names}"
                    })

            except Exception as e:
                self.results_log.append({
                    "check": "Duplicator Load",
                    "status": "Error",
                    "description": f"Failed to load duplicator file: {str(e)}"
                })

        # ---------------- LOAD INFRONT ----------------
        self.infront_df = None
        if infront_path:
            try:
                self.infront_df = infront_path
            except Exception as e:
                self.results_log.append({
                    "check": "Infront Load",
                    "status": "Error",
                    "description": f"Failed to load infront file: {str(e)}"
                })
        

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

        check_name = "Duplicator Mapping"

        if self.dup_df is None:
            self.results_log.append({
                "check": check_name,
                "status": "Skipped",
                "description": "Duplicator file not provided"
            })
            return

        dup_df = self.dup_df.copy()

        # ---------------- CLEAN COLUMN NAMES ----------------
        dup_df.columns = (
            dup_df.columns.astype(str)
            .str.strip().str.lower()
            .str.replace(" ", "_", regex=False)
        )

        # ---------------- DEBUG PRINT ----------------
        print("Duplicator columns:", dup_df.columns.tolist())

        required_cols = ["orig_market", "orig_channel", "dup_market", "dup_channel"]

        missing = [c for c in required_cols if c not in dup_df.columns]

        if missing:
            self.results_log.append({
                "check": check_name,
                "status": "Error",
                "description": f"Missing columns: {missing}"
            })
            return

        # ---------------- BUILD LOOKUP ----------------
        orig_set = set(zip(
            dup_df["orig_market"].astype(str).str.lower().str.strip(),
            dup_df["orig_channel"].astype(str).str.lower().str.strip()
        ))

        dup_set = set(zip(
            dup_df["dup_market"].astype(str).str.lower().str.strip(),
            dup_df["dup_channel"].astype(str).str.lower().str.strip()
        ))

        # ---------------- APPLY CHECK ----------------
        result_col = []
        remarks_col = []

        for _, row in self.df.iterrows():

            key = (
                str(row.get("market", "")).lower().strip(),
                str(row.get("channel", "")).lower().strip()
            )

            if key in orig_set:
                result_col.append("TRUE")
                remarks_col.append("Matched in Orig Mapping")

            elif key in dup_set:
                result_col.append("TRUE")
                remarks_col.append("Matched in Dup Mapping")

            else:
                result_col.append("FALSE")
                remarks_col.append("No mapping found")

        # ---------------- ADD TO OUTPUT ----------------
        self.df["duplicator_check_result"] = result_col
        self.df["duplicator_check_remarks"] = remarks_col

        # ---------------- SUMMARY ----------------
        passed = result_col.count("TRUE")
        failed = result_col.count("FALSE")

        self.results_log.append({
            "check": check_name,
            "status": "Completed",
            "description": f"{passed} passed, {failed} failed"
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
        check_key = "exclude_pre_post_programs"

        possible_columns = ["combined", "program_title", "title"]
        target_column = None

        for col in possible_columns:
            if col in self.df.columns:
                target_column = col
                break

        if target_column:
            mask = self.df[target_column].astype(str).str.contains(
                r"PRE|POST|P\.MATCH|P-MATCH",
                case=False,
                na=False
            )
            removed_count = mask.sum()
            self.df = self.df[~mask]

            self.results_log.append({
                "check_key": check_key,
                "status": "Success",
                "description": f"Excluded {removed_count} Pre/Post lines using '{target_column}'."
            })
        else:
            self.results_log.append({
                "check_key": check_key,
                "status": "Warning",
                "description": "No suitable column found for Pre/Post exclusion."
            })

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