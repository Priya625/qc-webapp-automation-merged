import pandas as pd
import numpy as np
import os

class SerieAValidator:

    def __init__(self, df, duplicator_path=None, infront_path=None):

        self.df = df.copy()

        # Normalize BSR columns
        self.df.columns = (
            self.df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
        )

        self.results_log = []

        # ---------------- LOAD DUPLICATOR ----------------
        self.dup_df = None

        if duplicator_path:
            try:
                excel_file = pd.ExcelFile(duplicator_path)

                # DEBUG
                print("Available sheets:", excel_file.sheet_names)

                # Try different header rows automatically
                for header_row in [0, 1, 2, 3]:
                    try:
                        temp_df = pd.read_excel(
                            duplicator_path,
                            sheet_name="Data Core",
                            header=header_row
                        )

                        temp_df.columns = (
                            temp_df.columns.astype(str)
                            .str.strip()
                            .str.lower()
                            .str.replace(" ", "_")
                        )

                        if "orig_market" in temp_df.columns:
                            self.dup_df = temp_df
                            print(f"✅ Correct header found at row {header_row}")
                            break

                    except:
                        continue

                if self.dup_df is None:
                    raise Exception("Could not detect correct header row")

                self.results_log.append({
                    "check": "Duplicator Load",
                    "status": "Success",
                    "description": "Loaded sheet: Data Core"
                })

            except Exception as e:
                self.results_log.append({
                    "check": "Duplicator Load",
                    "status": "Error",
                    "description": str(e)
                })

        # ---------------- LOAD INFRONT ----------------
        self.infront_df = None
        if infront_path:
            try:
                self.infront_df = pd.read_excel(infront_path)
            except Exception as e:
                self.results_log.append({
                    "check": "Infront Load",
                    "status": "Error",
                    "description": str(e)
                })

    # =========================================================
    # MAIN PROCESSOR
    # =========================================================
    def market_check_processor(self, active_checks):

        check_map = {
            "check_missing_duplicator_data": self.check_missing_duplicator_data
        }

        for check_key in active_checks:
            if check_key in check_map:
                try:
                    check_map[check_key]()
                except Exception as e:
                    self.results_log.append({
                        "check": check_key,
                        "status": "Error",
                        "description": str(e)
                    })

        return self.results_log

    # =========================================================
    # CHECK 1: DUPLICATOR VALIDATION
    # =========================================================
    def check_missing_duplicator_data(self):

        check_name = "Duplicator Mapping"

        if self.dup_df is None:
            self.results_log.append({
                "check": check_name,
                "status": "Skipped",
                "description": "Duplicator not loaded"
            })
            return

        dup_df = self.dup_df.copy()

        required_cols = ["orig_market", "orig_channel", "dup_market", "dup_channel"]

        missing = [c for c in required_cols if c not in dup_df.columns]

        if missing:
            self.results_log.append({
                "check": check_name,
                "status": "Error",
                "description": f"Missing columns: {missing}"
            })
            return

        # ---------------- CLEAN ----------------
        dup_df = dup_df.dropna(subset=required_cols)

        dup_df["orig_market"] = dup_df["orig_market"].astype(str).str.lower().str.strip()
        dup_df["orig_channel"] = dup_df["orig_channel"].astype(str).str.lower().str.strip()
        dup_df["dup_market"] = dup_df["dup_market"].astype(str).str.lower().str.strip()
        dup_df["dup_channel"] = dup_df["dup_channel"].astype(str).str.lower().str.strip()

        # ---------------- CREATE LOOKUPS ----------------
        orig_pairs = set(zip(dup_df["orig_market"], dup_df["orig_channel"]))
        dup_pairs = set(zip(dup_df["dup_market"], dup_df["dup_channel"]))

        all_valid_pairs = orig_pairs.union(dup_pairs)

        all_markets = set(dup_df["orig_market"]).union(set(dup_df["dup_market"]))
        all_channels = set(dup_df["orig_channel"]).union(set(dup_df["dup_channel"]))

        # ---------------- APPLY CHECK ----------------
        results = []
        remarks = []

        for _, row in self.df.iterrows():

            market = str(row.get("market", "")).lower().strip()
            channel = str(row.get("channel", "")).lower().strip()

            key = (market, channel)

            # ✅ TRUE CASE
            if key in all_valid_pairs:
                results.append("TRUE")
                remarks.append("")

            # ❌ FALSE CASES
            else:
                results.append("FALSE")

                if market in all_markets and channel not in all_channels:
                    remarks.append("Market exists but channel not mapped in duplicator")

                elif channel in all_channels and market not in all_markets:
                    remarks.append("Channel exists but market not mapped in duplicator")

                elif market in all_markets and channel in all_channels:
                    remarks.append("Market & Channel exist but not mapped together")

                else:
                    remarks.append("Market & Channel both not found in duplicator")

        # ---------------- WRITE RESULT ----------------
        self.df["duplicator_check"] = results
        self.df["duplicator_remark"] = remarks

        # ---------------- SUMMARY ----------------
        passed = results.count("TRUE")
        failed = results.count("FALSE")

        self.results_log.append({
            "check": check_name,
            "status": "Completed",
            "passed": passed,
            "failed": failed
        })

    # =========================================================
    # ✅ CHECK 2: AUDIENCE TREND
    # =========================================================

    def compare_audience_trends(self):
        check_name = "Audience Trend Check"
        df = self.df.copy()
        # ---------------- CREATE SEASON ----------------
        df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
        df["season"] = df["start_time"].dt.year
        required_cols = {
            "season", "market", "channel",
            "mat_country_id", "channel_id",
            "start_time", "audience"
        }
        if not required_cols.issubset(df.columns):
            self.df["audience_trend_check"] = "ERROR"
            self.df["audience_trend_remark"] = "Missing required columns"
            return
        # ---------------- CHECK SEASON COUNT ----------------
        if df["season"].nunique() < 2:
            self.df["audience_trend_check"] = "SKIPPED"
            self.df["audience_trend_remark"] = "Only one season present"
            return

        # ---------------- BC LINE ----------------

        df["bc_line"] = (
            df["mat_country_id"].astype(str) + "_" +
            df["channel_id"].astype(str) + "_" +
            df["start_time"].astype(str)
        )

        # ---------------- AGGREGATE ----------------

        summary = (
            df.groupby(["season", "market", "channel"])
            .agg(
                audience=("audience", "sum"),
                bc_lines=("bc_line", "nunique")
            )
            .reset_index()
        )
        pivot = summary.pivot_table(
            index=["market", "channel"],
            columns="season",
            values=["audience", "bc_lines"]
        )
        pivot = pivot.dropna()
        if pivot.shape[1] < 4:
            self.df["audience_trend_check"] = "SKIPPED"
            self.df["audience_trend_remark"] = "Not enough comparable seasons"
            return
        pivot.columns = ["_".join(map(str, col)) for col in pivot.columns]
        # ---------------- CHANGE % ----------------
        pivot["aud_change"] = (
            (pivot.iloc[:, 0] - pivot.iloc[:, 2]) /
            pivot.iloc[:, 2].replace(0, np.nan)
        ).abs() * 100
        pivot["bc_change"] = (
            (pivot.iloc[:, 1] - pivot.iloc[:, 3]) /
            pivot.iloc[:, 3].replace(0, np.nan)
        ).abs() * 100
        flagged = pivot[
            (pivot["aud_change"] >= 30) &
            (pivot["bc_change"] <= 10)
        ]
        # ---------------- MAP BACK ----------------
        flag_map = {
            (row["market"], row["channel"]): True
            for _, row in flagged.reset_index().iterrows()
        }
        results = []
        remarks = []
        for _, row in self.df.iterrows():
            key = (row["market"], row["channel"])
            if key in flag_map:
                results.append("FALSE")
                remarks.append("Audience spike without BC support")
            else:
                results.append("TRUE")
                remarks.append("")
        self.df["audience_trend_check"] = results
        self.df["audience_trend_remark"] = remarks
        self.results_log.append({
            "check": check_name,
            "status": "Completed"
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