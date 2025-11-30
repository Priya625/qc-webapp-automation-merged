import re
import os
import pandas as pd
import numpy as np
import logging
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

# Removed logging.basicConfig - it's now handled by app.py
DATE_FORMAT = "%Y-%m-%d"

GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
HEADER_FILL = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")


# ----------------------------- Helpers -----------------------------
def _find_column(df, candidates):
    """
    Case-insensitive lookup for a column in df.columns.
    candidates: list of possible header names (strings) from config.
    Returns first matching actual column name or None.
    """
    if not isinstance(candidates, list):
        candidates = [candidates] # Handle single-string entries
        
    lower_map = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand is None:
            continue
        key = cand.lower().strip()
        if key in lower_map:
            return lower_map[key]
    return None


def _is_present(val):
    """
    Treat numeric values (including 0) as present.
    For strings: strip whitespace and consider 'nan'/'none' as absent.
    None/NaN -> False.
    """
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except Exception:
        pass
    # Numeric -> present (including 0)
    if isinstance(val, (int, float)) and not (isinstance(val, float) and pd.isna(val)):
        return True
    s = str(val).strip()
    if s == "":
        return False
    if s.lower() in ("nan", "none"):
        return False
    return True

def parse_duration_to_minutes(duration_series):
    results = []
    for item in duration_series:
        if pd.isna(item):
            results.append(np.nan)
            continue
        if isinstance(item, (int, float)):
            results.append(float(item))
            continue
        s = str(item).strip()
        try:
            num = float(s)
            results.append(num)
            continue
        except ValueError:
            pass
        parts = s.split(':')
        if len(parts) >= 2:
            try:
                hours = float(re.sub(r"[^0-9.]", "", parts[0]))
                minutes = float(re.sub(r"[^0-9.]", "", parts[1]))
                seconds = 0.0
                if len(parts) >= 3:
                    seconds = float(re.sub(r"[^0-9.]", "", parts[2]))
                total_minutes = (hours * 60) + minutes + (seconds / 60)
                results.append(total_minutes)
            except (ValueError, IndexError):
                results.append(np.nan)
        else:
            results.append(np.nan)
    return pd.Series(results, index=duration_series.index)


# ----------------------------- 1️⃣ Detect Monitoring Period -----------------------------
def detect_period_from_rosco(rosco_path):
    """
    Attempts to find 'Monitoring Period' row anywhere in the Rosco file and extract two dates (YYYY-MM-DD).
    Returns (start_date, end_date) as pandas.Timestamp.
    Raises ValueError if not found or parsed.
    """
    # This function is heuristic-based and doesn't need config
    x = pd.read_excel(rosco_path, header=None, dtype=str)
    combined_text = x.fillna("").astype(str).apply(lambda row: " ".join(row.values), axis=1)
    match_rows = combined_text[combined_text.str.contains("Monitoring Period", case=False, na=False)]
    if match_rows.empty:
        match_rows = combined_text[combined_text.str.contains("Monitoring Periods|Monitoring period", case=False, na=False)]
    if match_rows.empty:
        all_text = " ".join(combined_text.tolist())
        found = re.findall(r"\d{4}-\d{2}-\d{2}", all_text)
        if len(found) >= 2:
            start_date = pd.to_datetime(found[0], format=DATE_FORMAT)
            end_date = pd.to_datetime(found[1], format=DATE_FORMAT)
            return start_date, end_date
        raise ValueError("Could not find 'Monitoring Period' text in Rosco file.")

    text_row = match_rows.iloc[0]
    found = re.findall(r"\d{4}-\d{2}-\d{2}", text_row)
    if len(found) >= 2:
        start_date = pd.to_datetime(found[0], format=DATE_FORMAT)
        end_date = pd.to_datetime(found[1], format=DATE_FORMAT)
        return start_date, end_date

    found_alt = re.findall(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", text_row)
    if len(found_alt) >= 2:
        try:
            start_date = pd.to_datetime(found_alt[0], dayfirst=False, errors="coerce")
            end_date = pd.to_datetime(found_alt[1], dayfirst=False, errors="coerce")
            if pd.notna(start_date) and pd.notna(end_date):
                return start_date, end_date
        except Exception:
            pass

    raise ValueError("Could not parse monitoring period dates from Rosco file.")


# ----------------------------- 2️⃣ Load BSR -----------------------------
def detect_header_row(bsr_path, bsr_cols):
    df_sample = pd.read_excel(bsr_path, header=None, nrows=200)
    
    # Use config columns to find the header
    key_cols = [
        bsr_cols.get('market', ['market'])[0],
        bsr_cols.get('tv_channel', ['channel'])[0],
        bsr_cols.get('date', ['date'])[0],
        bsr_cols.get('start_time', ['start'])[0]
    ]
    
    for i, row in df_sample.iterrows():
        row_str = " ".join(row.dropna().astype(str).tolist()).lower()
        # Find row that contains several key column names
        if sum(col.lower() in row_str for col in key_cols) >= 2:
            return i
            
    raise ValueError("Could not detect header row in BSR file.")


def load_bsr(bsr_path, bsr_cols):
    header_row = detect_header_row(bsr_path, bsr_cols)
    df = pd.read_excel(bsr_path, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ----------------------------- 3️⃣ Period Check -----------------------------
def period_check(df, start_date, end_date, bsr_cols):
    
    date_col = _find_column(df, bsr_cols.get('date', ['date']))
    
    if not date_col:
        logging.warning("Period Check: 'date' column not found.")
        df["Within_Period_OK"] = False
        df["Within_Period_Remark"] = "Date column not found"
        return df
        
    df["Date_checked"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df["Within_Period_OK"] = df["Date_checked"].between(start_date.date(), end_date.date())
    df["Within_Period_Remark"] = df["Within_Period_OK"].apply(lambda x: "" if x else "Date outside monitoring period")
    df = df.drop(columns=["Date_checked"], errors="ignore")
    return df


# ----------------------------- 4️⃣ Completeness Check -----------------------------
def completeness_check(df, bsr_cols, rules):
    
    # --- Map logical names to actual columns (from config) ---
    colmap = {
        "tv_channel": _find_column(df, bsr_cols['tv_channel']),
        "channel_id": _find_column(df, bsr_cols['channel_id']),
        "type_of_program": _find_column(df, bsr_cols['type_of_program']),
        "match_day": _find_column(df, bsr_cols['match_day']),
        "home_team": _find_column(df, bsr_cols['home_team']),
        "away_team": _find_column(df, bsr_cols['away_team']),
        "aud_estimates": _find_column(df, bsr_cols['aud_estimates']),
        "aud_metered": _find_column(df, bsr_cols['aud_metered']),
        "source": _find_column(df, bsr_cols['source'])
    }

    # --- Initialize result columns
    df["Completeness_OK"] = True
    df["Completeness_Remark"] = ""

    # --- Get rules from config ---
    live_types = set(rules.get('live_types', ['live', 'repeat', 'delayed']))
    relaxed_types = set(rules.get('relaxed_types', ['highlights']))

    # --- Iterate rows
    for idx, row in df.iterrows():
        missing = []

        # 1️⃣ Mandatory Fields
        for logical, display in [("tv_channel", "TV Channel"), ("channel_id", "Channel ID"),
                                 ("match_day", "Match Day"), ("source", "Source")]:
            colname = colmap.get(logical)
            if colname is None:
                missing.append(f"{display} (column not found)")
            elif not _is_present(row.get(colname)):
                missing.append(display)

        # 2️⃣ Audience Logic
        aud_est_col = colmap.get("aud_estimates")
        aud_met_col = colmap.get("aud_metered")

        if not aud_est_col and not aud_met_col:
            missing.append("Audience (Estimates/Metered) (columns not found)")
        else:
            est_present = _is_present(row.get(aud_est_col)) if aud_est_col else False
            met_present = _is_present(row.get(aud_met_col)) if aud_met_col else False

            if not est_present and not met_present:
                missing.append("Both Audience fields are empty")
            elif est_present and met_present:
                missing.append("Both Audience fields are filled")

        # 3️⃣ Type-based (Home/Away)
        type_col = colmap.get("type_of_program")
        prog_type = str(row.get(type_col) or "").strip().lower() if type_col else ""
        home_col, away_col = colmap.get("home_team"), colmap.get("away_team")

        if prog_type in live_types:
            if not home_col: missing.append("Home Team (column not found)")
            elif not _is_present(row.get(home_col)): missing.append("Home Team")
            
            if not away_col: missing.append("Away Team (column not found)")
            elif not _is_present(row.get(away_col)): missing.append("Away Team")

        elif prog_type not in relaxed_types:
            # Check for other types that *should* have teams
            if home_col and not _is_present(row.get(home_col)): missing.append("Home Team")
            if away_col and not _is_present(row.get(away_col)): missing.append("Away Team")

        # 4️⃣ Final result
        if missing:
            df.at[idx, "Completeness_OK"] = False
            df.at[idx, "Completeness_Remark"] = "; ".join(missing)
        else:
            df.at[idx, "Completeness_Remark"] = "All key fields present"

    return df


# ----------------------------- 5️⃣ Overlap / Duplicate / Day Break -----------------------------
def overlap_duplicate_daybreak_check(df, bsr_cols, rules):
    """
    Final optimized & realistic Overlap + Duplicate + Daybreak check.
    """

    df = df.copy()

    # ------------------------------------------------------------
    # Column mappings
    # ------------------------------------------------------------
    col_channel       = _find_column(df, bsr_cols['tv_channel'])
    col_channel_id    = _find_column(df, bsr_cols['channel_id'])
    col_market        = _find_column(df, bsr_cols['market'])
    col_broadcaster   = _find_column(df, bsr_cols['broadcaster'])
    col_date          = _find_column(df, bsr_cols['date'])
    col_title         = _find_column(df, bsr_cols['program_title'])
    col_start         = _find_column(df, bsr_cols['start_time'])
    col_end           = _find_column(df, bsr_cols['end_time'])

    required = [
        col_channel, col_channel_id, col_market, col_broadcaster,
        col_date, col_title, col_start, col_end
    ]

    if any(c is None for c in required):
        df["Overlap_OK"] = False
        df["Overlap_Remark"] = "Missing required columns"
        df["Duplicate_OK"] = False
        df["Duplicate_Remark"] = "Missing required columns"
        df["Daybreak_OK"] = False
        df["Daybreak_Remark"] = "Missing required columns"
        return df

    # ------------------------------------------------------------
    # Parse datetime
    # ------------------------------------------------------------
    df["_start_dt"] = pd.to_datetime(df[col_start], errors="coerce")
    df["_end_dt"]   = pd.to_datetime(df[col_end], errors="coerce")
    df["_orig_idx"] = df.index

    # ------------------------------------------------------------
    # Sort for checks
    # ------------------------------------------------------------
    df = df.sort_values(
        by=[col_channel, col_channel_id, col_market, col_date, "_start_dt"],
        na_position="last"
    ).reset_index(drop=True)

    n = len(df)

    # ------------------------------------------------------------
    # Output containers
    # ------------------------------------------------------------
    overlap_ok = [True] * n
    overlap_r  = [""]   * n

    duplicate_ok = [True] * n
    duplicate_r  = [""]   * n

    daybreak_ok = [True] * n
    daybreak_r  = [""]   * n

    # =====================================================================
    # 1️⃣ DUPLICATE CHECK 
    # =====================================================================
    dup_columns = [
        col_channel,
        col_channel_id,
        col_broadcaster,
        col_market,   
        col_date,
        col_start,
        col_end
    ]

    dup_mask = df.duplicated(subset=dup_columns, keep=False)

    for i in range(n):
        if dup_mask.iloc[i]:
            duplicate_ok[i] = False
            duplicate_r[i] = (
                "In-market duplicate (same channel/channel ID/broadcaster/"
                "market/date/start/end)"
            )

    # =====================================================================
    # 2️⃣ OVERLAP CHECK (same channel + same date + same market)
    # =====================================================================
    for i in range(1, n):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        if (
            curr[col_channel] == prev[col_channel] and
            curr[col_market] == prev[col_market] and
            curr[col_date] == prev[col_date]
        ):
            # overlap only if start < previous end
            if curr["_start_dt"] < prev["_end_dt"]:
                overlap_ok[i] = False
                overlap_r[i] = "Overlap detected between programs"

    # =====================================================================
    # 3️⃣ DAYBREAK CHECK 
    # =====================================================================
    gap_tolerance = rules.get("daybreak_gap_tolerance_min", 5)

    for i in range(1, n):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        # must be same feed chain
        if not (
            curr[col_channel]    == prev[col_channel] and
            curr[col_channel_id] == prev[col_channel_id] and
            curr[col_market]     == prev[col_market]
        ):
            continue

        if pd.isna(prev["_end_dt"]) or pd.isna(curr["_start_dt"]):
            continue

        #  REAL MIDNIGHT PATTERN (BSR behavior)
        if (
            prev["_end_dt"].hour >= 23 and     # late night finish
            curr["_start_dt"].hour <= 1        # early morning start
        ):
            gap = (curr["_start_dt"] - prev["_end_dt"]).total_seconds() / 60

            if 0 <= gap <= gap_tolerance:
                daybreak_ok[i] = True
                daybreak_r[i] = "Valid midnight continuation"
            else:
                daybreak_ok[i] = False
                daybreak_r[i] = f"Invalid continuation gap ({gap:.1f} min > {gap_tolerance} min)"

    # =====================================================================
    # Attach results
    # =====================================================================
    df["Duplicate_OK"] = duplicate_ok
    df["Duplicate_Remark"] = duplicate_r

    df["Overlap_OK"] = overlap_ok
    df["Overlap_Remark"] = overlap_r

    df["Daybreak_OK"] = daybreak_ok
    df["Daybreak_Remark"] = daybreak_r

    # cleanup
    df = df.sort_values("_orig_idx").drop(columns=["_start_dt", "_end_dt", "_orig_idx"])

    return df


# ----------------------------- 6️⃣ Program Category Check -----------------------------
def program_category_check(bsr_path, df, col_map, rules, file_rules):
    """
    FINAL Program Category Logic (as per business rules):
    ------------------------------------------------------
      LIVE     : First broadcast, fixture matches exactly, start within ±30 mins.
      DELAYED  : First broadcast, fixture matches exactly, but start outside ±30.
      REPEAT   : Not first broadcast of same event.
      HIGHLIGHT: Duration 10–40 AND description contains highlight keywords.
      MAGAZINE : Duration 10–40 AND description contains pre/post/studio etc.
      BSA Rule : If source=='BSA' and type is LIVE/REPEAT => duration <=180.

      Sheet matching columns:
         From BSR    -> competition/event, matchday, home, away, date, start, end
         From Fixture-> competition, matchday, home, away, date, start, end
    """

    # --------------------------- 1. Load Fixture Sheet ---------------------------
    xl = pd.ExcelFile(bsr_path)
    fixture_keyword = file_rules.get("fixture_sheet_keyword", "fixture")
    fixture_sheet = next((s for s in xl.sheet_names if fixture_keyword in s.lower()), None)

    if not fixture_sheet:
        df["Program_Category_Expected"] = pd.NA
        df["Program_Category_Actual"]   = df[col_map["bsr"]["type_of_program"]].astype(str).str.lower()
        df["Program_Category_OK"] = False
        df["Program_Category_Remark"] = "Fixture sheet missing"
        return df

    df_fix = xl.parse(fixture_sheet)

    # ------------------------- 2. Resolve Column Names ---------------------------
    b = col_map["bsr"]
    f = col_map["fixture"]

    col_comp_bsr  = _find_column(df, b.get("competition"))
    col_match_bsr = _find_column(df, b.get("matchday"))
    col_home_bsr  = _find_column(df, b.get("home_team"))
    col_away_bsr  = _find_column(df, b.get("away_team"))
    col_date_bsr  = _find_column(df, b.get("date"))
    col_start_bsr = _find_column(df, b.get("start_time"))
    col_end_bsr   = _find_column(df, b.get("end_time"))
    col_progtype  = _find_column(df, b.get("type_of_program"))
    col_desc      = _find_column(df, b.get("program_desc"))
    col_source    = _find_column(df, b.get("source"))
    col_duration  = _find_column(df, b.get("duration"))

    col_comp_fix  = _find_column(df_fix, f.get("competition"))
    col_match_fix = _find_column(df_fix, f.get("matchday"))
    col_home_fix  = _find_column(df_fix, f.get("home_team"))
    col_away_fix  = _find_column(df_fix, f.get("away_team"))
    col_date_fix  = _find_column(df_fix, f.get("date"))
    col_start_fix = _find_column(df_fix, f.get("start_time"))
    col_end_fix   = _find_column(df_fix, f.get("end_time"))

    required = [col_comp_bsr, col_match_bsr, col_home_bsr, col_away_bsr,
                col_date_bsr, col_start_bsr, col_end_bsr]
    if any(c is None for c in required):
        df["Program_Category_Expected"] = pd.NA
        df["Program_Category_Actual"]   = df[col_progtype]
        df["Program_Category_OK"] = False
        df["Program_Category_Remark"] = "Missing required BSR columns"
        return df

    # -------------------- 3. Clean Strings for Matching -------------------------
    def norm(x):
        if pd.isna(x): return ""
        x = str(x).lower().strip()
        x = x.replace(".", "").replace("fc ", "").replace("cf ", "")
        x = x.replace("-", " ").replace("–", " ")
        return re.sub(r"\s+", " ", x)

    df["_comp"]   = df[col_comp_bsr].map(norm)
    df["_match"]  = df[col_match_bsr].astype(str).str.lower().str.strip()
    df["_home"]   = df[col_home_bsr].map(norm)
    df["_away"]   = df[col_away_bsr].map(norm)
    df["_date"]   = pd.to_datetime(df[col_date_bsr], errors="coerce").dt.date

    df_fix["_comp"]  = df_fix[col_comp_fix].map(norm)
    df_fix["_match"] = df_fix[col_match_fix].astype(str).str.lower().str.strip()
    df_fix["_home"]  = df_fix[col_home_fix].map(norm)
    df_fix["_away"]  = df_fix[col_away_fix].map(norm)
    df_fix["_date"]  = pd.to_datetime(df_fix[col_date_fix], errors="coerce").dt.date

    # Parse datetime for start/end
    df["_start"] = pd.to_datetime(df[col_date_bsr].astype(str) + " " + df[col_start_bsr].astype(str), errors="coerce")
    df["_end"]   = pd.to_datetime(df[col_date_bsr].astype(str) + " " + df[col_end_bsr].astype(str), errors="coerce")

    df_fix["_start"] = pd.to_datetime(df_fix[col_date_fix].astype(str) + " " + df_fix[col_start_fix].astype(str), errors="coerce")
    df_fix["_end"]   = pd.to_datetime(df_fix[col_date_fix].astype(str) + " " + df_fix[col_end_fix].astype(str), errors="coerce")

    # Duration
    df["duration_min"] = parse_duration_to_minutes(df[col_duration]) if col_duration else np.nan

    # Actual Program Type
    df["Program_Category_Actual"] = df[col_progtype].astype(str).str.lower().str.strip()

    # -------------------- 4. Create Event Key for Both Sheets -------------------
    df["_event_key"] = df.apply(lambda r: (r["_comp"], r["_match"], r["_home"], r["_away"], r["_date"]), axis=1)
    df_fix["_event_key"] = df_fix.apply(lambda r: (r["_comp"], r["_match"], r["_home"], r["_away"], r["_date"]), axis=1)

    # Build fixture groups for lookup
    fix_groups = df_fix.groupby("_event_key")

    # -------------------- 5. Initialize Output Columns --------------------------
    df["Program_Category_Expected"] = pd.NA
    df["Program_Category_OK"] = False
    df["Program_Category_Remark"] = ""

    # Rules
    live_tol = rules.get("live_tolerance_min", 30)
    highlight_keys = [k.lower() for k in rules.get("highlight_keywords", [])]
    magazine_keys  = [k.lower() for k in rules.get("magazine_keywords", [])]
    support_min = rules.get("support_duration_min", 10)
    support_max = rules.get("support_duration_max", 40)
    bsa_max = rules.get("bsa_max_duration", 180)

    # -------------------- 6. Process Group-wise (Per Match Event) ----------------
    for event_key, grp in df.groupby("_event_key"):
        grp_sorted = grp.sort_values("_start")
        indices = grp_sorted.index.tolist()

        # Find fixture rows for this event
        if event_key in fix_groups.groups:
            fix_df = df_fix.loc[fix_groups.groups[event_key]]
        else:
            fix_df = pd.DataFrame()

        for pos, idx in enumerate(indices):
            row = df.loc[idx]
            desc = row[col_desc].lower() if col_desc else ""
            source = row[col_source].lower() if col_source else ""
            dur = row["duration_min"]

            # ------------ A. Highlights Override ------------
            if not pd.isna(dur) and support_min <= dur <= support_max:
                if any(k in desc for k in highlight_keys):
                    df.at[idx,"Program_Category_Expected"]="highlights"
                    df.at[idx,"Program_Category_OK"] = row["Program_Category_Actual"]=="highlights"
                    df.at[idx,"Program_Category_Remark"]="Highlights rule matched"
                    continue

                if any(k in desc for k in magazine_keys):
                    df.at[idx,"Program_Category_Expected"]="magazine"
                    df.at[idx,"Program_Category_OK"] = row["Program_Category_Actual"] in ["magazine","support","studio"]
                    df.at[idx,"Program_Category_Remark"]="Magazine rule matched"
                    continue

            # ------------ B. Fixture Handling ------------
            if fix_df.empty:
                df.at[idx,"Program_Category_Expected"]=pd.NA
                df.at[idx,"Program_Category_OK"]=False
                df.at[idx,"Program_Category_Remark"]="No matching fixture found"
                continue

            # Pick fixture row with closest start time
            bsr_start = row["_start"]
            best_diff=None
            for fidx,frow in fix_df.iterrows():
                if pd.isna(frow["_start"]) or pd.isna(bsr_start): continue
                diff=abs((bsr_start-frow["_start"]).total_seconds()/60)
                if best_diff is None or diff<best_diff:
                    best_diff=diff

            # ------------ C. Live / Delayed / Repeat ------------
            if pos==0:   # first broadcast
                if best_diff is not None and best_diff <= live_tol:
                    expected="live"
                    remark=f"Within ±{live_tol} min -> live"
                else:
                    expected="delayed"
                    remark=f"Outside ±{live_tol} min -> delayed"
            else:
                expected="repeat"
                remark=f"{pos+1}th broadcast -> repeat"

            df.at[idx,"Program_Category_Expected"]=expected
            df.at[idx,"Program_Category_OK"] = (row["Program_Category_Actual"]==expected)
            df.at[idx,"Program_Category_Remark"]=remark

            # ------------ D. BSA Rule ------------
            if "bsa" in source and expected in ["live","repeat"]:
                if pd.isna(dur) or dur > bsa_max:
                    df.at[idx,"Program_Category_OK"]=False
                    df.at[idx,"Program_Category_Remark"]+=" | BSA duration invalid"

    # Cleanup helper columns
    df.drop(columns=[c for c in ["_comp","_match","_home","_away","_date","_start","_end","_event_key"] if c in df.columns], inplace=True)

    return df

# 8️⃣ Event / Matchday / Competition Check
def check_event_matchday_competition(df_worksheet, df_data=None, rosco_path=None, debug_rows=20):
    """
    Validate Event / Competition / Matchday / Match combinations.

    Inputs:
      - df_worksheet : DataFrame of the main worksheet (the BSR "Worksheet")
          expected columns: "Competition", "Event", "Matchday", "Home Team", "Away Team", maybe "Match"
      - df_data : optional DataFrame extracted from the 'Data' sheet (the reference/master lists).
      - rosco_path : optional path to Excel; used if df_data is None to try to extract reference values from that file.
      - debug_rows: how many rows to print for debug output

    Output:
      - same df_worksheet with two new columns:
          Event_Matchday_Competition_OK (bool)
          Event_Matchday_Competition_Remark (string)
    """

    # --- Helper: normalize text ---
    def norm(x):
        if pd.isna(x):
            return ""
        return str(x).strip()

    def norm_lower(x):
        return norm(x).lower()

    # --- Get reference competitions / allowed values ---
    reference_comps = set()
    reference_matches = set()  # optional: canonical "home vs away" pairs if available
    reference_matchday_counts = {}  # optional expected counts per (competition, matchday)

    if df_data is None and rosco_path is not None:
        # attempt to load a 'Data' sheet or the first sheet that looks like the data table
        try:
            xls = pd.read_excel(rosco_path, sheet_name=None)
            # try common names
            priority = ["Data", "data", "Monitoring list", "monitoring list", "Monitoring List"]
            found_df = None
            for p in priority:
                if p in xls:
                    found_df = xls[p]
                    break
            if found_df is None:
                # fallback: pick sheet that has words like 'Type of programme' or 'Competition' in header rows
                for name, sheet in xls.items():
                    header_text = " ".join(sheet.columns.astype(str).tolist()).lower()
                    if "competition" in header_text or "type of programme" in header_text or "type of program" in header_text:
                        found_df = sheet
                        break
            if found_df is not None:
                df_data = found_df
        except Exception:
            df_data = None

    # If df_data is available, extract competition names and optional counts
    if isinstance(df_data, pd.DataFrame):
        # strategy: scan df_data content for competition-like strings
        df_tmp = df_data.astype(str).applymap(lambda v: v.strip() if pd.notna(v) else "")
        # collect distinct non-empty strings that look like competition names
        for col in df_tmp.columns:
            for val in df_tmp[col].unique():
                v = str(val).strip()
                if v and v not in ["0", "nan", "-", "None"]:
                    # filter out lines that look numeric counts (only digits)
                    if not re.fullmatch(r"^\d+$", v):
                        reference_comps.add(v.lower())

        # attempt to read counts if present: some Data sheets have count rows above/below the headers
        # Look for numeric entries adjacent to competition names in columns
        # Heuristic: if the first few rows contain digits under the same columns as competition names, store count.
        try:
            # look at the first ~10 rows for numeric counts under columns that are competition names
            for col in df_data.columns:
                numeric_counts = []
                for r in range(min(10, len(df_data))):
                    try:
                        v = df_data.iloc[r][col]
                        if pd.notna(v) and str(v).strip().isdigit():
                            numeric_counts.append(int(str(v).strip()))
                    except Exception:
                        continue
                if numeric_counts:
                    # pick a representative (first) numeric if consistent
                    reference_matchday_counts[col.strip().lower()] = numeric_counts[0]
        except Exception:
            pass

    # fallback: if still empty, use some likely defaults
    if not reference_comps:
        reference_comps = set([
            "bundesliga", "2. bundesliga", "dfb-pokal", "dfl supercup",
            "premier league", "epl", "la liga", "serie a", "champions league"
        ])

    # Precompute a lowercase set for quick lookup
    reference_comps_lower = set(x.lower() for x in reference_comps)

    # --- Prepare output columns ---
    df = df_worksheet.copy()
    df["Event_Matchday_Competition_OK"] = False
    df["Event_Matchday_Competition_Remark"] = ""

    # We'll build grouping counts to verify number of matches per (Competition, Matchday)
    grouped_counts = {}

    # iterate rows
    for idx, row in df.iterrows():
        competition = norm(row.get("Competition", ""))
        event = norm(row.get("Event", ""))
        matchday = norm(row.get("Matchday", ""))

        # some BSRs have 'Matchday' in other column names like 'Matchday ' or 'Match Day' - check alternatives
        if not matchday:
            # try columns similar to matchday
            for c in df.columns:
                if "matchday" in c.lower() or "match day" in c.lower() or c.lower().strip() == "match":
                    matchday = norm(row.get(c, ""))
                    if matchday:
                        break

        # find home/away or match field
        home = norm(row.get("Home Team", "")) or norm(row.get("HomeTeam", "")) or norm(row.get("Home", ""))
        away = norm(row.get("Away Team", "")) or norm(row.get("AwayTeam", "")) or norm(row.get("Away", ""))

        remarks = []
        ok = True

        # 1) Missing fields
        if not competition or competition.strip() in ["-", "nan", "none"]:
            ok = False
            remarks.append("Missing Competition")
        if not event or event.strip() in ["-", "nan", "none"]:
            ok = False
            remarks.append("Missing Event")
        if not matchday or matchday.strip() in ["-", "nan", "none"]:
            ok = False
            remarks.append("Missing Matchday")
        if not (home and away):
            # sometimes matches are in 'Match' or 'Program Title', try match detection
            match_text = norm(row.get("Match", "")) or norm(row.get("Program Title", "")) or norm(row.get("Combined", ""))
            # a simple heuristic: look for ' vs ' or ' v ' separators
            if " vs " in match_text.lower() or " v " in match_text.lower():
                # we accept this as a match, but still prefer to split
                try:
                    parts = re.split(r"\s+v(?:s|)\.?\s+|\s+vs\.?\s+|\s+v\s+", match_text, flags=re.IGNORECASE)
                    if len(parts) >= 2:
                        home = parts[0].strip()
                        away = parts[1].strip()
                except Exception:
                    pass
            else:
                ok = False
                remarks.append("Missing Home/Away or Match field")

        # 2) Validate competition against reference list
        comp_l = competition.lower()
        # some competitions appear with extra words, do a contains check
        comp_matches_reference = False
        for rc in reference_comps_lower:
            if rc and (rc in comp_l or comp_l in rc):
                comp_matches_reference = True
                break
        if not comp_matches_reference:
            ok = False
            remarks.append("Competition not in reference list")

        # 3) Simple event-matchday-match consistency: check if 'matchday' value format looks valid (MD, Round, etc.)
        # Accept common formats: 'Matchday 01', 'MD01', 'Round 01', 'Round 1', 'Matchday 1'
        if matchday:
            if not re.search(r"(matchday|md|round|rd|r|matchday)\s*\d+", matchday.lower()):
                # allow some textual forms like 'Finals', 'Semi', 'Quarter'
                if matchday.lower() not in ["final", "finals", "semi", "semifinal", "quarterfinal", "playoffs", "-"]:
                    # it's not necessarily an error; just add a warning
                    remarks.append("Unusual matchday format")

        # 4) If we have a reference expected counts mapping (from df_data), count per (competition, matchday)
        comp_key = (competition.strip().lower(), matchday.strip().lower())
        grouped_counts.setdefault(comp_key, 0)
        grouped_counts[comp_key] += 1

        # Compose final remark and set OK
        df.at[idx, "Event_Matchday_Competition_OK"] = ok
        df.at[idx, "Event_Matchday_Competition_Remark"] = "; ".join(remarks) if remarks else "OK"

    # 5) If reference_matchday_counts available, compare counts and append remarks for rows belonging to mismatch groups
    # reference_matchday_counts keys may be competition names -> expected counts per matchday (heuristic)
    if reference_matchday_counts:
        # For each group in grouped_counts, compare to reference (best-effort)
        for (comp, mday), observed in grouped_counts.items():
            expected = None
            # try to find matching competition in reference counts map
            for ref_comp_name, cnt in reference_matchday_counts.items():
                if ref_comp_name and (ref_comp_name in comp or comp in ref_comp_name):
                    expected = cnt
                    break
            if expected is not None and observed != expected:
                # flag all rows in df with this (comp, mday)
                mask = df[
                    df.get("Competition", "").astype(str).str.strip().str.lower() == comp
                ]["Competition"].notna()
                # append a remark for each row in this group
                for idx in df[
                    (df.get("Competition", "").astype(str).str.strip().str.lower() == comp) &
                    (df.get("Matchday", "").astype(str).str.strip().str.lower() == mday)
                ].index:
                    prev = df.at[idx, "Event_Matchday_Competition_Remark"]
                    extra = f"Mismatch matches per matchday: expected {expected}, found {observed}"
                    df.at[idx, "Event_Matchday_Competition_Remark"] = (prev + "; " + extra) if prev else extra
                    df.at[idx, "Event_Matchday_Competition_OK"] = False

    # --- Debug prints (first few rows) ---
    print("=== Event/Matchday/Competition QC summary (first rows) ===")
    for idx in range(min(debug_rows, len(df))):
        r = df.iloc[idx]
        print(f"[Row {idx}] Competition='{r.get('Competition','')}' | Event='{r.get('Event','')}' | Matchday='{r.get('Matchday','')}' | "
              f"Home='{r.get('Home Team', r.get('Home', ''))}' Away='{r.get('Away Team', r.get('Away', ''))}' | "
              f"OK={r['Event_Matchday_Competition_OK']} | Remark={r['Event_Matchday_Competition_Remark']}")
    print("=== End summary ===\n")

    return df

#-------------- 9️⃣ Market / Channel /  Consistency Check -----------------

def market_channel_consistency_check(df_bsr, rosco_path, col_map, file_rules):
    
    logging.info("🔍 Starting Market & Channel Consistency Check...")
    
    bsr_cols = col_map['bsr']
    rosco_cols = col_map['rosco']
    
    # --- Normalization helper for ROSCO ---
    def normalize_channel(name):
        if pd.isna(name): return ""
        s = str(name)
        s = re.sub(r"\(.*?\)|\[.*?\]", "", s)
        s = re.split(r"[-–—]", s)[0]
        s = re.sub(r"[^0-9a-zA-Z\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip().lower()
        return s

    # --- Load ROSCO reference sheet ---
    rosco_df = None
    if rosco_path:
        try:
            xls = pd.ExcelFile(rosco_path)
            ignore_sheet = file_rules.get('rosco_ignore_sheet', 'general')
            sheet_name = next((s for s in xls.sheet_names if ignore_sheet not in s.lower()), None)
            if sheet_name:
                rosco_df = xls.parse(sheet_name)
            else:
                logging.warning(f"⚠️ No valid sheet found in ROSCO (ignoring '{ignore_sheet}').")
        except Exception as e:
            logging.error(f"❌ Error loading ROSCO file: {e}")
            df_bsr["Market_Channel_Consistency_OK"] = False
            df_bsr["Market_Channel_Program_Remark"] = f"Error loading ROSCO: {e}"
            return df_bsr

    # --- Build valid (Market, Channel) pairs from ROSCO ---
    valid_pairs = set()
    rosco_country_col = rosco_cols.get('channel_country', 'ChannelCountry')
    rosco_name_col = rosco_cols.get('channel_name', 'ChannelName')
    
    if rosco_df is not None:
        if {rosco_country_col, rosco_name_col}.issubset(rosco_df.columns):
            for _, row in rosco_df.iterrows():
                market = str(row[rosco_country_col]).strip().lower()
                channel = normalize_channel(row[rosco_name_col])
                if market and channel:
                    valid_pairs.add((market, channel))
            logging.info(f"✅ Loaded {len(valid_pairs)} valid Market+Channel pairs from ROSCO.")
        else:
            logging.warning(f"⚠️ '{rosco_country_col}' or '{rosco_name_col}' not in ROSCO sheet.")

    # --- Prepare result columns ---
    df_bsr["Market_Channel_Consistency_OK"] = True
    df_bsr["Market_Channel_Program_Remark"] = "OK"
    
    # --- Find BSR columns ---
    bsr_market_col = _find_column(df_bsr, bsr_cols['market'])
    bsr_channel_col = _find_column(df_bsr, bsr_cols['tv_channel'])
    
    if not bsr_market_col or not bsr_channel_col:
        logging.error("❌ Market/Channel Check: BSR columns not found. Skipping.")
        df_bsr["Market_Channel_Consistency_OK"] = False
        df_bsr["Market_Channel_Program_Remark"] = "BSR columns not found"
        return df_bsr

    # --- Validate each row in BSR ---
    for idx, row in df_bsr.iterrows():
        remarks = []
        market = str(row.get(bsr_market_col, "")).strip().lower()
        channel = str(row.get(bsr_channel_col, "")).strip()

        if not market or not channel:
            df_bsr.at[idx, "Market_Channel_Consistency_OK"] = False
            remarks.append("Missing market or channel")
        elif valid_pairs:
            if (market, normalize_channel(channel)) not in valid_pairs:
                df_bsr.at[idx, "Market_Channel_Consistency_OK"] = False
                remarks.append("Market+Channel not found in ROSCO")

        df_bsr.at[idx, "Market_Channel_Program_Remark"] = "; ".join(remarks) if remarks else "OK"

    logging.info("✅ Market & Channel Consistency Check completed.")
    return df_bsr

# -----------------------------------------------------------
# 10️⃣ Domestic Market Coverage Check
def domestic_market_coverage_check(df_worksheet, reference_df=None, debug_rows=10):
    df = df_worksheet.copy()
    df["Domestic_Market_Coverage_OK"] = True
    df["Domestic_Market_Remark"] = ""

    DOMESTIC_MAP = {
        "bundesliga": ["germany", "deutschland"],
        "premier league": ["united kingdom", "england"],
        "la liga": ["spain"],
        "serie a": ["italy"],
        "ligue 1": ["france"],
    }

    for idx, row in df.iterrows():
        comp = str(row.get("Competition", "")).lower()
        market = str(row.get("Market", "")).lower()
        progtype = str(row.get("Type of Program", "")).lower()

        domestic_markets = []
        for key, vals in DOMESTIC_MAP.items():
            if key in comp:
                domestic_markets = vals
                break
        if domestic_markets and any(k in progtype for k in ["live", "broadcast", "direct"]) and market not in domestic_markets:
            df.at[idx, "Domestic_Market_Coverage_OK"] = False
            df.at[idx, "Domestic_Market_Remark"] = f"Missing domestic live coverage for {market}"
    return df

# -----------------------------------------------------------
# 11️⃣ Rates & Ratings Check
# --------------------------------------------
def rates_and_ratings_check(df, bsr_cols):
    
    est_col = _find_column(df, bsr_cols['aud_estimates'])
    met_col = _find_column(df, bsr_cols['aud_metered'])
    
    if est_col is None:
        df[est_col] = pd.NA # Create dummy column to avoid errors
        logging.warning("Rates/Ratings Check: Audience Estimates column not found.")
    if met_col is None:
        df[met_col] = pd.NA
        logging.warning("Rates/Ratings Check: Audience Metered column not found.")

    present_est = df[est_col].apply(_is_present)
    present_met = df[met_col].apply(_is_present)

    both_empty_mask = (~present_est) & (~present_met)
    both_present_mask = (present_est) & (present_met)
    exactly_one_mask = (present_est ^ present_met)

    df["Rates_Ratings_QC_OK"] = True
    df["Rates_Ratings_QC_Remark"] = ""
    
    df.loc[both_empty_mask, "Rates_Ratings_QC_OK"] = False
    df.loc[both_empty_mask, "Rates_Ratings_QC_Remark"] = "Missing audience ratings (both empty)"
    
    df.loc[both_present_mask, "Rates_Ratings_QC_OK"] = False
    df.loc[both_present_mask, "Rates_Ratings_QC_Remark"] = "Invalid: both metered and estimated present"
    
    df.loc[exactly_one_mask, "Rates_Ratings_QC_OK"] = True
    df.loc[exactly_one_mask, "Rates_Ratings_QC_Remark"] = "Valid: one rating source available"

    return df

# -----------------------------------------------------------
# 12️⃣ Comparison of Duplicated Markets
def duplicated_market_check(df_bsr, macro_path, project, col_map, file_rules, debug=False):
    
    result_col = "Duplicated_Markets_Check_OK"
    remark_col = "Duplicated_Markets_Remark"
    
    df_bsr[result_col] = pd.NA # Default to Not Applicable
    df_bsr[result_col] = df_bsr[result_col].astype('object')
    df_bsr[remark_col] = "Not Applicable"
    
    league_keyword = project.get('league_keyword', 'F24 Spain')
    bsr_cols = col_map['bsr']
    macro_cols = col_map['macro']

    if not macro_path or not os.path.exists(macro_path):
        df_bsr[remark_col] = "Macro file missing"
        return df_bsr

    try:
        # --- Load and clean Macro Data ---
        macro_sheet = file_rules.get('macro_sheet_name', 'Data Core')
        header_row = file_rules.get('macro_header_row', 1)
        macro_df = pd.read_excel(macro_path, sheet_name=macro_sheet, header=header_row, dtype=str)
        macro_df.columns = macro_df.columns.str.strip()

        # Find macro columns
        proj_col = macro_cols['projects']
        orig_mkt_col = macro_cols['orig_market']
        orig_ch_col = macro_cols['orig_channel']
        dup_mkt_col = macro_cols['dup_market']
        dup_ch_col = macro_cols['dup_channel']
        
        macro_df = macro_df[
            macro_df[proj_col].astype(str).str.contains(league_keyword, case=False, na=False)
        ].copy()

        if macro_df.empty:
            df_bsr[remark_col] = f"No duplication rules found for {league_keyword}"
            return df_bsr

        for col in [orig_mkt_col, orig_ch_col, dup_mkt_col, dup_ch_col]:
            macro_df[col] = macro_df[col].astype(str).str.strip().str.lower()

        # --- Find BSR columns ---
        mkt_col = _find_column(df_bsr, bsr_cols['market'])
        ch_col = _find_column(df_bsr, bsr_cols['tv_channel'])
        comp_col = _find_column(df_bsr, bsr_cols['competition'])
        evt_col = _find_column(df_bsr, bsr_cols['event'])

        # --- Filter BSR for selected league (competition/event) ---
        in_league = (
            df_bsr[comp_col].astype(str).str.lower().str.contains(league_keyword.lower(), na=False)
            | df_bsr[evt_col].astype(str).str.lower().str.contains(league_keyword.lower(), na=False)
        )
        df_league = df_bsr[in_league].copy()

        if df_league.empty:
            df_bsr[remark_col] = f"No events found for {league_keyword}"
            return df_bsr

        # --- Core Duplication Logic ---
        for _, row in macro_df.iterrows():
            orig_market = row[orig_mkt_col]
            orig_channel = row[orig_ch_col]
            dup_market = row[dup_mkt_col]
            dup_channel = row[dup_ch_col]

            orig_events = set(df_league[
                (df_league[mkt_col].astype(str).str.lower() == orig_market)
                & (df_league[ch_col].astype(str).str.lower() == orig_channel)
            ][evt_col])

            dup_events = set(df_league[
                (df_league[mkt_col].astype(str).str.lower() == dup_market)
                & (df_league[ch_col].astype(str).str.lower() == dup_channel)
            ][evt_col])

            status, remark = pd.NA, "Not Applicable"
            if not orig_events:
                status = pd.NA
                remark = f"No events found in {orig_market} / {orig_channel}"
            elif orig_events.issubset(dup_events):
                status = True
                remark = f"All events correctly duplicated to {dup_market} / {dup_channel}"
            else:
                missing = orig_events - dup_events
                status = False
                remark = f"Missing {len(missing)} events in {dup_market} / {dup_channel}"

            # Apply results to all relevant rows
            orig_rows_mask = (df_bsr[mkt_col].astype(str).str.lower() == orig_market) & \
                             (df_bsr[ch_col].astype(str).str.lower() == orig_channel) & in_league
            dup_rows_mask = (df_bsr[mkt_col].astype(str).str.lower() == dup_market) & \
                            (df_bsr[ch_col].astype(str).str.lower() == dup_channel) & in_league

            df_bsr.loc[orig_rows_mask | dup_rows_mask, result_col] = status
            df_bsr.loc[orig_rows_mask | dup_rows_mask, remark_col] = remark

        return df_bsr

    except Exception as e:
        df_bsr[result_col] = False
        df_bsr[remark_col] = str(e)
        return df_bsr
# -----------------------------------------------------------
# 13️⃣ Country & Channel IDs Check
def country_channel_id_check(df, bsr_cols):
    
    df["Market_Channel_ID_OK"] = True
    df["Market_Channel_ID_Remark"] = "OK"

    ch_col = _find_column(df, bsr_cols['tv_channel'])
    ch_id_col = _find_column(df, bsr_cols['channel_id'])
    mkt_col = _find_column(df, bsr_cols['market'])
    mkt_id_col = _find_column(df, bsr_cols['market_id'])
    
    if not all([ch_col, ch_id_col, mkt_col, mkt_id_col]):
        logging.warning("ID Check: Missing one or more ID columns. Skipping.")
        df["Market_Channel_ID_OK"] = False
        df["Market_Channel_ID_Remark"] = "Check skipped: ID columns not found"
        return df

    def norm(x):
        return str(x).strip() if pd.notna(x) else ""

    channel_id_map = {}
    market_id_map = {}
    
    # Build maps first
    for idx, row in df.iterrows():
        channel = norm(row.get(ch_col))
        channel_id = norm(row.get(ch_id_col))
        market = norm(row.get(mkt_col))
        market_id = norm(row.get(mkt_id_col))

        if channel and channel_id and channel not in channel_id_map:
            channel_id_map[channel] = channel_id
        if market and market_id and market not in market_id_map:
            market_id_map[market] = market_id
            
    # Check for inconsistencies
    for idx, row in df.iterrows():
        channel = norm(row.get(ch_col))
        channel_id = norm(row.get(ch_id_col))
        market = norm(row.get(mkt_col))
        market_id = norm(row.get(mkt_id_col))
        
        remarks = []
        ok = True

        if channel and channel_id_map.get(channel) != channel_id:
            remarks.append(f"Channel '{channel}' has multiple IDs")
            ok = False
        if market and market_id_map.get(market) != market_id:
            remarks.append(f"Market '{market}' has multiple IDs")
            ok = False
            
        df.at[idx, "Market_Channel_ID_OK"] = ok
        df.at[idx, "Market_Channel_ID_Remark"] = "; ".join(remarks) if remarks else "OK"

    return df

# -----------------------------------------------------------
# 14️⃣ Client Data / LSTV / OTT Check (corrected)
#def client_lstv_ott_check(df_worksheet, project_config=None):
    """
    Checks:
      - Market and Channel ID consistency
      - Inclusion of Client Data, LSTV, OTT sources
    Returns:
      df with:
        - Client_LSTV_OTT_OK (True/False)
        - Client_LSTV_OTT_Remark
    """

    df = df_worksheet.copy()
    df["Client_LSTV_OTT_OK"] = True
    df["Client_LSTV_OTT_Remark"] = ""

    # --- 1️⃣ Market / Channel ID consistency ---
    if "Market ID" in df.columns and "Channel ID" in df.columns:
        # Identify Channel IDs belonging to multiple Market IDs
        multi_market = df.groupby("Channel ID")["Market ID"].nunique()
        multi_market_channels = multi_market[multi_market > 1].index.tolist()

        # Identify Market IDs belonging to multiple Channel IDs
        multi_channel = df.groupby("Market ID")["Channel ID"].nunique()
        multi_channel_ids = multi_channel[multi_channel > 1].index.tolist()
    else:
        multi_market_channels = []
        multi_channel_ids = []

    # --- 2️⃣ Client / LSTV / OTT inclusion ---
    pay_free_col = "Pay/Free TV" if "Pay/Free TV" in df.columns else None

    # Define expected sources
    expected_sources = ["lstv", "client", "ott"]

    for idx, row in df.iterrows():
        remarks = []
        ok = True

        # Market / Channel mapping issues
        if row.get("Channel ID") in multi_market_channels:
            ok = False
            remarks.append("Channel assigned to multiple Market IDs")

        if row.get("Market ID") in multi_channel_ids:
            ok = False
            remarks.append("Market ID assigned to multiple Channel IDs")

        # Client / LSTV / OTT source checks
        if pay_free_col:
            val = str(row.get(pay_free_col, "")).strip().lower()
            # Only mark False if none of the expected sources are present
            if not any(source in val for source in expected_sources):
                ok = False
                remarks.append(f"Missing required source (Client/LSTV/OTT): {row.get(pay_free_col, '')}")

        # Write results
        df.at[idx, "Client_LSTV_OTT_OK"] = ok
        df.at[idx, "Client_LSTV_OTT_Remark"] = "; ".join(remarks) if remarks else "OK"

    return df
# -----------------------------------------------------------
# ✅ Excel Coloring for True/False checks
def color_excel(output_path, df):
    from openpyxl import load_workbook
    from openpyxl.styles import PatternFill

    GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

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
# -----------------------------------------------------------
# Summary Sheet
def generate_summary_sheet(output_path, df):
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