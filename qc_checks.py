import pandas as pd
import re
import math
import datetime
import os
import numpy as np
import logging
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

DATE_FORMAT = "%Y-%m-%d"

# Excel color styles
GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
HEADER_FILL = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")

# ----------------------------- Helpers -----------------------------
def _find_column(df, candidates):
    if df is None:
        return None
    if not isinstance(candidates, list):
        candidates = [candidates]
    cols_lower = {str(c).lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand is None:
            continue
        k = str(cand).lower().strip()
        if k in cols_lower:
            return cols_lower[k]
    return None

def _is_present(val):
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except Exception:
        pass
    if isinstance(val, (int, float)) and not (isinstance(val, float) and pd.isna(val)):
        return True
    s = str(val).strip()
    if s == "":
        return False
    if s.lower() in ("nan", "none", "-"):
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
        except Exception:
            pass
        parts = s.split(':')
        if len(parts) >= 2:
            try:
                hours = float(re.sub(r"[^0-9.]", "", parts[0])) if parts[0] else 0.0
                minutes = float(re.sub(r"[^0-9.]", "", parts[1])) if parts[1] else 0.0
                seconds = 0.0
                if len(parts) >= 3:
                    seconds = float(re.sub(r"[^0-9.]", "", parts[2])) if parts[2] else 0.0
                total_minutes = (hours * 60) + minutes + (seconds / 60)
                results.append(total_minutes)
            except Exception:
                results.append(np.nan)
        else:
            results.append(np.nan)
    return pd.Series(results, index=duration_series.index)

def to_time_str(val):
    """Convert Excel time/float/time/string to HH:MM:SS string."""
    if pd.isna(val):
        return None

    # if already datetime.time
    if isinstance(val, datetime.time):
        return val.strftime("%H:%M:%S")

    # Excel float time (0.0–1.0)
    try:
        if isinstance(val, float) or isinstance(val, int):
            total_seconds = int(val * 24 * 3600)
            h = total_seconds // 3600
            m = (total_seconds % 3600) // 60
            s = total_seconds % 60
            return f"{h:02}:{m:02}:{s:02}"
    except:
        pass

    # fallback: string
    try:
        t = pd.to_datetime(str(val), errors="coerce")
        if isinstance(t, pd.Timestamp):
            return t.strftime("%H:%M:%S")
    except:
        return None

    return None


def to_date_str(val):
    """Convert Excel date/datetime/string to YYYY-MM-DD."""
    if pd.isna(val):
        return None

    if isinstance(val, datetime.date):
        return val.strftime("%Y-%m-%d")

    try:
        d = pd.to_datetime(val, errors="coerce")
        if isinstance(d, pd.Timestamp):
            return d.strftime("%Y-%m-%d")
    except:
        return None

    return None


def combine_parse(date_val, time_val):
    d = to_date_str(date_val)
    t = to_time_str(time_val)
    if not d or not t:
        return pd.NaT
    return pd.to_datetime(f"{d} {t}", errors="coerce")

# ----------------------------- AUTO SORT BSR -----------------------------
def auto_sort_bsr(df, bsr_cols):
    """
    Correct business-level BSR sorting:
    Region → Country → Channel ID → UTC datetime
    """

    df = df.copy()

    col_region = _find_column(df, ["Region", "Wider Region"])
    col_country = _find_column(df, ["Country", "Market"])
    col_channel_id = _find_column(df, ["Channel ID"])
    col_date_utc = _find_column(df, ["BSR_UTC_Date", "Date (UTC)", "Date"])
    col_start_utc = _find_column(df, ["Start (UTC)", "Start UTC"])

    if not all([col_country, col_channel_id, col_date_utc, col_start_utc]):
        logging.warning("Auto-sort skipped: required BSR columns missing")
        return df

    # Build true UTC datetime
    df["_utc_dt"] = df.apply(
        lambda r: combine_parse(r[col_date_utc], r[col_start_utc]),
        axis=1
    )

    sort_cols = []
    if col_region:
        sort_cols.append(col_region)

    sort_cols.extend([
        col_country,
        col_channel_id,
        "_utc_dt"
    ])

    df = df.sort_values(
        by=sort_cols,
        ascending=True,
        na_position="last"
    ).reset_index(drop=True)

    df.drop(columns=["_utc_dt"], inplace=True, errors="ignore")

    logging.info(
        "✅ BSR auto-sorted by Region → Country → Channel ID → UTC datetime"
    )

    return df



# ----------------------------- 1️⃣ Detect Monitoring Period -----------------------------
def detect_period_from_rosco(rosco_path):
    df = pd.read_excel(rosco_path, header=None)

    label_col = df.iloc[:, 1].astype(str)
    period_row_mask = label_col.str.contains(
        "monitoring period", case=False, na=False
    )

    if not period_row_mask.any():
        raise ValueError("Missing monitoring period label in Column B of Rosco")

    row_idx = period_row_mask.idxmax()

    if df.shape[1] <= 2:
        raise ValueError(
            f"Missing monitoring period, please fill cell C{row_idx + 1} of Rosco"
        )

    user_input_text = str(df.iloc[row_idx, 2]).strip()

    if not user_input_text or user_input_text.lower() == "nan":
        raise ValueError(
            f"Missing monitoring period in cell C{row_idx + 1} of Rosco"
        )

    found = re.findall(r"\d{4}-\d{2}-\d{2}", user_input_text)

    if len(found) < 2:
        raise ValueError(
            f"Invalid date format in cell C{row_idx + 1}. "
            "Expected two dates (YYYY-MM-DD)."
        )

    start_date = pd.to_datetime(found[0], format=DATE_FORMAT).date()
    end_date   = pd.to_datetime(found[1], format=DATE_FORMAT).date()

    if start_date > end_date:
        raise ValueError(
            f"Invalid monitoring period in cell C{row_idx + 1}: "
            "start date is after end date"
        )

    return start_date, end_date


# ----------------------------- 2️⃣ Load BSR -----------------------------
def detect_header_row_in_sheet(bsr_path, sheet_name):
    df_sample = pd.read_excel(
        bsr_path,
        sheet_name=sheet_name,   #  LOCKED to this sheet
        header=None,
        nrows=200
    )

    for i, row in df_sample.iterrows():
        row_str = " ".join(row.dropna().astype(str)).lower()

        if "region" in row_str and "market" in row_str and "broadcaster" in row_str:
            return i

        if "date" in row_str and ("utc" in row_str or "gmt" in row_str):
            return i

    raise ValueError(
        f"Header not found in sheet '{sheet_name}'"
    )


# def df(bsr_path):
#     header_row = detect_header_row(bsr_path)
#     df = pd.read_excel(bsr_path, header=header_row)
#     df.columns = [str(c).strip() for c in df.columns]
#     return df

def load_bsr(bsr_path):
    xl = pd.ExcelFile(bsr_path)

    allowed_sheets = {"worksheet", "database"}
    target_sheet = None

    #  Find ONLY worksheet / database
    for sheet in xl.sheet_names:
        if sheet.strip().lower() in allowed_sheets:
            target_sheet = sheet
            break

    if not target_sheet:
        raise ValueError(
            f"No valid sheet ('Worksheet' or 'Database') found in {os.path.basename(bsr_path)}"
        )

    #  Header detection ONLY on the chosen sheet
    header_row = detect_header_row_in_sheet(bsr_path, target_sheet)

    #  Load ONLY that sheet
    df = pd.read_excel(
        bsr_path,
        sheet_name=target_sheet,
        header=header_row
    )

    df.columns = [str(c).strip() for c in df.columns]
    return df

# ----------------------------- 3️⃣ Period Check -----------------------------
def period_check(bsr_df, start_date, end_date):

    bsr_df = bsr_df.copy()

    # Normalize monitoring period
    start_ts = pd.to_datetime(start_date).normalize()
    end_ts   = pd.to_datetime(end_date).normalize()

    # --- Explicit, SAFE column detection ---
    utc_col = None
    local_col = None

    for c in bsr_df.columns:
        cname = str(c).lower().replace(" ", "").replace("_", "")
        if "date" in cname and "utc" in cname:
            utc_col = c
        elif cname == "date":
            local_col = c

    # Safety check (optional but recommended)
    if utc_col is None and local_col is None:
        raise ValueError("No valid date columns found in BSR")

    # --- Safe datetime normalization ---
    def normalize_dt(series):
        if pd.api.types.is_datetime64_any_dtype(series):
            return series.dt.normalize()
        return pd.to_datetime(series, errors="coerce").dt.normalize()

    bsr_df["BSR_UTC_Date"] = (
        normalize_dt(bsr_df[utc_col]) if utc_col else pd.NaT
    )

    bsr_df["BSR_Local_Date"] = (
        normalize_dt(bsr_df[local_col]) if local_col else pd.NaT
    )

    # --- OR logic (FINAL business rule) ---
    utc_in_range = bsr_df["BSR_UTC_Date"].between(start_ts, end_ts)
    local_in_range = bsr_df["BSR_Local_Date"].between(start_ts, end_ts)

    bsr_df["Within_Period_OK"] = utc_in_range | local_in_range

    bsr_df["Within_Period_Remark"] = bsr_df["Within_Period_OK"].apply(
        lambda x: "" if x else "Date outside monitoring period"
    )

    return bsr_df


# ----------------------------- 4️⃣ Completeness Check -----------------------------
def completeness_check(df, bsr_cols, rules):
    colmap = {
        "tv_channel": _find_column(df, bsr_cols['tv_channel']),
        "channel_id": _find_column(df, bsr_cols.get('channel_id')),
        "type_of_program": _find_column(df, bsr_cols.get('type_of_program')),
        "match_day": _find_column(df, bsr_cols.get('matchday') or bsr_cols.get('match_day', [])),
        "home_team": _find_column(df, bsr_cols.get('home_team')),
        "away_team": _find_column(df, bsr_cols.get('away_team')),
        "aud_estimates": _find_column(df, bsr_cols.get('aud_estimates')),
        "aud_metered": _find_column(df, bsr_cols.get('aud_metered')),
        "source": _find_column(df, bsr_cols.get('source'))
    }
    df["Completeness_OK"] = True
    df["Completeness_Remark"] = ""
    live_types = set(rules.get('live_types', ['live', 'repeat', 'delayed']))
    relaxed_types = set(rules.get('relaxed_types', ['highlights']))
    for idx, row in df.iterrows():
        missing = []
        for logical, display in [("tv_channel", "TV Channel"), ("channel_id", "Channel ID"),
                                 ("match_day", "Match Day"), ("source", "Source")]:
            colname = colmap.get(logical)
            if colname is None:
                missing.append(f"{display} (column not found)")
            elif not _is_present(row.get(colname)):
                missing.append(display)
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
        type_col = colmap.get("type_of_program")
        prog_type = str(row.get(type_col) or "").strip().lower() if type_col else ""
        home_col, away_col = colmap.get("home_team"), colmap.get("away_team")
        if prog_type in live_types:
            if not home_col: missing.append("Home Team (column not found)")
            elif not _is_present(row.get(home_col)): missing.append("Home Team")
            if not away_col: missing.append("Away Team (column not found)")
            elif not _is_present(row.get(away_col)): missing.append("Away Team")
        elif prog_type not in relaxed_types:
            if home_col and not _is_present(row.get(home_col)): missing.append("Home Team")
            if away_col and not _is_present(row.get(away_col)): missing.append("Away Team")
        if missing:
            df.at[idx, "Completeness_OK"] = False
            df.at[idx, "Completeness_Remark"] = "; ".join(missing)
        else:
            df.at[idx, "Completeness_Remark"] = "All key fields present"
    return df


# ----------------------------- 5️⃣ Overlap / Duplicate / Day Break -----------------------------
def overlap_duplicate_daybreak_check(df, bsr_cols, rules):
    import pandas as pd

    df = df.copy()

    # --------------------------------------------------
    # Column resolution
    # --------------------------------------------------
    col_channel = _find_column(df, bsr_cols.get("tv_channel"))
    col_channel_id = _find_column(df, bsr_cols.get("channel_id"))
    col_market = _find_column(df, bsr_cols.get("market"))
    col_broadcaster = _find_column(df, bsr_cols.get("broadcaster"))

    col_date = (
        _find_column(df, ["Date (UTC)", "Date (UTC/GMT)"])
        or _find_column(df, ["Date"])
    )
    col_start = (
        _find_column(df, ["Start (UTC)", "Start UTC"])
        or _find_column(df, ["Start"])
    )
    col_end = (
        _find_column(df, ["End (UTC)", "End UTC"])
        or _find_column(df, ["End"])
    )

    col_prog_type = (
        _find_column(df, bsr_cols.get("type_of_program"))
        or _find_column(df, ["Program Type", "Type of Program"])
    )

    if not col_market or not col_date or not col_start or not col_end:
        return df

    compare_channel = col_channel if col_channel else col_channel_id
    if not compare_channel:
        return df

    # --------------------------------------------------
    # Build timezone-naive datetimes ONLY
    # --------------------------------------------------
    def build_dt(d, t):
        if pd.isna(d) or pd.isna(t):
            return pd.NaT
        try:
            date_part = pd.to_datetime(d, errors="coerce")
            if pd.isna(date_part):
                return pd.NaT

            ts = pd.to_datetime(
                f"{date_part.date()} {t}",
                errors="coerce"
            )

            # FORCE tz-naive
            if isinstance(ts, pd.Timestamp) and ts.tzinfo is not None:
                ts = ts.tz_localize(None)

            return ts
        except Exception:
            return pd.NaT

    df["_start_dt"] = df.apply(lambda r: build_dt(r[col_date], r[col_start]), axis=1)
    df["_end_dt"]   = df.apply(lambda r: build_dt(r[col_date], r[col_end]), axis=1)

    # HARD safety: ensure tz-naive
    df["_start_dt"] = pd.to_datetime(df["_start_dt"], errors="coerce").dt.tz_localize(None)
    df["_end_dt"]   = pd.to_datetime(df["_end_dt"], errors="coerce").dt.tz_localize(None)

    # --------------------------------------------------
    # Fix cross-midnight programs
    # --------------------------------------------------
    mask_midnight = (
        pd.notna(df["_start_dt"]) &
        pd.notna(df["_end_dt"]) &
        (df["_end_dt"] < df["_start_dt"])
    )
    df.loc[mask_midnight, "_end_dt"] += pd.Timedelta(days=1)

    # --------------------------------------------------
    # Normalize program type
    # --------------------------------------------------
    df["_prog_type_norm"] = (
        df[col_prog_type].fillna("").astype(str).str.lower().str.strip()
        if col_prog_type else ""
    )

    # --------------------------------------------------
    # Preserve original order
    # --------------------------------------------------
    df["_orig_idx"] = df.index

    # --------------------------------------------------
    # ✅ CRITICAL: FULL CHRONOLOGICAL SORT
    # Channel → Market → Date → Start time
    # --------------------------------------------------
    df["_sort_date"] = df["_start_dt"].dt.date

    df = df.sort_values(
        [compare_channel, col_market, "_sort_date", "_start_dt"],
        na_position="last"
    ).reset_index(drop=True)

    n = len(df)

    # --------------------------------------------------
    # Output containers
    # --------------------------------------------------
    overlap_ok   = [pd.NA] * n
    overlap_r    = [""] * n
    duplicate_ok = [True] * n
    duplicate_r  = [""] * n
    daybreak_ok  = [pd.NA] * n
    daybreak_r   = [""] * n

    # --------------------------------------------------
    # Duplicate check (unchanged)
    # --------------------------------------------------
    dup_cols = [compare_channel, col_market, col_date, col_start, col_end]
    if col_broadcaster:
        dup_cols.insert(2, col_broadcaster)

    try:
        dup_mask = df.duplicated(subset=dup_cols, keep=False)
    except Exception:
        dup_mask = pd.Series([False] * n)

    for i in range(n):
        if dup_mask.iloc[i]:
            duplicate_ok[i] = False
            duplicate_r[i] = "In-market duplicate (same channel/market/date/start/end)"

    # --------------------------------------------------
    # ✅ OVERLAP CHECK (NO GROUPBY — ORDER SAFE)
    # --------------------------------------------------
    VALID_TYPES = {"live", "repeat", "delayed"}

    prev_key = None
    prev_end = None

    for i in range(n):
        row = df.iloc[i]
        key = (row[compare_channel], row[col_market], row["_sort_date"])
        start = row["_start_dt"]
        end = row["_end_dt"]
        ptype = row["_prog_type_norm"]

        if key != prev_key:
            prev_end = None

        if ptype and ptype not in VALID_TYPES:
            overlap_ok[i] = pd.NA
            overlap_r[i] = f"Ignored program type '{ptype}'"
            prev_key = key
            continue

        if pd.isna(start) or pd.isna(end) or end <= start:
            overlap_ok[i] = pd.NA
            overlap_r[i] = "Not Applicable – invalid timing"
            prev_key = key
            continue

        if prev_end is None:
            overlap_ok[i] = True
            overlap_r[i] = "OK (first program)"
            prev_end = end
            prev_key = key
            continue

        if start == prev_end:
            overlap_ok[i] = True
            overlap_r[i] = "OK – back-to-back"
            prev_end = end
            prev_key = key
            continue

        if start < prev_end:
            overlap_ok[i] = False
            overlap_r[i] = (
                f"Overlap: starts {start.time()} "
                f"before previous ends {prev_end.time()}"
            )
            prev_end = max(prev_end, end)
            prev_key = key
            continue

        overlap_ok[i] = True
        overlap_r[i] = "OK"
        prev_end = end
        prev_key = key

    # --------------------------------------------------
    # Daybreak logic (unchanged)
    # --------------------------------------------------
    gap_tolerance = rules.get("daybreak_gap_tolerance_min", 5)

    for i in range(1, n):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]

        if not (
            str(prev[compare_channel]) == str(curr[compare_channel]) and
            str(prev[col_market]) == str(curr[col_market])
        ):
            continue

        if pd.isna(prev["_end_dt"]) or pd.isna(curr["_start_dt"]):
            daybreak_ok[i] = pd.NA
            daybreak_r[i] = "Not Applicable – missing timestamps"
            continue

        if prev["_end_dt"].hour >= 23 and curr["_start_dt"].hour <= 1:
            gap = (curr["_start_dt"] - prev["_end_dt"]).total_seconds() / 60
            if 0 <= gap <= gap_tolerance:
                daybreak_ok[i] = True
                daybreak_r[i] = "Valid midnight continuation"
            else:
                daybreak_ok[i] = False
                daybreak_r[i] = f"Invalid continuation gap ({gap:.1f} min)"
        else:
            daybreak_ok[i] = pd.NA
            daybreak_r[i] = "Not Applicable"

    # --------------------------------------------------
    # Final assignment
    # --------------------------------------------------
    df["Duplicate_OK"] = duplicate_ok
    df["Duplicate_Remark"] = duplicate_r
    df["Overlap_OK"] = overlap_ok
    df["Overlap_Remark"] = overlap_r
    df["Daybreak_OK"] = daybreak_ok
    df["Daybreak_Remark"] = daybreak_r

    return df.sort_values("_orig_idx").drop(
        columns=[
            "_start_dt",
            "_end_dt",
            "_sort_date",
            "_orig_idx",
            "_prog_type_norm"
        ],
        errors="ignore"
    )


# ----------------------------- 6️⃣ Program Category Check -----------------------------
def program_category_check(bsr_path, df, col_map, rules, file_rules):
    # ======================================================
    # 0. SAFE INIT
    # ======================================================
    df = df.copy()
    df["Program_Category_Expected"] = pd.NA
    df["Program_Category_Remark"] = ""
    df["Program_Category_OK"] = False

    # ======================================================
    # 1. NORMALIZED COLUMN FINDER (AUTONOMOUS)
    # ======================================================
    def normalize(s):
        s = str(s).lower()
        s = s.replace("programme", "program")
        s = re.sub(r"[^a-z0-9]", "", s)
        return s

    def find_any(dfx, candidates):
        norm_cols = {normalize(c): c for c in dfx.columns}
        for cand in candidates:
            n = normalize(cand)
            for nc, original in norm_cols.items():
                if n == nc or n in nc or nc in n:
                    return original
        return None

    # ======================================================
    # 2. AUTODETECT BSR COLUMNS (NO col_map DEPENDENCY)
    # ======================================================
    col_home = find_any(df, ["home team", "team home", "home"])
    col_away = find_any(df, ["away team", "team away", "away"])
    col_date = find_any(df, ["date", "utc date", "date utc", "date(utc)"])
    col_start = find_any(df, ["start", "start utc", "program start"])
    col_type = find_any(df, [
        "program type", "programme type",
        "type of program", "type of programme"
    ])

    missing = [k for k, v in {
        "Home Team": col_home,
        "Away Team": col_away,
        "Date": col_date,
        "Start": col_start,
        "Program Type": col_type
    }.items() if v is None]

    if missing:
        df["Program_Category_Remark"] = f"Missing BSR columns: {', '.join(missing)}"
        return df

    # ======================================================
    # 3. FIXTURE SHEET SAFE LOAD
    # ======================================================
    try:
        xl = pd.ExcelFile(bsr_path)
    except Exception:
        df["Program_Category_Remark"] = "Unable to read BSR file"
        return df

    fixture_kw = file_rules.get("fixture_sheet_keyword", "fixture")
    fixture_kw = fixture_kw if isinstance(fixture_kw, list) else [fixture_kw]

    fixture_sheet = next(
        (s for s in xl.sheet_names if any(k.lower() in s.lower() for k in fixture_kw)),
        None
    )

    if not fixture_sheet:
        df["Program_Category_Remark"] = "Fixture sheet missing"
        return df

    df_fix = xl.parse(fixture_sheet)

    # ======================================================
    # 4. AUTODETECT FIXTURE COLUMNS
    # ======================================================
    col_home_f = find_any(df_fix, ["home team", "home"])
    col_away_f = find_any(df_fix, ["away team", "away"])
    col_date_f = find_any(df_fix, ["date", "fixture date"])
    col_start_f = find_any(df_fix, ["start", "start time", "kickoff"])

    if any(v is None for v in [col_home_f, col_away_f, col_date_f, col_start_f]):
        df["Program_Category_Remark"] = "Missing Fixture columns"
        return df

    # ======================================================
    # 5. NORMALIZE DATA
    # ======================================================
    def clean(x):
        return re.sub(r"\s+", " ", str(x).lower()).strip()

    df["_home"] = df[col_home].map(clean)
    df["_away"] = df[col_away].map(clean)
    df["_event"] = df["_home"] + "||" + df["_away"]
    df["_start"] = pd.to_datetime(
        df[col_date].astype(str) + " " + df[col_start].astype(str),
        errors="coerce"
    )
    df["Program_Category_Actual"] = df[col_type].str.lower().str.strip()

    df_fix["_home"] = df_fix[col_home_f].map(clean)
    df_fix["_away"] = df_fix[col_away_f].map(clean)
    df_fix["_start"] = pd.to_datetime(
        df_fix[col_date_f].astype(str) + " " + df_fix[col_start_f].astype(str),
        errors="coerce"
    )

    df = df.sort_values(["_event", "_start"])

    LIVE_TOL = rules.get("live_tolerance_min", 35)

    # ======================================================
    # 6. MAIN LOGIC
    # ======================================================
    for idx, row in df.iterrows():
        actual = row["Program_Category_Actual"]
        start = row["_start"]
        event = row["_event"]

        if actual in ("highlights", "magazine", "magazine & support"):
            df.at[idx, "Program_Category_Expected"] = actual
            df.at[idx, "Program_Category_Remark"] = "Non-live respected"
            continue

        fx = df_fix[
            (df_fix["_home"] == row["_home"]) &
            (df_fix["_away"] == row["_away"])
        ]

        if fx.empty or pd.isna(start):
            df.at[idx, "Program_Category_Remark"] = "No matching fixture"
            continue

        fix_start = fx["_start"].iloc[0]
        first_start = df.loc[df["_event"] == event, "_start"].min()

        if start != first_start:
            df.at[idx, "Program_Category_Expected"] = "repeat"
            df.at[idx, "Program_Category_Remark"] = "Repeat"
            continue

        diff = (start - fix_start).total_seconds() / 60

        if abs(diff) <= LIVE_TOL:
            df.at[idx, "Program_Category_Expected"] = "live"
            df.at[idx, "Program_Category_Remark"] = "Live"
        elif diff > LIVE_TOL:
            df.at[idx, "Program_Category_Expected"] = "delayed"
            df.at[idx, "Program_Category_Remark"] = "Delayed"
        else:
            df.at[idx, "Program_Category_Remark"] = "Before fixture"

    # ======================================================
    # 7. QC FLAG
    # ======================================================
    df["Program_Category_OK"] = (
        df["Program_Category_Actual"] == df["Program_Category_Expected"]
    )

    df.drop(columns=["_home", "_away", "_event", "_start"], inplace=True, errors="ignore")
    return df

# 8️⃣ Event / Matchday / Competition Check
def check_event_matchday_competition(df_worksheet,df_fixtures, rosco_path=None, debug_rows=20):
    """
    Validate Worksheet rows against Fixture List using exact match.

    Logic:
    - Use Event column if present, else fallback to Competition
    - Match on:
        Event/Competition + Matchday + Home Team + Away Team
    - If exact match exists in Fixture List → OK
    - Else → fail with remark
    """

    # ---------- helpers ----------
    def norm(x):
        if pd.isna(x):
            return ""
        return str(x).strip().lower()

    def get_col(df, possible_names):
        for c in df.columns:
            if c.strip().lower() in possible_names:
                return c
        return None

    # ---------- resolve column names ----------
    ws_event_col = get_col(df_worksheet, {"event"})
    ws_comp_col = get_col(df_worksheet, {"competition"})
    ws_matchday_col = get_col(df_worksheet, {"matchday", "match day"})
    ws_home_col = get_col(df_worksheet, {"home team", "hometeam", "home"})
    ws_away_col = get_col(df_worksheet, {"away team", "awayteam", "away"})

    fx_event_col = get_col(df_fixtures, {"event"})
    fx_comp_col = get_col(df_fixtures, {"competition"})
    fx_matchday_col = get_col(df_fixtures, {"matchday", "match day"})
    fx_home_col = get_col(df_fixtures, {"home team", "hometeam", "home"})
    fx_away_col = get_col(df_fixtures, {"away team", "awayteam", "away"})

    # ---------- build fixture lookup set ----------
    fixture_keys = set()

    for _, r in df_fixtures.iterrows():
        event_val = norm(r.get(fx_event_col)) or norm(r.get(fx_comp_col))
        key = (
            event_val,
            norm(r.get(fx_matchday_col)),
            norm(r.get(fx_home_col)),
            norm(r.get(fx_away_col))
        )
        fixture_keys.add(key)

    # ---------- prepare output ----------
    df = df_worksheet.copy()
    df["Event_Matchday_Competition_OK"] = False
    df["Event_Matchday_Competition_Remark"] = ""

    # ---------- row-wise validation ----------
    for idx, r in df.iterrows():
        event_val = norm(r.get(ws_event_col)) or norm(r.get(ws_comp_col))
        matchday = norm(r.get(ws_matchday_col))
        home = norm(r.get(ws_home_col))
        away = norm(r.get(ws_away_col))

        key = (event_val, matchday, home, away)

        if key in fixture_keys and all(key):
            df.at[idx, "Event_Matchday_Competition_OK"] = True
            df.at[idx, "Event_Matchday_Competition_Remark"] = "OK"
        else:
            df.at[idx, "Event_Matchday_Competition_OK"] = False
            df.at[idx, "Event_Matchday_Competition_Remark"] = "Exact match not found in fixture"

    # ---------- debug ----------
    print("=== Exact Fixture Match QC (sample rows) ===")
    for i in range(min(debug_rows, len(df))):
        r = df.iloc[i]
        print(
            f"[Row {i}] Event/Comp='{norm(r.get(ws_event_col)) or norm(r.get(ws_comp_col))}' | "
            f"MD='{r.get(ws_matchday_col)}' | "
            f"Home='{r.get(ws_home_col)}' | Away='{r.get(ws_away_col)}' | "
            f"OK={r['Event_Matchday_Competition_OK']} | "
            f"Remark={r['Event_Matchday_Competition_Remark']}"
        )
    print("=== End QC ===\n")

    return df

# -----------------------------------------------------------
# 9️⃣ Market / Channel / Program / Duration Consistency Check

def market_channel_consistency_check(df_bsr, rosco_path, col_map, file_rules):
    logging.info("🔍 Starting Market & Channel Consistency Check...")
    bsr_cols = col_map['bsr']
    rosco_cols = col_map.get('rosco', {})
    def normalize_channel(name):
        if pd.isna(name) or name is None:
            return ""
        s = str(name)
        s = re.sub(r"\(.*?\)|\[.*?\]", "", s)
        s = re.split(r"[-–—]", s)[0]
        s = re.sub(r"[^0-9a-zA-Z\s]", " ", s)
        return re.sub(r"\s+", " ", s).strip().lower()
    rosco_df = None
    if rosco_path:
        try:
            xls = pd.ExcelFile(rosco_path)
            ignore_sheet = file_rules.get('rosco_ignore_sheet', 'general')
            sheet_name = next((s for s in xls.sheet_names if ignore_sheet not in s.lower()), None)
            if sheet_name:
                rosco_df = xls.parse(sheet_name)
            else:
                logging.warning(f"No valid sheet found in ROSCO (ignoring '{ignore_sheet}').")
        except Exception as e:
            logging.error(f"Error loading ROSCO file: {e}")
            df_bsr["Market_Channel_Consistency_OK"] = False
            df_bsr["Market_Channel_Program_Remark"] = f"Error loading ROSCO: {e}"
            return df_bsr
    valid_pairs = set()
    rosco_country_col = rosco_cols.get('channel_country', 'ChannelCountry')
    rosco_name_col = rosco_cols.get('channel_name', 'ChannelName')
    if rosco_df is not None and not rosco_df.empty and {rosco_country_col, rosco_name_col}.issubset(rosco_df.columns):
        for _, row in rosco_df.iterrows():
            market = str(row[rosco_country_col]).strip().lower()
            channel = normalize_channel(row[rosco_name_col])
            if market and channel:
                valid_pairs.add((market, channel))
        logging.info(f"Loaded {len(valid_pairs)} valid Market+Channel pairs from ROSCO.")
    df_bsr["Market_Channel_Consistency_OK"] = True
    df_bsr["Market_Channel_Program_Remark"] = "OK"
    bsr_market_col = _find_column(df_bsr, bsr_cols.get('market'))
    bsr_channel_col = _find_column(df_bsr, bsr_cols.get('tv_channel'))
    if not bsr_market_col or not bsr_channel_col:
        logging.error("Market/Channel Check: BSR columns not found. Skipping.")
        df_bsr["Market_Channel_Consistency_OK"] = False
        df_bsr["Market_Channel_Program_Remark"] = "BSR columns not found"
        return df_bsr
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
def domestic_market_check(df_worksheet, bsr_cols, monitoring_start_date=None, debug=False):
    df = df_worksheet.copy()
    df["Domestic_Market_Coverage_OK"] = True
    df["Domestic_Market_Remark"] = ""
    col_comp = _find_column(df, bsr_cols.get('competition', ['Competition']))
    col_mkt = _find_column(df, bsr_cols.get('market', ['Market']))
    col_date = _find_column(df, bsr_cols.get('date', ['Date']))
    col_prog_type = _find_column(df, bsr_cols.get('type_of_program', ['Type of Program']))
    if not all([col_comp, col_mkt, col_date, col_prog_type]):
        df["Domestic_Market_Coverage_OK"] = False
        df["Domestic_Market_Remark"] = "Skipped: Missing core BSR columns in file/config."
        return df
    DOMESTIC_MAP = {
        "premier league": ["united kingdom", "england"],
        "epl": ["united kingdom", "england"],
        "la liga": ["spain"],
        "bundesliga": ["germany", "deutschland"],
        "serie a": ["italy"],
        "ligue 1": ["france"]
    }
    monitoring_start = None
    if monitoring_start_date is not None:
        try:
            monitoring_start = pd.to_datetime(monitoring_start_date).date()
        except Exception:
            monitoring_start = None
    for idx, row in df.iterrows():
        comp = str(row.get(col_comp, "")).strip().lower()
        market = str(row.get(col_mkt, "")).strip().lower()
        date_raw = row.get(col_date)
        try:
            row_date = pd.to_datetime(date_raw).date()
        except Exception:
            row_date = None
        if monitoring_start and row_date and row_date < monitoring_start:
            continue
        domestic_markets = []
        for comp_kw, markets in DOMESTIC_MAP.items():
            if comp_kw in comp:
                domestic_markets = markets
                break
        if not domestic_markets:
            continue
        market_ok = any(dm in market for dm in domestic_markets)
        if not market_ok:
            df.at[idx, "Domestic_Market_Coverage_OK"] = False
            df.at[idx, "Domestic_Market_Remark"] = f"Missing domestic coverage. Expected one of: {domestic_markets}"
        else:
            df.at[idx, "Domestic_Market_Remark"] = "OK"
    return df

# -----------------------------------------------------------
# 11️⃣ Rates & Ratings Check
# --------------------------------------------
def rates_and_ratings_check(df, bsr_cols):
    est_col = _find_column(df, bsr_cols.get('aud_estimates'))
    met_col = _find_column(df, bsr_cols.get('aud_metered'))
    est_col_exists = est_col is not None and est_col in df.columns
    met_col_exists = met_col is not None and met_col in df.columns
    if est_col is None:
        est_col = "Audience_Estimates_Dummy"
        df[est_col] = np.nan
        logging.warning("Rates/Ratings Check: Audience Estimates column not found.")
    if met_col is None:
        met_col = "Audience_Metered_Dummy"
        df[met_col] = np.nan
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
    if est_col == "Audience_Estimates_Dummy" and est_col in df.columns:
        df.drop(columns=[est_col], inplace=True)
    if met_col == "Audience_Metered_Dummy" and met_col in df.columns:
        df.drop(columns=[met_col], inplace=True)
    return df

# -----------------------------------------------------------
# 12️⃣ Comparison of Duplicated Markets
def duplicated_market_check(df_bsr, macro_path, project, col_map, file_rules, debug=False):

    result_col = "Duplicated_Markets_Check_OK"
    remark_col = "Duplicated_Markets_Remark"

    df_bsr[result_col] = pd.NA
    df_bsr[remark_col] = "Not Applicable"

    league_keyword = str(project.get("league_keyword", "F24 Spain")).lower()
    bsr_cols = col_map["bsr"]
    macro_cols = col_map["macro"]

    if not macro_path or not os.path.exists(macro_path):
        df_bsr[result_col] = False
        df_bsr[remark_col] = "Macro file missing"
        return df_bsr


    # -------------------------------------------------------
    #  STEP 1 — Load Excel WITHOUT trusting header_row
    # -------------------------------------------------------
    try:
        xl = pd.ExcelFile(macro_path, engine="openpyxl")

        # Pick correct sheet
        preferred = file_rules.get("macro_sheet_name", "Data Core").lower()
        sheet = next((s for s in xl.sheet_names if s.lower() == preferred), xl.sheet_names[0])

        # Read top 20 rows without header
        tmp = pd.read_excel(macro_path, sheet_name=sheet, header=None, nrows=20, dtype=str)

        required_cols = ["Projects", "Orig Market", "Orig Channel", "Dup Market", "Dup Channel"]

        header_row_index = None

        #  Find the row where all required column names appear
        for i in range(len(tmp)):
            row_vals = [str(x).strip().lower() for x in list(tmp.iloc[i].values)]
            if all(any(req.lower() == val for val in row_vals) for req in required_cols):
                header_row_index = i
                break

        if header_row_index is None:
            df_bsr[result_col] = False
            df_bsr[remark_col] = "Could not locate header row in macro file."
            return df_bsr

        # Now correctly load macro_df using detected header row
        macro_df = pd.read_excel(
            macro_path,
            sheet_name=sheet,
            header=header_row_index,
            dtype=str,
            engine="openpyxl"
        )

        macro_df.columns = [str(c).strip() for c in macro_df.columns]

    except Exception as e:
        df_bsr[result_col] = False
        df_bsr[remark_col] = f"Macro load error: {e}"
        return df_bsr


    # -------------------------------------------------------
    #  STEP 2 — Find required columns reliably
    # -------------------------------------------------------
    def find_col(df, key):
        if isinstance(key, list):
            candidates = key
        else:
            candidates = [key]

        lower = {c.lower(): c for c in df.columns}
        for cand in candidates:
            c = str(cand).strip().lower()
            if c in lower:
                return lower[c]
        return None

    proj_col = find_col(macro_df, macro_cols["projects"])
    orig_mkt_col = find_col(macro_df, macro_cols["orig_market"])
    orig_ch_col = find_col(macro_df, macro_cols["orig_channel"])
    dup_mkt_col = find_col(macro_df, macro_cols["dup_market"])
    dup_ch_col = find_col(macro_df, macro_cols["dup_channel"])

    missing = [col for col in [proj_col, orig_mkt_col, orig_ch_col, dup_mkt_col, dup_ch_col] if col is None]
    if missing:
        df_bsr[result_col] = False
        df_bsr[remark_col] = "Macro file columns not found (after auto-detect)."
        return df_bsr


    # -------------------------------------------------------
    #  STEP 3 — Filter by project keyword
    # -------------------------------------------------------
    macro_df = macro_df[
        macro_df[proj_col].astype(str).str.lower().str.contains(league_keyword, na=False)
    ]

    if macro_df.empty:
        df_bsr[result_col] = pd.NA
        df_bsr[remark_col] = f"No duplication rules found for {league_keyword}"
        return df_bsr


    # -------------------------------------------------------
    #  STEP 4 — Run duplication checks (unchanged logic)
    # -------------------------------------------------------
    mkt_col = find_col(df_bsr, bsr_cols["market"])
    ch_col = find_col(df_bsr, bsr_cols["tv_channel"])
    comp_col = find_col(df_bsr, bsr_cols["competition"])
    evt_col = find_col(df_bsr, bsr_cols["event"])

    in_league = (
        df_bsr[comp_col].astype(str).str.lower().str.contains(league_keyword, na=False)
        | df_bsr[evt_col].astype(str).str.lower().str.contains(league_keyword, na=False)
    )

    df_bsr.loc[~in_league, result_col] = pd.NA
    df_bsr.loc[~in_league, remark_col] = "Not Applicable"

    df_league = df_bsr[in_league].copy()

    for _, r in macro_df.iterrows():
        orig_market = str(r[orig_mkt_col]).strip().lower()
        orig_channel = str(r[orig_ch_col]).strip().lower()
        dup_market = str(r[dup_mkt_col]).strip().lower()
        dup_channel = str(r[dup_ch_col]).strip().lower()

        orig_rows = df_league[
            (df_league[mkt_col].str.lower() == orig_market) &
            (df_league[ch_col].str.lower() == orig_channel)
        ]
        dup_rows = df_league[
            (df_league[mkt_col].str.lower() == dup_market) &
            (df_league[ch_col].str.lower() == dup_channel)
        ]

        orig_events = set(orig_rows[evt_col].dropna().str.lower().str.strip())
        dup_events = set(dup_rows[evt_col].dropna().str.lower().str.strip())

        if not orig_events:
            status = pd.NA
            remark = f"No events found for {orig_market}/{orig_channel}"
        elif orig_events.issubset(dup_events):
            status = True
            remark = f"All {len(orig_events)} events duplicated"
        else:
            missing = orig_events - dup_events
            status = False
            remark = f"Missing {len(missing)} events"

        mask = (
            (df_bsr[mkt_col].str.lower() == orig_market) &
            (df_bsr[ch_col].str.lower() == orig_channel) &
            in_league
        ) | (
            (df_bsr[mkt_col].str.lower() == dup_market) &
            (df_bsr[ch_col].str.lower() == dup_channel) &
            in_league
        )

        df_bsr.loc[mask, result_col] = status
        df_bsr.loc[mask, remark_col] = remark

    return df_bsr
# -----------------------------------------------------------
# 13️⃣ Country & Channel IDs Check
def country_channel_id_check(df, bsr_cols):
    """
    Check consistency of channel IDs per (market, tv_channel) pair.

    RULE:
    - For each (Market, TV-Channel) pair → must have exactly ONE unique non-blank Channel ID.
    - If same pair appears with different non-blank Channel IDs → inconsistent.
    - If the only channel_id is blank → inconsistent (Missing channel ID).
    - Same TV-Channel across different markets is allowed (treated independently).

    Adds these columns:
        Market_Channel_ID_OK (bool)
        Market_Channel_ID_Remark (str)
    """

    df = df.copy()  # work on a copy to avoid side-effects
    df["Market_Channel_ID_OK"] = True
    df["Market_Channel_ID_Remark"] = "OK"

    # Resolve column names (use _find_column)
    ch_col = _find_column(df, bsr_cols.get("tv_channel"))
    ch_id_col = _find_column(df, bsr_cols.get("channel_id"))
    mkt_col = _find_column(df, bsr_cols.get("market"))

    if not all([ch_col, ch_id_col, mkt_col]):
        logging.warning("Country/Channel ID Check: Missing required columns. Skipping.")
        df["Market_Channel_ID_OK"] = False
        df["Market_Channel_ID_Remark"] = "Check skipped: missing required columns"
        return df

    def norm(x):
        if pd.isna(x) or x is None:
            return ""
        return str(x).strip()

    # Build mapping: (market, tv_channel) → set(channel_ids) & row indices
    pair_ids = {}
    pair_idxs = {}

    for idx, row in df.iterrows():
        market = norm(row.get(mkt_col, ""))
        channel = norm(row.get(ch_col, ""))
        channel_id = norm(row.get(ch_id_col, ""))

        # Normalize for comparisons (lower-case for market and channel)
        key = (market.lower(), channel.lower())

        pair_ids.setdefault(key, set()).add(channel_id)
        pair_idxs.setdefault(key, []).append(idx)

    # Evaluate each pair
    for key, id_set in pair_ids.items():
        idxs = pair_idxs.get(key, [])
        # consider only non-blank IDs for uniqueness check
        non_blank_ids = {cid for cid in id_set if cid != ""}

        inconsistent = False
        remark = "OK"

        if len(non_blank_ids) == 0:
            inconsistent = True
            remark = "Missing channel ID"
        elif len(non_blank_ids) > 1:
            inconsistent = True
            # keep blanks visible as <BLANK> if present
            ids_list = [cid if cid != "" else "<BLANK>" for cid in sorted(id_set)]
            # include market/channel for clarity in remark
            market_display, channel_display = key
            remark = f"Conflicting Channel IDs for {channel_display} in market {market_display}: {', '.join(ids_list)}"
        else:
            inconsistent = False
            remark = "OK"

        for i in idxs:
            df.at[i, "Market_Channel_ID_OK"] = not inconsistent
            df.at[i, "Market_Channel_ID_Remark"] = remark

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

    if "Summary" in wb.sheetnames:
        del wb["Summary"]

    ws = wb.create_sheet("Summary")

    qc_columns = [col for col in df.columns if col.endswith("_OK")]

    summary_rows = []

    for col in qc_columns:
        series = df[col]

        passed = series.eq(True).sum()
        failed = series.eq(False).sum()
        total = passed + failed

        summary_rows.append([
            col,
            int(total),
            int(passed),
            int(failed)
        ])

    summary_df = pd.DataFrame(
        summary_rows,
        columns=["Check", "Total Evaluated", "Passed", "Failed"]
    )

    for r in dataframe_to_rows(summary_df, index=False, header=True):
        ws.append(r)

    wb.save(output_path)