import pandas as pd
import re
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



# ----------------------------- 1️⃣ Detect Monitoring Period -----------------------------
def detect_period_from_rosco(rosco_path):
    df = pd.read_excel(rosco_path, header=None)
    value_col = df.iloc[:, 1].astype(str)
    period_row = value_col[value_col.str.contains("Monitoring Periods", na=False)]
    if period_row.empty:
        raise ValueError("Could not find 'Monitoring Periods' in Rosco file.")
    text = period_row.iloc[0]
    found = re.findall(r"\d{4}-\d{2}-\d{2}", text)
    if len(found) >= 2:
        start_date = pd.to_datetime(found[0], format=DATE_FORMAT)
        end_date = pd.to_datetime(found[1], format=DATE_FORMAT)
        return start_date, end_date
    else:
        raise ValueError("Could not parse monitoring period dates from Rosco file.")


# ----------------------------- 2️⃣ Load BSR -----------------------------
def detect_header_row(bsr_path):
    df_sample = pd.read_excel(bsr_path, header=None, nrows=200)
    for i, row in df_sample.iterrows():
        row_str = " ".join(row.dropna().astype(str).tolist()).lower()
        if "region" in row_str and "market" in row_str and "broadcaster" in row_str:
            return i
        if "date" in row_str and ("utc" in row_str or "gmt" in row_str):
            return i
    raise ValueError("Could not detect header row in BSR file.")


def load_bsr(bsr_path):
    header_row = detect_header_row(bsr_path)
    df = pd.read_excel(bsr_path, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ----------------------------- 3️⃣ Period Check -----------------------------
def period_check(df, start_date, end_date):
    date_col = next((c for c in df.columns if "date" in str(c).lower()), None)
    if not date_col:
        df["Within_Period_OK"] = True
        df["Within_Period_Remark"] = ""
        return df
    df["Date_checked"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df["Within_Period_OK"] = df["Date_checked"].between(start_date.date(), end_date.date())
    df["Within_Period_Remark"] = df["Within_Period_OK"].apply(lambda x: "" if x else "Date outside monitoring period")
    return df


# ----------------------------- 4️⃣ Completeness Check -----------------------------
def completeness_check(df):
    keywords = ["channel", "aud", "price", "match"]
    matched_cols = [col for col in df.columns if any(kw in str(col).lower() for kw in keywords)]
    if not matched_cols:
        df["Completeness_OK"] = True
        df["Completeness_Remark"] = ""
        return df
    df["Completeness_OK"] = df[matched_cols].notna().all(axis=1)
    df["Completeness_Remark"] = df["Completeness_OK"].apply(lambda x: "" if x else "Missing key fields")
    return df


# ----------------------------- 5️⃣ Overlap / Duplicate / Day Break -----------------------------
def overlap_duplicate_daybreak_check(df):
    df_result = df.copy()
    channel_col = next((c for c in df.columns if "channel" in str(c).lower()), None)
    start_col = next((c for c in df.columns if "start" in str(c).lower()), None)
    end_col = next((c for c in df.columns if "end" in str(c).lower()), None)
    date_col = next((c for c in df.columns if "date" in str(c).lower()), None)

    for col in [start_col, end_col]:
        if col:
            df_result[col] = pd.to_datetime(df_result[col], errors="coerce")

    overlap_flags = [False] * len(df_result)
    if channel_col and start_col and end_col and date_col:
        df_sorted = df_result.sort_values(by=[channel_col, date_col, start_col]).reset_index(drop=True)
        prev_end = prev_channel = prev_date = None
        for i, row in df_sorted.iterrows():
            overlap = False
            if prev_channel == row[channel_col] and prev_date == row[date_col]:
                if pd.notna(row[start_col]) and pd.notna(prev_end) and row[start_col] < prev_end:
                    overlap = True
            overlap_flags[i] = overlap
            prev_end = row[end_col]
            prev_channel = row[channel_col]
            prev_date = row[date_col]
        df_result["No_Overlap"] = ~pd.Series(overlap_flags, index=df_sorted.index)
    else:
        df_result["No_Overlap"] = True

    df_result["No_Overlap_Remark"] = df_result["No_Overlap"].apply(lambda x: "" if x else "Overlap detected")

    # Duplicate check
    exclude_keywords = ["_ok", "within", "date_checked"]
    dup_cols = [c for c in df_result.columns if not any(x in str(c).lower() for x in exclude_keywords)]
    if dup_cols:
        hashes = pd.util.hash_pandas_object(df_result[dup_cols], index=False)
        df_result["Is_Duplicate"] = hashes.duplicated(keep=False)
    else:
        df_result["Is_Duplicate"] = False
    df_result["Is_Duplicate_OK"] = ~df_result["Is_Duplicate"]
    df_result["Is_Duplicate_Remark"] = df_result["Is_Duplicate"].apply(lambda x: "" if not x else "Duplicate row found")

    # Day break check
    if start_col and end_col:
        df_result["Day_Break_OK"] = ~((df_result[start_col].dt.day != df_result[end_col].dt.day) &
                                      (df_result[start_col].dt.hour >= 20))
    else:
        df_result["Day_Break_OK"] = True
    df_result["Day_Break_Remark"] = df_result["Day_Break_OK"].apply(lambda x: "" if x else "Day break mismatch")
    return df_result


# ----------------------------- 6️⃣ Program Category Check -----------------------------
def program_category_check(bsr_path, df, col_map, rules, file_rules):
    # ---------- Load fixture sheet ----------
    xl = pd.ExcelFile(bsr_path)
    # 1. Get keyword(s) from config (which might be a list)
    fixture_keywords = file_rules.get("fixture_sheet_keyword", ["fixture"])
    
    # Ensure fixture_keywords is always a list for iteration
    if not isinstance(fixture_keywords, list):
        fixture_keywords = [fixture_keywords]
    
    # 2. Iterate through all keywords to find a matching sheet
    fixture_sheet = None
    for s in xl.sheet_names:
        s_lower = s.lower()
        if any(kw.lower() in s_lower for kw in fixture_keywords):
            fixture_sheet = s
            break

    if not fixture_sheet:
        df["Program_Category_Expected"] = pd.NA
        df["Program_Category_Actual"] = ""
        df["Program_Category_OK"] = False
        df["Program_Category_Remark"] = "Fixture sheet missing"
        return df

    df_fix = xl.parse(fixture_sheet)

    # ---------- Column detection ----------
    b = col_map["bsr"]
    f = col_map["fixture"]

    col_home_bsr = _find_column(df, b.get("home_team"))
    col_away_bsr = _find_column(df, b.get("away_team"))
    col_date_bsr = _find_column(df, b.get("date"))
    col_start_bsr = _find_column(df, b.get("start_time"))
    col_progtype = _find_column(df, b.get("type_of_program"))
    col_broadcaster = _find_column(df, b.get("broadcaster"))

    col_combined = _find_column(df, b.get("combined"))
    col_prog_desc = _find_column(df, b.get("program_description"))
    col_prog_title = _find_column(df, b.get("program_title"))
    col_duration = _find_column(df, b.get("duration"))

    col_home_fix = _find_column(df_fix, f.get("home_team"))
    col_away_fix = _find_column(df_fix, f.get("away_team"))
    col_date_fix = _find_column(df_fix, f.get("date"))
    col_start_fix = _find_column(df_fix, f.get("start_time"))

    # ---------- Required columns check ----------
    req = [col_home_bsr, col_away_bsr, col_date_bsr, col_start_bsr]
    if any(c is None for c in req):
        df["Program_Category_Expected"] = pd.NA
        df["Program_Category_Actual"] = df[col_progtype] if col_progtype else ""
        df["Program_Category_OK"] = False
        df["Program_Category_Remark"] = "Missing required columns for LIVE check"
        return df

    # ---------- Helpers ----------
    def clean(x):
        if pd.isna(x):
            return ""
        x = str(x).strip().lower()
        x = re.sub(r"[^\w\s&]", " ", x)  # keep & since "Magazine & Support" uses it
        return re.sub(r"\s+", " ", x).strip()

    def parse_datetime(d, t):
        try:
            return combine_parse(d, t)
        except:
            return pd.NaT

    def parse_duration_minutes(x):
        if pd.isna(x):
            return None
        try:
            num = float(x)
            if 0 <= num <= 10000:
                return int(num)
        except Exception:
            pass
        s = str(x).strip().lower()
        m = re.match(r"^(\d+):(\d+)(?::(\d+))?$", s)
        if m:
            parts = [int(p) for p in m.groups() if p is not None]
            if len(parts) == 2:
                h_or_m, m_or_s = parts
                if h_or_m > 5:
                    return h_or_m * 60 + m_or_s
                else:
                    return int(round((h_or_m * 60 + m_or_s) / 60.0))
            elif len(parts) == 3:
                h, mm, ss = parts
                return h * 60 + mm + round(ss / 60)
        m2 = re.search(r"(\d+)", s)
        if m2:
            return int(m2.group(1))
        return None

    def contains_any_keyword(text, keywords):
        if not text:
            return False
        txt = clean(text)
        for kw in keywords:
            k = re.sub(r"[^\w\s&]", " ", str(kw)).strip().lower()
            if not k:
                continue
            if re.search(rf"\b{re.escape(k)}\b", txt):
                return True
        return False

    # ---------- Prepare fixture lookup ----------
    df_fix["_home"] = df_fix[col_home_fix].map(clean)
    df_fix["_away"] = df_fix[col_away_fix].map(clean)
    df_fix["_date"] = pd.to_datetime(df_fix[col_date_fix], errors="coerce").dt.date
    df_fix["_start"] = [
        parse_datetime(df_fix.at[i, col_date_fix], df_fix.at[i, col_start_fix])
        for i in df_fix.index
    ]

    # ---------- Prepare BSR ----------
    df["_home"] = df[col_home_bsr].map(clean)
    df["_away"] = df[col_away_bsr].map(clean)
    df["_event_key"] = df["_home"] + "||" + df["_away"]
    df["_date"] = pd.to_datetime(df[col_date_bsr], errors="coerce").dt.date
    df["_start"] = [
        parse_datetime(df.at[i, col_date_bsr], df.at[i, col_start_bsr])
        for i in df.index
    ]
    df["_broad"] = df[col_broadcaster].astype(str).str.lower().str.strip() if col_broadcaster else ""

    # normalize actual to lower-case comparable form
    df["Program_Category_Actual"] = (
        df[col_progtype].astype(str).str.lower().str.strip() if col_progtype else ""
    )

    # combined text for keyword searches
    def get_combined_text(row):
        parts = []
        for c in (col_combined, col_prog_desc, col_prog_title):
            if c and pd.notna(row.get(c, "")):
                parts.append(str(row[c]))
        return " ".join(parts).strip()

    df["_combined_text"] = df.apply(get_combined_text, axis=1).astype(str)
    df["_duration_min"] = df[col_duration].apply(parse_duration_minutes) if col_duration else None

    # keywords & bounds
    highlights_keywords = ["hits", "highlights", "post", "review", "overview", "recap", "summary"]
    magazine_keywords = ["pre", "post", "studio", "interview", "analysis", "previo"]
    dur_min_bound, dur_max_bound = rules.get("flag_duration_min", 10), rules.get("flag_duration_max", 50)

    df["Program_Category_Expected"] = pd.NA
    df["Program_Category_Remark"] = ""

    LIVE_TOL = rules.get("live_tolerance_min", 35)

    # ---------- MAIN LOOP ----------
    for idx, row in df.iterrows():
        ev_key = row["_event_key"]
        h = row["_home"]
        a = row["_away"]
        d = row["_date"]
        bsr_start = row["_start"]
        actual = row["Program_Category_Actual"]
        combined_text = row["_combined_text"]
        dur_min = row["_duration_min"] if col_duration else None

        # ---------- 1) HIGHLIGHTS / MAGAZINE & SUPPORT - OVERRIDE (must NOT use fixture) ----------
        dur_ok = True
        if col_duration:
            if dur_min is None:
                dur_ok = False
            else:
                dur_ok = (dur_min_bound <= dur_min <= dur_max_bound)

        # If actual explicitly labels it, respect that first (normalized)
        if isinstance(actual, str) and actual == "highlights":
            df.at[idx, "Program_Category_Expected"] = "highlights"
            df.at[idx, "Program_Category_Remark"] = "Detected as Highlights (Program type)"
            # override everything else
            continue

        if isinstance(actual, str) and actual in ("magazine", "magazine & support", "magazine & support".lower()):
            # set expected exactly to the normalized form used by actual when present
            df.at[idx, "Program_Category_Expected"] = "magazine & support"
            df.at[idx, "Program_Category_Remark"] = "Detected as Magazine & Support (Program type)"
            continue

        # Keyword+duration based detection (also overrides)
        if dur_ok and contains_any_keyword(combined_text, highlights_keywords):
            df.at[idx, "Program_Category_Expected"] = "highlights"
            df.at[idx, "Program_Category_Remark"] = f"Detected as Highlights (duration {dur_min} min & keyword match)"
            continue

        if dur_ok and contains_any_keyword(combined_text, magazine_keywords):
            df.at[idx, "Program_Category_Expected"] = "magazine & support"
            df.at[idx, "Program_Category_Remark"] = f"Detected as Magazine & Support (duration {dur_min} min & keyword match)"
            continue

        # ---------- 2) REPEAT LOGIC (runs before fixture/live/delayed) ----------
        if actual == "repeat":
            same_event = df[df["_event_key"] == ev_key]
            earlier = same_event[
                pd.to_datetime(same_event["_start"], errors="coerce") <
                pd.to_datetime(bsr_start, errors="coerce")
            ]
            if not earlier.empty:
                first_time = pd.to_datetime(earlier["_start"], errors="coerce").min()
                diff = (bsr_start - first_time).total_seconds() / 60
                df.at[idx, "Program_Category_Expected"] = "repeat"
                df.at[idx, "Program_Category_Remark"] = f"Repeat (earlier BSR broadcast exists, {diff:.1f} min earlier)"
                continue
            else:
                df.at[idx, "Program_Category_Expected"] = pd.NA
                df.at[idx, "Program_Category_Remark"] = "Repeat flagged but no earlier BSR broadcast found"
                continue

        # ---------- 3) FIXTURE LOOKUP (only used for live/delayed/repeat) ----------
        fixture_rows = df_fix[
            (df_fix["_home"] == h)
            & (df_fix["_away"] == a)
            & (df_fix["_date"] == d)
        ]

        # If no fixture, do NOT set "No matching fixture found" — leave Expected as NA and blank remark
        if fixture_rows.empty:
            # leave Program_Category_Expected as pd.NA (unless set earlier)
            # do not set remark to "No matching fixture"
            continue

        # fixture exists -> evaluate live/delayed/repeat
        fix_start = fixture_rows["_start"].iloc[0]
        if pd.isna(bsr_start) or pd.isna(fix_start):
            df.at[idx, "Program_Category_Expected"] = pd.NA
            df.at[idx, "Program_Category_Remark"] = "Invalid datetime"
            continue

        diff_min = abs((bsr_start - fix_start).total_seconds() / 60)

        # LIVE
        if diff_min <= LIVE_TOL:
            df.at[idx, "Program_Category_Expected"] = "live"
            df.at[idx, "Program_Category_Remark"] = f"Live (within ±{LIVE_TOL} min)"
            continue

        # DELAYED: only if this is the earliest BSR for the event AND occurs after fixture start
        same_event = df[df["_event_key"] == ev_key]
        earliest = pd.to_datetime(same_event["_start"], errors="coerce").min()

        if pd.to_datetime(bsr_start, errors="coerce") == earliest:
            # ensure it's after fixture start
            if (bsr_start - fix_start).total_seconds() > 0:
                df.at[idx, "Program_Category_Expected"] = "delayed"
                remark = f"Delayed (first telecast outside window; diff {diff_min:.1f} min)"
                if actual != "delayed":
                    remark = remark + f"; note: Program_Type actual='{actual}'"
                df.at[idx, "Program_Category_Remark"] = remark
                continue
            else:
                df.at[idx, "Program_Category_Expected"] = pd.NA
                df.at[idx, "Program_Category_Remark"] = "Broadcast recieved before the fixture start"
                continue
        else:
            # not earliest -> repeat
            df.at[idx, "Program_Category_Expected"] = "repeat"
            later_diff = (bsr_start - earliest).total_seconds() / 60
            df.at[idx, "Program_Category_Remark"] = f"Repeat (first telecast was {later_diff:.1f} min earlier)"
            continue

    # ---------- FINAL OK ----------
    df["Program_Category_OK"] = df["Program_Category_Actual"] == df["Program_Category_Expected"]

    # cleanup internal cols
    df.drop(columns=["_home", "_away", "_event_key", "_date", "_start", "_broad", "_combined_text", "_duration_min"],
            errors="ignore", inplace=True)

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
    # 🔥 STEP 1 — Load Excel WITHOUT trusting header_row
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

        # 🔍 Find the row where all required column names appear
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
    # 🔥 STEP 2 — Find required columns reliably
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
    # 🔥 STEP 3 — Filter by project keyword
    # -------------------------------------------------------
    macro_df = macro_df[
        macro_df[proj_col].astype(str).str.lower().str.contains(league_keyword, na=False)
    ]

    if macro_df.empty:
        df_bsr[result_col] = pd.NA
        df_bsr[remark_col] = f"No duplication rules found for {league_keyword}"
        return df_bsr


    # -------------------------------------------------------
    # 🔥 STEP 4 — Run duplication checks (unchanged logic)
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
def country_channel_id_check(df):
    """
    Ensures that each channel and market is mapped to a single, consistent ID.
    Outputs two columns:
      - Market_Channel_ID_OK (True/False)
      - Market_Channel_ID_Remark (string)
    """

    df_result = df.copy()
    df_result["Market_Channel_ID_OK"] = True
    df_result["Market_Channel_ID_Remark"] = ""

    def norm(x):
        return str(x).strip() if pd.notna(x) else ""

    # Maps to track consistency
    channel_id_map = {}
    market_id_map = {}

    for idx, row in df_result.iterrows():
        channel = norm(row.get("TV-Channel"))
        channel_id = norm(row.get("Channel ID"))
        market = norm(row.get("Market"))
        market_id = norm(row.get("Market ID"))

        remarks = []
        ok = True

        # ✅ Check 1 – Same channel shouldn't have multiple Channel IDs
        if channel:
            if channel in channel_id_map and channel_id_map[channel] != channel_id:
                remarks.append(
                    f"Channel '{channel}' has multiple IDs ({channel_id_map[channel]} vs {channel_id})"
                )
                ok = False
            else:
                channel_id_map[channel] = channel_id

        # ✅ Check 2 – Same market shouldn't have multiple Market IDs
        if market:
            if market in market_id_map and market_id_map[market] != market_id:
                remarks.append(
                    f"Market '{market}' has multiple IDs ({market_id_map[market]} vs {market_id})"
                )
                ok = False
            else:
                market_id_map[market] = market_id

        # ✅ Check 3 – Same Channel ID shouldn't be used for multiple channels
        if channel_id and list(channel_id_map.values()).count(channel_id) > 1:
            remarks.append(f"Channel ID '{channel_id}' assigned to multiple channels")
            ok = False

        # ✅ Check 4 – Same Market ID shouldn't be used for multiple markets
        if market_id and list(market_id_map.values()).count(market_id) > 1:
            remarks.append(f"Market ID '{market_id}' assigned to multiple markets")
            ok = False

        # ✅ Write results
        df_result.at[idx, "Market_Channel_ID_OK"] = ok
        df_result.at[idx, "Market_Channel_ID_Remark"] = "; ".join(remarks) if remarks else "OK"

    return df_result

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