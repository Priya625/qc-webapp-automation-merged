import pandas as pd
import re
from datetime import datetime, timedelta, time
from .common import _find_column

def overlap_duplicate_daybreak_check(df, bsr_cols, rules):
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
    # Channel -> Market -> Date -> Start time
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
    overlap_ok    = [pd.NA] * n
    overlap_r     = [""] * n
    duplicate_ok = [True] * n
    duplicate_r  = [""] * n
    daybreak_ok  = [pd.NA] * n
    daybreak_r   = [""] * n

    # --------------------------------------------------
    # Duplicate check (IGNORE INTERNET / WWW)
    # --------------------------------------------------
    col_pay_free = _find_column(df, ["Pay/Free TV", "Pay Free TV", "Platform", "Distribution"])

    dup_cols = [compare_channel, col_market, col_date, col_start, col_end]
    if col_broadcaster:
        dup_cols.insert(2, col_broadcaster)

    # Default: no duplicates
    dup_mask = pd.Series([False] * n)

    try:
        if col_pay_free:
            # Identify internet / www rows (case-insensitive, partial match)
            internet_mask = (
                df[col_pay_free]
                .fillna("")
                .astype(str)
                .str.lower()
                .str.contains(r"internet|internet stream|www", regex=True)
            )

            # Run duplicate check ONLY on non-internet rows
            non_internet_df = df.loc[~internet_mask, dup_cols]
            dup_non_internet = non_internet_df.duplicated(keep=False)

            # Map results back to full DataFrame
            dup_mask.loc[~internet_mask] = dup_non_internet.values
        else:
            # No Pay/Free TV column -> original behavior
            dup_mask = df.duplicated(subset=dup_cols, keep=False)

    except Exception:
        dup_mask = pd.Series([False] * n)

    # Assign results
    for i in range(n):
        if dup_mask.iloc[i]:
            duplicate_ok[i] = False
            duplicate_r[i] = "In-market duplicate (same channel/market/date/start/end)"

    # --------------------------------------------------
    # ✅ OVERLAP CHECK (WITH INTERNET / MATCH BYPASS)
    # --------------------------------------------------
    VALID_TYPES = {"live", "repeat", "delayed"}

    col_combined = _find_column(df, ["Combined"])
    col_phase = _find_column(df, ["Phase/Fixture/Episode", "Phase / Fixture / Episode"])
    col_prog_desc = _find_column(df, ["Program Description", "Program Desc"])

    def normalize_match_text(text):
        if not text:
            return ""
        text = str(text).lower()
        text = re.sub(r"(simulcast|live|repeat|delayed)", "", text)
        text = re.sub(r"\bvs\.?\b|\bv\b", "vs", text)
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def get_match_signature(row):
        parts = []
        if col_combined:
            parts.append(str(row[col_combined] or ""))
        if col_phase:
            parts.append(str(row[col_phase] or ""))
        if col_prog_desc:
            parts.append(str(row[col_prog_desc] or ""))
        return normalize_match_text(" ".join(parts))

    def is_internet_row(row):
        if not col_pay_free:
            return False
        val = str(row[col_pay_free] or "").lower()
        return "internet" in val or "www" in val

    prev_key = None
    prev_end = None
    prev_row = None
    prev_match = None

    for i in range(n):
        row = df.iloc[i]
        key = (row[compare_channel], row[col_market], row["_sort_date"])
        start = row["_start_dt"]
        end = row["_end_dt"]
        ptype = row["_prog_type_norm"]

        if key != prev_key:
            prev_end = None
            prev_row = None
            prev_match = None

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
            prev_row = row
            prev_match = get_match_signature(row)
            prev_key = key
            continue

        if start == prev_end:
            overlap_ok[i] = True
            overlap_r[i] = "OK – back-to-back"
            prev_end = end
            prev_row = row
            prev_match = get_match_signature(row)
            prev_key = key
            continue

        if start < prev_end:
            # ---- INTERNET / MATCH BYPASS LOGIC ----
            if (
                ptype in VALID_TYPES and
                is_internet_row(row) and
                prev_row is not None and
                is_internet_row(prev_row)
            ):
                curr_match = get_match_signature(row)

                if curr_match and prev_match and curr_match != prev_match:
                    overlap_ok[i] = True
                    overlap_r[i] = "OK – Internet simulcast with different match"
                    prev_end = max(prev_end, end)
                    prev_row = row
                    prev_match = curr_match
                    prev_key = key
                    continue

            overlap_ok[i] = False
            overlap_r[i] = (
                f"Overlap: starts {start.time()} "
                f"before previous ends {prev_end.time()}"
            )
            prev_end = max(prev_end, end)
            prev_row = row
            prev_match = get_match_signature(row)
            prev_key = key
            continue

        overlap_ok[i] = True
        overlap_r[i] = "OK"
        prev_end = end
        prev_row = row
        prev_match = get_match_signature(row)
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


def program_category_check(bsr_path, df, col_map, rules, file_rules):
    import pandas as pd
    import re
    from datetime import datetime, timedelta, time

    # -------------------------
    # 0. GET USER CONFIGURATION
    # --------------------------------------------------
    try:
        # Default to 35 minutes if not provided by user
        tol_min = float(rules.get("live_tolerance_min", 35))
    except (ValueError, TypeError):
        tol_min = 35.0
    
    tolerance = timedelta(minutes=tol_min)

    # -------------------------
    # Helpers
    # -------------------------
    def find_col(names):
        for n in names:
            for c in df.columns:
                if c.strip().lower() == n.strip().lower():
                    return c
        return None

    def parse_date(val):
        if pd.isna(val):
            return None
        if isinstance(val, (datetime, pd.Timestamp)):
            return val.date()
        return pd.to_datetime(val, dayfirst=True, errors="coerce").date()

    def parse_time(val):
        if pd.isna(val):
            return None
        if isinstance(val, time):
            return val
        if isinstance(val, (datetime, pd.Timestamp)):
            return val.time()
        if isinstance(val, str) and not val.strip().startswith("#"):
            return pd.to_datetime(val, errors="coerce").time()
        return None

    # -------------------------
    # Column detection
    # -------------------------
    col_program_type = find_col(["program type", "type of program"])
    col_desc = find_col(["combined (translated)", "program description", "description"])
    col_duration = find_col(["duration", "duration (mins)"])

    col_date_utc = find_col(["date(utc)"])
    col_start_utc = find_col(["start(utc)"])
    col_end_utc = find_col(["end(utc)"])

    col_home = find_col(["home team"])
    col_away = find_col(["away team"])

    # ✅ FIXED: Use standard naming convention for Excel coloring
    df["Program_Category_OK"] = ""
    df["Program_Category_Remark"] = ""
    
    # ✅ NEW: Audit column to prove which tolerance was used
    df["QC_Config_Category_Used"] = f"Live Tolerance: {tol_min} min"

    if not col_program_type:
        df["Program_Category_Remark"] = "Skipped: Program Type column not found"
        return df

    # -------------------------
    # Keywords
    # -------------------------
    highlight_re = re.compile(
        r"\b(hits|hl|highlights|hlts|overview|review|show|goals?|summary|specials|league|reload)\b",
        re.I
    )

    magazine_re = re.compile(
        r"\b(sports|show|league|magazine|support|studio|magazin|weekly|preview|analysis|review|specials|weekly new|coming soon)\b",
        re.I
    )

    # -------------------------
    # Load fixtures
    # -------------------------
    fixtures_df = None
    try:
        xl = pd.ExcelFile(bsr_path)
        for s in xl.sheet_names:
            if "fixture" in s.lower():
                fixtures_df = xl.parse(s)
                break
    except Exception:
        fixtures_df = None

    # -------------------------
    # Precompute BSR UTC start
    # -------------------------
    df["_bsr_start_utc"] = None
    for i, r in df.iterrows():
        d = parse_date(r.get(col_date_utc))
        t = parse_time(r.get(col_start_utc))
        if d and t:
            df.at[i, "_bsr_start_utc"] = datetime.combine(d, t)

    # -------------------------
    # First broadcast map
    # -------------------------
    first_broadcast = {}
    for _, r in df.iterrows():
        if not r["_bsr_start_utc"]:
            continue
        key = (
            str(r.get(col_home)).strip().lower(),
            str(r.get(col_away)).strip().lower(),
            r["_bsr_start_utc"].date()
        )
        if key not in first_broadcast or r["_bsr_start_utc"] < first_broadcast[key]:
            first_broadcast[key] = r["_bsr_start_utc"]

    # -------------------------
    # Validation
    # -------------------------
    for idx, row in df.iterrows():
        ptype = str(row[col_program_type]).strip().lower()
        desc = str(row[col_desc]) if col_desc else ""
        bsr_start = row["_bsr_start_utc"]

        # ===== HIGHLIGHTS =====
        if ptype == "highlights":
            try:
                dur = float(row[col_duration])
            except Exception:
                dur = 0

            if not highlight_re.search(desc):
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = "Missing highlight keywords"
            elif dur <= 10:
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = "Highlight duration <= 10 minutes"
            else:
                df.at[idx, "Program_Category_OK"] = True
                df.at[idx, "Program_Category_Remark"] = "Valid Highlights program"

        # ===== MAGAZINE & SUPPORT =====
        elif ptype in ["magazine & support", "magazine and support"]:
            if not magazine_re.search(desc):
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = "Missing magazine/support keywords"
            else:
                df.at[idx, "Program_Category_OK"] = True
                df.at[idx, "Program_Category_Remark"] = "Valid Magazine & Support program"

        # ===== LIVE =====
        elif ptype == "live":
            if fixtures_df is None or not bsr_start:
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = "Invalid Live timing or fixtures missing"
                continue

            home = str(row[col_home]).strip().lower()
            away = str(row[col_away]).strip().lower()

            matched = False
            for _, fx in fixtures_df.iterrows():
                fx_date = parse_date(fx.get("Date"))
                fx_st = parse_time(fx.get("Start Time"))
                fx_et = parse_time(fx.get("End Time"))
                if not fx_date or not fx_st or not fx_et:
                    continue

                fx_start = datetime.combine(fx_date, fx_st)
                fx_end = datetime.combine(fx_date, fx_et)
                if fx_end <= fx_start:
                    fx_end += timedelta(days=1)

                if (
                    home == str(fx.get("Home Team", "")).strip().lower()
                    and away == str(fx.get("Away Team", "")).strip().lower()
                    and abs(bsr_start - fx_start) <= tolerance
                ):
                    matched = True
                    break

            if matched:
                df.at[idx, "Program_Category_OK"] = True
                df.at[idx, "Program_Category_Remark"] = f"Valid Live program (within {tol_min}m)"
            else:
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = f"Live program outside {tol_min}m tolerance"

        # ===== DELAYED =====
        elif ptype == "delayed":
            home = str(row[col_home]).strip().lower()
            away = str(row[col_away]).strip().lower()
            first_time = first_broadcast.get((home, away, bsr_start.date())) if bsr_start else None

            if not bsr_start or not first_time:
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = "Unable to determine first broadcast"
            elif bsr_start == first_time:
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = "First broadcast cannot be Delayed"
            elif abs(bsr_start - first_time) <= tolerance:
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = f"Delayed program falls within Live tolerance ({tol_min}m)"
            else:
                df.at[idx, "Program_Category_OK"] = True
                df.at[idx, "Program_Category_Remark"] = "Valid Delayed first broadcast"

        # ===== REPEAT =====
        elif ptype == "repeat":
            home = str(row[col_home]).strip().lower()
            away = str(row[col_away]).strip().lower()
            first_time = first_broadcast.get((home, away, bsr_start.date())) if bsr_start else None

            if not bsr_start or not first_time:
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = "Unable to determine first broadcast for Repeat"
            elif bsr_start > first_time:
                df.at[idx, "Program_Category_OK"] = True
                df.at[idx, "Program_Category_Remark"] = "Valid Repeat broadcast"
            else:
                df.at[idx, "Program_Category_OK"] = False
                df.at[idx, "Program_Category_Remark"] = "First broadcast cannot be Repeat"

        else:
            df.at[idx, "Program_Category_OK"] = pd.NA
            df.at[idx, "Program_Category_Remark"] = "Program type not applicable"

    # Cleanup
    df.drop(columns=["_bsr_start_utc"], inplace=True, errors="ignore")
    return df