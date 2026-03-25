import pandas as pd
import logging
import re
import os
from .common import _find_column

def check_event_matchday_competition(df_worksheet, df_fixtures, rosco_path=None, debug_rows=20):
    """
    Validate Worksheet rows against Fixture List using exact match.

    Logic:
    - Use Event column if present, else fallback to Competition
    - Match on:
        Event/Competition + Matchday + Home Team + Away Team
    - If exact match exists in Fixture List -> OK
    - Else -> fail with remark
    """

    # ---------- helpers ----------
    def norm(x):
        if pd.isna(x):
            return ""
        return str(x).strip().lower()

    def get_col(df, possible_names):
        for c in df.columns:
            c_norm = str(c).strip().lower()   # FIX: cast to string
            if c_norm in possible_names:
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

    # ---------- debug (Optional: remove in production if too noisy) ----------
    # print("=== Exact Fixture Match QC (sample rows) ===")
    # for i in range(min(debug_rows, len(df))):
    #     r = df.iloc[i]
    #     print(
    #         f"[Row {i}] Event/Comp='{norm(r.get(ws_event_col)) or norm(r.get(ws_comp_col))}' | "
    #         f"MD='{r.get(ws_matchday_col)}' | "
    #         f"Home='{r.get(ws_home_col)}' | Away='{r.get(ws_away_col)}' | "
    #         f"OK={r['Event_Matchday_Competition_OK']} | "
    #         f"Remark={r['Event_Matchday_Competition_Remark']}"
    #     )
    # print("=== End QC ===\n")

    return df

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

        # Skip rows before monitoring start (if defined)
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