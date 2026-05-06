import pandas as pd


def extract_general_info_entities(general_df):

    events = set()
    sports = set()
    seasons = set()

    current_key = None

    for _, row in general_df.iterrows():

        key = str(row.get("Name", "")).strip()
        value = str(row.get("Value", "")).strip()

        if "sports" in key.lower():
            current_key = "sports"
        elif "events" in key.lower():
            current_key = "events"
        elif "seasons" in key.lower():
            current_key = "seasons"

        elif value:
            if current_key == "sports":
                sports.add(value.lower())
            elif current_key == "events":
                events.add(value.lower())
            elif current_key == "seasons":
                seasons.add(value.lower())

    return {
        "sports": sports,
        "events": events,
        "seasons": seasons
    }

def duplicate_aid_final_dpmm(mm_df):
    df = mm_df.copy()
    df.columns = df.columns.str.strip()

    # Mapping to DPMM specific columns
    # We use 'progr. start' as it uniquely identifies the broadcast slot in this file
    group_cols = [
        "programme category",
        "country",
        "channel",
        "programme",
        "progr. start" 
    ]

    # Reference column for AID in DPMM is "aID (MM)"
    aid_col = "aID (MM)"

    # Create combo id
    df["_combo_id"] = df.groupby(group_cols).ngroup()

    # Count AIDs per combination (Check if one slot has multiple AIDs)
    df["_aid_count_per_combo"] = df.groupby(group_cols)[aid_col].transform("nunique")

    # Count combinations per AID (Check if one AID is used for different slots)
    df["_combo_count_per_aid"] = df.groupby(aid_col)["_combo_id"].transform("nunique")

    flags = []
    remarks = []

    for _, row in df.iterrows():
        # PRIORITY 1: Same AID used across multiple combinations
        if row["_combo_count_per_aid"] > 1:
            flags.append(False)
            remarks.append(
                f"AID {row[aid_col]} is used across multiple program combinations"
            )

        # PRIORITY 2: Multiple AIDs for same combination
        elif row["_aid_count_per_combo"] > 1:
            flags.append(False)
            remarks.append(
                f"Multiple AIDs assigned to same program combination ({row['programme']} at {row['progr. start']})"
            )

        # 🟢 VALID
        else:
            flags.append(True)
            remarks.append("")

    df["Duplicate_AID_Check_Flag"] = flags
    df["Duplicate_AID_Check_Remark"] = remarks

    # Clean up
    df.drop(columns=["_combo_id", "_aid_count_per_combo", "_combo_count_per_aid"], inplace=True)

    return df


def audience_spotprice_check_dpmm(df):
    df = df.copy()
    df.columns = df.columns.str.strip()

    # DPMM Specific Column Names
    # Note: Using 'audience (in 000\'s)' based on Column S in image_043dd0.png
    audience_col = "audience (in 000's)"
    spot_price_col = "spot price"

    flags = []
    remarks = []

    for _, row in df.iterrows():
        # Get values safely
        aud_val = row.get(audience_col, None)
        spot_val = row.get(spot_price_col, None)

        # Logic to determine if "blank" (Checking for NaN, empty strings, OR 0)
        audience_blank = pd.isna(aud_val) or str(aud_val).strip() == "" or aud_val == 0
        spot_blank = pd.isna(spot_val) or str(spot_val).strip() == "" or spot_val == 0

        if audience_blank and spot_blank:
            flags.append(False)
            remarks.append("Audience and Spot Price both are missing or zero")

        elif audience_blank:
            flags.append(False)
            remarks.append("Audience value is missing or zero")

        elif spot_blank:
            flags.append(False)
            remarks.append("Spot Price is missing or zero")

        else:
            flags.append(True)
            remarks.append("")

    df["Audience_SpotPrice_Check_Flag"] = flags
    df["Audience_SpotPrice_Check_Remark"] = remarks

    return df

def program_category_check_dpmm(mm_df):
    df = mm_df.copy()
    df.columns = df.columns.str.strip()

    category_col = "programme category"

    # Expanded valid list to match DPMM export naming conventions
    valid_categories = [
        "live",
        "sport (live)",
        "sport (magazine)",
        "sport (highlights)",
        "magazine",
        "highlights",
        "delayed",
        "relive",
        "news",
        "sport (news)"
    ]

    flags = []
    remarks = []

    for _, row in df.iterrows():
        category = row.get(category_col)

        # Normalize for comparison
        if pd.isna(category):
            category_clean = ""
        else:
            category_clean = str(category).strip().lower()

        # ❌ Invalid / blank
        if category_clean == "":
            flags.append(False)
            remarks.append("Programme category is missing")

        elif category_clean not in valid_categories:
            flags.append(False)
            remarks.append(f"Invalid programme category: {category}")

        # ✅ Valid
        else:
            flags.append(True)
            remarks.append("")

    df["Program_Category_Check_Flag"] = flags
    df["Program_Category_Check_Remark"] = remarks

    return df

import re

def normalize_channel(name):
    if pd.isna(name):
        return ""
    name = str(name).lower()
    name = re.sub(r"\(.*?\)", "", name)   # remove brackets
    name = re.sub(r"[^a-z0-9]", "", name) # remove special chars
    return name.strip()


def channel_country_mapping_check_dpmm(mm_df, rosco_path):
    # We create a copy and strip/lowercase for internal logic consistency
    df = mm_df.copy()
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()

    # -----------------------------------
    # STEP 1: READ ROSCO FILE (Logic stays same)
    # -----------------------------------
    rosco_excel = pd.ExcelFile(rosco_path)
    mapping_dict = {}

    for sheet in rosco_excel.sheet_names:
        if "rosco" not in sheet.lower():
            continue

        temp = pd.read_excel(rosco_excel, sheet_name=sheet)
        temp.columns = temp.columns.str.strip().str.lower()

        if "channelname" in temp.columns and "channelcountry" in temp.columns:
            for _, row in temp.iterrows():
                # Note: normalize_channel() must be defined in your global scope
                ch_name = normalize_channel(row["channelname"])
                ch_country = str(row["channelcountry"]).strip().lower()

                if ch_name:
                    mapping_dict[ch_name] = ch_country

    # -----------------------------------
    # STEP 2: VALIDATE DPMM DATA
    # -----------------------------------
    flags = []
    remarks = []

    for _, row in df.iterrows():
        mm_channel_raw = row.get("channel")
        
        # FIX: In DPMM Export, the column is 'country' not 'channel country'
        mm_country_raw = row.get("country") 

        mm_channel = normalize_channel(mm_channel_raw)
        mm_country = str(mm_country_raw).strip().lower()

        # ❌ Channel not found in ROSCO
        if mm_channel not in mapping_dict:
            flags.append(False)
            remarks.append(f"Channel '{mm_channel_raw}' not found in ROSCO mapping")

        # ❌ Country mismatch
        elif mapping_dict[mm_channel] != mm_country:
            flags.append(False)
            remarks.append(
                f"Channel '{mm_channel_raw}' mapped to '{mapping_dict[mm_channel]}' but found as '{mm_country_raw}'"
            )

        # ✅ Valid
        else:
            flags.append(True)
            remarks.append("")

    # Restore original column casing for the output
    df.columns = original_cols
    df["Channel_Country_Check_Flag"] = flags
    df["Channel_Country_Check_Remark"] = remarks

    return df

def apt_bt_check_dpmm(mm_df, bt_threshold=None):
    df = mm_df.copy()
    # Store original columns to restore later
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()

    # DPMM column names (verified from glossary)
    apt_col = "apt"
    bt_col = "bt"
    category_col = "programme category"
    # Keeping this for compatibility, though usually missing in DPMM
    live_apt_col = "apt live" 

    flags = []
    remarks = []

    for _, row in df.iterrows():
        category = str(row.get(category_col, "")).lower()

        # Safe conversions
        try:
            apt = float(row.get(apt_col))
        except:
            apt = None

        try:
            bt = float(row.get(bt_col))
        except:
            bt = None

        # -----------------------------------
        # PRIORITY 1: Live / Relive APT < 50%
        # -----------------------------------
        # Added 'sport (live)' to match DPMM export category names
        live_categories = ["live", "relive", "sport (live)", "sport (live), spiel"]
        
        if any(cat in category for cat in live_categories) and apt is not None and bt is not None:
            # Using LaTeX for the logic: $APT < 0.5 \times BT$
            if apt < 0.5 * bt:
                flags.append(False)
                remarks.append("APT is less than 50% of BT for live/relive entry")
                continue

        # -----------------------------------
        # PRIORITY 2: Exceptionally high BT
        # -----------------------------------
        if bt_threshold is not None and bt is not None:
            if bt >= bt_threshold:
                flags.append(False)
                remarks.append(f"BT exceeds threshold ({bt_threshold})")
                continue

        #  VALID
        flags.append(True)
        remarks.append("")

    # Restore casing and add results
    df.columns = original_cols
    df["APT_BT_Check_Flag"] = flags
    df["APT_BT_Check_Remark"] = remarks

    return df

def season_monitoring_check_dpmm(mm_df, start_date, end_date):
    df = mm_df.copy()
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()

    # DPMM combined date-time column
    date_col = "progr. start"

    # Convert Streamlit input dates to datetime objects
    start = pd.to_datetime(start_date, errors="coerce")
    end = pd.to_datetime(end_date, errors="coerce")

    flags = []
    remarks = []

    for _, row in df.iterrows():
        # Extract date from the combined string (e.g., '01/03/2026 13:21:00')
        # We use dayfirst=True because European exports usually follow DD/MM/YYYY
        prog_date = pd.to_datetime(row.get(date_col), dayfirst=True, errors="coerce")

        if pd.isna(prog_date):
            flags.append(False)
            remarks.append("Invalid or missing programme start date")

        # Check if the date falls outside the selected window
        elif prog_date.date() < start.date() or prog_date.date() > end.date():
            flags.append(False)
            remarks.append(
                f"Date {prog_date.date()} is outside monitoring period ({start.date()} to {end.date()})"
            )

        else:
            flags.append(True)
            remarks.append("")

    # Restore column casing and add flags
    df.columns = original_cols
    df["Season_Check_Flag"] = flags
    df["Season_Check_Remark"] = remarks

    return df

def fixture_validation_check_dpmm(mm_df, o_fixture_df):
    df = mm_df.copy()
    m_fixture_df = o_fixture_df.copy()
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()
    m_fixture_df.columns = m_fixture_df.columns.str.strip().str.lower()

    # Required columns based on your logic
    required_cols = ["event", "matchday", "matchday date", "match"]

    # Safety Check: Verify columns exist in both files
    missing_in_df = [c for c in required_cols if c not in df.columns]
    if missing_in_df:
        # If columns are missing, we flag the whole sheet as uncheckable 
        # instead of crashing the Streamlit app.
        df["Fixture_Validation_Flag"] = False
        df["Fixture_Validation_Remark"] = f"Missing columns in DPMM: {missing_in_df}. Ensure you are using the 'MM matchdays' data."
        return df

    # Normalize data for comparison (lowercase and string)
    for col in required_cols:
        df[col] = df[col].astype(str).str.lower().str.strip()
        m_fixture_df[col] = m_fixture_df[col].astype(str).str.lower().str.strip()

    # Create a unique "fingerprint" for every valid match in the Fixture file
    fixture_set = set()
    for _, row in m_fixture_df.iterrows():
        key = (row["event"], row["matchday"], row["matchday date"], row["match"])
        fixture_set.add(key)

    flags = []
    remarks = []

    # Check each row in the DPMM data against the official fixture list
    for _, row in df.iterrows():
        key = (row["event"], row["matchday"], row["matchday date"], row["match"])

        if key in fixture_set:
            flags.append(True)
            remarks.append("")
        else:
            flags.append(False)
            remarks.append("Match details (Event/Matchday/Date/Match) do not match official Fixture file")

    df["Fixture_Validation_Flag"] = flags
    df["Fixture_Validation_Remark"] = remarks

    return df

def stadium_consistency_check_dpmm(mm_df):
    df = mm_df.copy()
    # Standardize column naming
    df.columns = df.columns.str.strip().str.lower()

    # Required columns for DPMM context
    required_cols = [
        "programme category",
        "matchday date",
        "stadium"
    ]

    # Verify column existence to prevent crashes
    for col in required_cols:
        if col not in df.columns:
            df["Stadium_Consistency_Flag"] = False
            df["Stadium_Consistency_Remark"] = f"Missing column: {col}. Check if 'MM matchdays' sheet is being used."
            return df

    # Optional but helpful identifiers
    match_col = "match" if "match" in df.columns else None
    team_col = "team" if "team" in df.columns else None

    # 1. Clean and Normalize Data
    df["programme category"] = df["programme category"].astype(str).str.lower()
    df["matchday date"] = df["matchday date"].astype(str).str.strip()
    df["stadium"] = df["stadium"].astype(str).str.strip().lower()

    # 2. Filter for Live Events
    # DPMM categories like 'Sport (live)' are captured here
    live_df = df[df["programme category"].str.contains("live", na=False)].copy()

    if live_df.empty:
        df["Stadium_Consistency_Flag"] = True
        df["Stadium_Consistency_Remark"] = "No live programs found to check."
        return df

    # 3. Create a unique ID for the Match/Team
    def get_id(row):
        m = str(row.get("match", "")).strip().lower()
        t = str(row.get("team", "")).strip().lower()
        if m and m not in ["", "nan"]: return m
        if t and t not in ["", "nan"]: return t
        return "unknown_id"

    live_df["identifier"] = live_df.apply(get_id, axis=1)

    # 4. Group by Identifier + Date and count unique Stadiums
    # If count > 1, the same match has different stadiums in different rows
    stats = live_df.groupby(["identifier", "matchday date"])["stadium"].nunique()
    invalid_keys = stats[stats > 1].index

    # 5. Apply Flags
    flags = []
    remarks = []

    for _, row in df.iterrows():
        cat = str(row["programme category"]).lower()
        if "live" not in cat:
            flags.append(True)
            remarks.append("")
            continue

        m_id = get_id(row)
        m_date = str(row["matchday date"]).strip()
        
        if (m_id, m_date) in invalid_keys:
            flags.append(False)
            remarks.append(f"Inconsistent Stadium: '{row['stadium']}' does not match other entries for this match/date")
        else:
            flags.append(True)
            remarks.append("")

    df["Stadium_Consistency_Flag"] = flags
    df["Stadium_Consistency_Remark"] = remarks

    return df

def event_quality_check_dpmm(mm_df):
    df = mm_df.copy()
    # Handle both casing styles to be safe
    df.columns = df.columns.str.strip()
    
    # Required columns
    category_col = "programme category"
    # In DPMM, the column is usually lowercase 'bt'
    bt_col = "bt" if "bt" in df.columns else ("BT" if "BT" in df.columns else None)

    if category_col not in df.columns:
        df["Event_Quality_Flag"] = False
        df["Event_Quality_Remark"] = f"Missing column: {category_col}"
        return df

    # Normalize category strings
    df[category_col] = df[category_col].astype(str).str.lower().str.strip()

    # Allowed keywords based on DPMM Export categories
    allowed_keywords = ["live", "delayed", "highlight", "highlights", "magazine", "news"]

    flags = []
    remarks = []

    for _, row in df.iterrows():
        category = row[category_col]

        # 1. Category Existence Check
        if not any(keyword in category for keyword in allowed_keywords):
            flags.append(False)
            remarks.append(f"Unrecognized category type: {category}")
            continue

        # 2. Minimum Duration Check for Live Programs
        # Logic: If it's a live sport, it should generally be longer than a few minutes.
        if "live" in category and bt_col:
            try:
                bt_value = float(row[bt_col])
                # Threshold check: In many exports, BT is in days. 
                # If your BT is in minutes, use 60. If BT is in days, 60 mins = ~0.0416
                # Assuming minutes for this logic:
                if bt_value < 45:  # Adjust this threshold based on your specific requirements
                    flags.append(False)
                    remarks.append(f"BT too low ({bt_value}) for a live program")
                    continue
            except (ValueError, TypeError):
                flags.append(False)
                remarks.append("Invalid or missing BT value")
                continue

        # ✅ DEFAULT PASS
        flags.append(True)
        remarks.append("")

    df["Event_Quality_Flag"] = flags
    df["Event_Quality_Remark"] = remarks

    return df

def home_market_check_dpmm(mm_df):
    df = mm_df.copy()
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()

    # DPMM Column Mapping
    # Note: In DPMM 'country' (Col K) is the broadcast market/channel country.
    # The team's origin country is often in the same column but identified by the 'home' team row.
    required_cols = [
        "match",
        "programme category",
        "home/away",
        "country"
    ]


    for col in required_cols:
        if col not in df.columns:
            df["Home_Market_Flag"] = False
            df["Home_Market_Remark"] = f"Missing column: {col}"
            return df

    # Normalize data
    df["match"] = df["match"].astype(str).str.strip().str.lower()
    df["programme category"] = df["programme category"].astype(str).str.strip().str.lower()
    df["home/away"] = df["home/away"].astype(str).str.strip().str.lower()
    df["country"] = df["country"].astype(str).str.strip().str.lower()

    # Filter for Live/Delayed where the actual match happens
    live_df = df[df["programme category"].str.contains("live|delayed", na=False)].copy()

    # All unique broadcast markets currently in this file
    all_markets_in_data = set(df["country"].dropna().unique())

    # Map: Match -> Set of all countries where it was broadcast
    match_market_map = (
        live_df.groupby("match")["country"]
        .apply(lambda x: set(x))
        .to_dict()
    )

    # Map: Match -> The "Home" team's country
    home_country_map = {}
    for _, row in live_df.iterrows():
        if row["home/away"] == "home":
            home_country_map[row["match"]] = row["country"]

    flags = []
    remarks = []

    for _, row in df.iterrows():
        category = row["programme category"]
        match = row["match"]

        # 1. Skip non-relevant categories
        if not ("live" in category or "delayed" in category):
            flags.append(True)
            remarks.append("")
            continue

        # 2. Skip if we can't identify the home country for this match
        if match not in home_country_map:
            flags.append(True)
            remarks.append("")
            continue

        home_country = home_country_map[match]
        available_broadcast_markets = match_market_map.get(match, set())

        # ✅ CASE 1: The match was broadcast in its home country
        if home_country in available_broadcast_markets:
            flags.append(True)
            remarks.append("")
            continue

        # ✅ CASE 2: The home country isn't even in our monitoring list (so it's okay it's missing)
        if home_country not in all_markets_in_data:
            flags.append(True)
            remarks.append(f"{home_country} not in monitored markets")
            continue

        #  CASE 3: The match exists, the home market is monitored, but no broadcast was found
        flags.append(False)
        remarks.append(f"Missing home market broadcast: {home_country}")

    df.columns = original_cols
    df["Home_Market_Check_Flag"] = flags
    df["Home_Market_Check_Remark"] = remarks

    return df

def ps_market_channel_check_dpmm(mm_df, rosco_df):
    df = mm_df.copy()
    # Store original column names to restore casing later
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()
    
    rosco_df = rosco_df.copy()
    rosco_df.columns = rosco_df.columns.str.strip()

    # 1. Normalize ROSCO (Master Reference)
    # Using the exact headers usually found in the Monitoring List sheet
    rosco_df["ChannelCountry"] = rosco_df["ChannelCountry"].astype(str).str.lower().str.strip()
    rosco_df["ChannelName"] = rosco_df["ChannelName"].astype(str).str.lower().str.strip()

    valid_markets = set(rosco_df["ChannelCountry"].dropna().unique())
    valid_channels = set(rosco_df["ChannelName"].dropna().unique())

    flags = []
    remarks = []

    # 2. Validate DPMM Data
    for _, row in df.iterrows():
        # DPMM Column Mapping: 'country' is the market, 'channel' is the station
        market = str(row.get("country", "")).lower().strip()
        channel = str(row.get("channel", "")).lower().strip()

        issues = []

        if market not in valid_markets:
            issues.append(f"Invalid market: {market}")

        if channel not in valid_channels:
            issues.append(f"Invalid channel: {channel}")

        if issues:
            flags.append(False)
            remarks.append(" | ".join(issues))
        else:
            flags.append(True)
            remarks.append("")

    # Restore casing and append results
    df.columns = original_cols
    df["PS_Market_Channel_Flag"] = flags
    df["PS_Market_Channel_Remark"] = remarks

    return df

def ps_content_check_dpmm(mm_df, rosco_df):
    df = mm_df.copy()
    # Save original columns to restore casing later
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()
    
    rosco_df = rosco_df.copy()
    rosco_df.columns = rosco_df.columns.str.strip()

    # 1. Normalize both datasets
    # DPMM column is 'programme'
    df["programme"] = df["programme"].astype(str).str.lower().str.strip()
    # ROSCO column is 'ChannelPrograms'
    rosco_df["ChannelPrograms"] = rosco_df["ChannelPrograms"].astype(str).str.lower().str.strip()

    # Create a unique set of approved program names
    valid_programs = set(rosco_df["ChannelPrograms"].dropna().unique())

    flags = []
    remarks = []

    # 2. Compare every row
    for _, row in df.iterrows():
        prog = row.get("programme", "")

        if prog not in valid_programs:
            flags.append(False)
            remarks.append(f"Programme name '{prog}' is not in the approved ROSCO list")
        else:
            flags.append(True)
            remarks.append("")

    # Restore casing and add results
    df.columns = original_cols
    df["PS_Content_Flag"] = flags
    df["PS_Content_Remark"] = remarks

    return df


def mm_bsr_consistency_check_dpmm(mm_df, bsr_input):
    try:
        # 1. LOAD BSR (Handling both file path or dataframe)
        if isinstance(bsr_input, str):
            bsr_df = pd.read_excel(bsr_input)
        else:
            bsr_df = bsr_input.copy()

        # 2. CLEAN & NORMALIZE
        mm_df = mm_df.copy()
        original_cols = mm_df.columns.tolist()
        mm_df.columns = mm_df.columns.str.strip().str.lower()
        bsr_df.columns = bsr_df.columns.str.strip().str.lower()

        # Create 'match' in BSR if only home/away teams exist
        if "home team" in bsr_df.columns and "away team" in bsr_df.columns:
            bsr_df["match"] = (
                bsr_df["home team"].astype(str).str.strip() + 
                " vs " + 
                bsr_df["away team"].astype(str).str.strip()
            )

        # Helper to clean text
        def clean_text(col):
            return col.astype(str).str.lower().str.strip()

        # Necessary identifying columns for the check
        id_cols = ["event", "matchday", "competition", "match"]
        
        # Verify columns exist in mm_df (DPMM often has these in 'MM matchdays')
        missing_id = [c for c in id_cols if c not in mm_df.columns]
        if missing_id:
            mm_df["MM_BSR_Flag"] = False
            mm_df["MM_BSR_Remark"] = f"Missing DPMM columns: {missing_id}. Ensure you are using Matchday data."
            return mm_df

        for col in id_cols:
            mm_df[col] = clean_text(mm_df[col])
            if col in bsr_df.columns:
                bsr_df[col] = clean_text(bsr_df[col])

        # 3. CREATE UNIQUE KEYS
        # This acts like a unique ID: Event|Matchday|Match
        mm_df["_key"] = mm_df["event"] + "|" + mm_df["matchday"] + "|" + mm_df["match"]
        bsr_df["_key"] = bsr_df["event"] + "|" + bsr_df["matchday"] + "|" + bsr_df["match"]

        # Map BSR by key (Handling duplicates by keeping first)
        bsr_map = bsr_df.drop_duplicates(subset=["_key"]).set_index("_key")

        flags = []
        remarks = []

        # 4. VALIDATION LOOP
        for _, row in mm_df.iterrows():
            key = row["_key"]

            # Case 1: Match doesn't exist in BSR
            if key not in bsr_map.index:
                flags.append(False)
                remarks.append("Match fingerprint not found in BSR file")
                continue

            bsr_row = bsr_map.loc[key]

            # Case 2: Competition Mismatch (e.g., MM says 'League' but BSR says 'Cup')
            mm_comp = str(row.get("competition", "")).strip()
            bsr_comp = str(bsr_row.get("competition", "")).strip()

            if mm_comp != bsr_comp:
                flags.append(False)
                remarks.append(f"Comp mismatch → MM: {mm_comp} | BSR: {bsr_comp}")
                continue

            # ✅ CASE 3: VALID
            flags.append(True)
            remarks.append("")

        # Restore original casing and apply results
        mm_df.columns = original_cols + ["_key"] # Temporarily allow the key
        mm_df["MM_BSR_Check_Flag"] = flags
        mm_df["MM_BSR_Check_Remark"] = remarks

        # Cleanup
        mm_df.drop(columns=["_key"], inplace=True)
        return mm_df

    except Exception as e:
        mm_df["MM_BSR_Check_Flag"] = False
        mm_df["MM_BSR_Check_Remark"] = f"System Error: {str(e)}"
        return mm_df

def audience_spot_range_clean_view_dpmm(mm_df):
    df = mm_df.copy()
    df.columns = df.columns.str.strip()

    # DPMM Specific Headers
    category_col = "programme category"
    channel_col = "channel"
    audience_col = "audience (in 000's)"
    spot_col = "spot price"

    output = []

    # Filter out rows missing core grouping info to prevent crashes
    df = df.dropna(subset=[category_col, channel_col])
    
    grouped = df.groupby([category_col, channel_col])

    for (category, channel), group in grouped:
        group = group.copy()

        # Convert to numeric, forced errors to NaN
        group[audience_col] = pd.to_numeric(group[audience_col], errors="coerce")
        group[spot_col] = pd.to_numeric(group[spot_col], errors="coerce")

        # Calculate the Median (The 'Normal' value for this group)
        median_val = group[audience_col].median()

        # If no valid data to compare against, skip this group
        if pd.isna(median_val) or median_val == 0:
            continue

        # Define the 'Safety Zone' (50% above or below the median)
        lower_limit = median_val * 0.5
        upper_limit = median_val * 1.5

        for _, row in group.iterrows():
            val = row[audience_col]
            flag = True
            remark = ""

            if pd.notna(val):
                # DPMM is in 000s, so multiply by 1,000 for raw viewers
                audience_viewers = int(val * 1_000)

                if val > upper_limit:
                    flag = False
                    remark = f"Audience ({val}) is > 50% above group median ({median_val:.2f})"
                elif val < lower_limit:
                    flag = False
                    remark = f"Audience ({val}) is > 50% below group median ({median_val:.2f})"
            else:
                audience_viewers = None
                flag = False
                remark = "Audience data missing"

            output.append({
                "Programme Category": category,
                "Channel": channel,
                "Audience (viewers)": audience_viewers,
                "Spot Price": row[spot_col],
                "Flag": flag,
                "Remark": remark
            })

    return pd.DataFrame(output)

def ea_creation_check_dpmm(mm_df):
    df = mm_df.copy()
    # Ensure column names are clean
    df.columns = df.columns.str.strip()

    # The exact DPMM column for identifying EA status
    status_col = "child analysis status"

    flags = []
    remarks = []

    for _, row in df.iterrows():
        # Using .get() to avoid KeyErrors if the column is missing
        val = row.get(status_col)

        # In DPMM, if this is blank or 'n/a', it means the EA hasn't been created yet.
        # We treat both NaN and empty strings as 'Not Created'.
        if pd.isna(val) or str(val).strip().lower() in ["", "n/a", "none"]:
            flags.append(False)
            remarks.append("EA not created (Child Analysis status is blank or n/a)")
        
        # If it says 'pending' or 'id ready', it counts as the EA exists/is in progress
        else:
            flags.append(True)
            remarks.append("")

    df["EA_Creation_Flag"] = flags
    df["EA_Creation_Remark"] = remarks

    return df

def previous_delivery_check_dpmm(current_df, prev_df):
    # 1. CLEAN COLUMN NAMES
    current_df = current_df.copy()
    prev_df = prev_df.copy()
    
    current_df.columns = current_df.columns.str.strip().str.lower()
    prev_df.columns = prev_df.columns.str.strip().str.lower()

    # DPMM Specific Headers
    audience_col = "audience (in 000's)"
    spot_col = "spot price"
    cat_col = "programme category"
    chan_col = "channel"

    required_cols = [cat_col, chan_col, audience_col, spot_col]

    for col in required_cols:
        if col not in current_df.columns or col not in prev_df.columns:
            return pd.DataFrame([{"Error": f"Missing column: {col}"}])

    # 2. CONVERT NUMERIC + UNIT ADJUSTMENT (000s -> Raw)
    for df in [current_df, prev_df]:
        df[audience_col] = pd.to_numeric(df[audience_col], errors="coerce")
        df[spot_col] = pd.to_numeric(df[spot_col], errors="coerce")
        # Multiplied by 1000 for DPMM 000's unit
        df["audience_raw"] = df[audience_col] * 1_000 

    # 3. GROUP DATA
    curr_grp = current_df.groupby([cat_col, chan_col])
    prev_grp = prev_df.groupby([cat_col, chan_col])

    output = []

    for key, curr_group in curr_grp:
        category, channel = key

        # CURRENT MEDIANS
        curr_med_aud = curr_group["audience_raw"].median()
        curr_med_sp = curr_group[spot_col].median()

        # PREVIOUS BENCHMARKS
        if key in prev_grp.groups:
            prev_group = prev_grp.get_group(key)
            
            p_aud = prev_group["audience_raw"]
            p_sp = prev_group[spot_col]

            # Range: 50% below min to 50% above max
            aud_lower, aud_upper = p_aud.min() * 0.5, p_aud.max() * 1.5
            sp_lower, sp_upper = p_sp.min() * 0.5, p_sp.max() * 1.5
            
            p_med_aud = p_aud.median()
            p_med_sp = p_sp.median()
        else:
            aud_lower = aud_upper = sp_lower = sp_upper = None
            p_med_aud = p_med_sp = None

        # 4. VALIDATION
        flag = True
        remarks = []

        if aud_lower is not None:
            if not (aud_lower <= curr_med_aud <= aud_upper):
                flag = False
                remarks.append(f"Audience Outlier (Current: {curr_med_aud:,.0f})")

        if sp_lower is not None:
            if not (sp_lower <= curr_med_sp <= sp_upper):
                flag = False
                remarks.append(f"Price Outlier (Current: {curr_med_sp:,.0f})")

        output.append({
            "Category": category,
            "Channel": channel,
            "Current Aud Median": round(curr_med_aud, 0) if pd.notna(curr_med_aud) else 0,
            "Prev Aud Median": round(p_med_aud, 0) if p_med_aud else 0,
            "Current Spot Median": round(curr_med_sp, 0) if pd.notna(curr_med_sp) else 0,
            "Flag": flag,
            "Remark": " | ".join(remarks)
        })

    return pd.DataFrame(output)

def live_delayed_check_dpmm(mm_df):
    df = mm_df.copy()
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()

    # Required columns in DPMM context
    required_cols = ["programme category", "match", "bt"]

    # Safety check for missing columns
    for col in required_cols:
        if col not in df.columns:
            df["Live_Delayed_Check_Flag"] = False
            df["Live_Delayed_Check_Remark"] = f"Missing column: {col}"
            return df

    flags = []
    remarks = []

    for _, row in df.iterrows():
        category = str(row.get("programme category", "")).lower()
        match = str(row.get("match", "")).strip()
        bt = row.get("bt")

        # 1. Skip rows that aren't 'Live' or 'Delayed'
        # This ignores things like 'Highlights' or 'News'
        if not any(x in category for x in ["live", "delayed"]):
            flags.append(True)
            remarks.append("")
            continue

        issues = []

        # 2. Check for missing Match Name
        # If it's a Live match, the 'Match' column cannot be empty
        if match == "" or match.lower() in ["nan", "none"]:
            issues.append("Match name missing")

        # 3. Broadcast Time (BT) validation
        # Logic: A full match (Live or Delayed) must be at least 90 minutes.
        try:
            bt_val = float(bt)
            # Threshold: 90 (assuming BT is in minutes)
            if bt_val < 90:
                issues.append(f"BT too low ({bt_val}) for full match")
        except (ValueError, TypeError):
            issues.append("Invalid BT number")

        # Final Decision
        if issues:
            flags.append(False)
            remarks.append(" | ".join(issues))
        else:
            flags.append(True)
            remarks.append("")

    # Restore casing and apply results
    df.columns = original_cols
    df["Live_Delayed_Check_Flag"] = flags
    df["Live_Delayed_Check_Remark"] = remarks

    return df

def program_analysis_status_check_dpmm(mm_df):
    df = mm_df.copy()
    original_cols = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower()

    # DPMM Specific column search
    possible_cols = [
        "status (mm)",
        "status",
        "child analysis status"
    ]

    status_col = None
    for col in possible_cols:
        if col in df.columns:
            status_col = col
            break

    if not status_col:
        # Instead of raising an error which stops the app, we flag it gracefully
        df["Program_Status_Flag"] = False
        df["Program_Status_Remark"] = "Status column not found in this DPMM sheet."
        return df

    flags = []
    remarks = []

    for _, row in df.iterrows():
        # Get the raw status (e.g., 'done (delivered)')
        status_raw = str(row.get(status_col, "")).strip().lower()

        # DPMM often uses 'done (delivered)' or just 'done'
        if "done" in status_raw:
            flags.append(True)
            remarks.append("")

        elif "ready" in status_raw:
            flags.append(False)
            remarks.append(f"Status is '{status_raw}' (Work is finished but not yet finalized/delivered)")

        elif status_raw in ["", "nan", "none"]:
            flags.append(False)
            remarks.append("Status column is empty")

        else:
            # Catching things like 'pending' or 'in progress'
            flags.append(False)
            remarks.append(f"Analysis incomplete. Current status: {status_raw}")

    # Restore casing and apply results
    df.columns = original_cols
    df["Program_Status_Check_Flag"] = flags
    df["Program_Status_Check_Remark"] = remarks

    return df