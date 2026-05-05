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


def audience_spotprice_check(df):

    df.columns = df.columns.str.strip()

    audience_col = "audience (in mio)"
    spot_price_col = "spot price"

    flags = []
    remarks = []

    for _, row in df.iterrows():

        audience = row[audience_col]
        spot_price = row[spot_price_col]

        audience_blank = pd.isna(audience) or str(audience).strip() == ""
        spot_blank = pd.isna(spot_price) or str(spot_price).strip() == ""

        if audience_blank and spot_blank:
            flags.append(False)
            remarks.append("Audience and Spot Price both are missing")

        elif audience_blank:
            flags.append(False)
            remarks.append("Audience value is missing")

        elif spot_blank:
            flags.append(False)
            remarks.append("Spot Price is missing")

        else:
            flags.append(True)
            remarks.append("")

    # 👉 IMPORTANT: Assign directly to SAME DF
    df["Audience_SpotPrice_Check_Flag"] = flags
    df["Audience_SpotPrice_Check_Remark"] = remarks

    return df

def program_category_check(df):

    df = df.copy()
    df.columns = df.columns.str.strip()

    category_col = "programme category"

    valid_categories = [
        "live",
        "sport (live)",
        "magazine",
        "highlights",
        "delayed",
        "relive",
        "news"
    ]

    flags = []
    remarks = []

    for _, row in df.iterrows():

        category = row.get(category_col)

        # Normalize
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
            remarks.append("Correct category")

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


def channel_country_mapping_check(df, rosco_path):

    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    # -----------------------------------
    # STEP 1: READ ROSCO FILE
    # -----------------------------------
    rosco_excel = pd.ExcelFile(rosco_path)

    mapping_dict = {}

    for sheet in rosco_excel.sheet_names:

        if "rosco" not in sheet.lower():
            continue

        temp = pd.read_excel(rosco_excel, sheet_name=sheet)
        temp.columns = temp.columns.str.strip().str.lower()

        # Match correct columns
        if "channelname" in temp.columns and "channelcountry" in temp.columns:

            for _, row in temp.iterrows():
                ch_name = normalize_channel(row["channelname"])
                ch_country = str(row["channelcountry"]).strip().lower()

                if ch_name:
                    mapping_dict[ch_name] = ch_country

    # -----------------------------------
    # STEP 2: VALIDATE MM DATA
    # -----------------------------------
    flags = []
    remarks = []

    for _, row in df.iterrows():

        mm_channel_raw = row.get("channel")
        mm_country_raw = row.get("channel country")

        mm_channel = normalize_channel(mm_channel_raw)
        mm_country = str(mm_country_raw).strip().lower()

        # ❌ Channel not found
        if mm_channel not in mapping_dict:
            flags.append(False)
            remarks.append(f"Channel '{mm_channel_raw}' not found in ROSCO mapping")

        # ❌ Country mismatch
        elif mapping_dict[mm_channel] != mm_country:
            flags.append(False)
            remarks.append(
                f"Channel '{mm_channel_raw}' mapped to '{mapping_dict[mm_channel]}' but found in '{mm_country_raw}'"
            )

        # ✅ Valid
        else:
            flags.append(True)
            remarks.append("")

    df["Channel_Country_Check_Flag"] = flags
    df["Channel_Country_Check_Remark"] = remarks

    return df

def apt_bt_check(df, bt_threshold=None):

    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    apt_col = "apt"
    live_apt_col = "apt live"
    bt_col = "bt"
    category_col = "programme category"

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

        try:
            apt_live = float(row.get(live_apt_col))
        except:
            apt_live = None

        # -----------------------------------
        # PRIORITY 1: Live / Relive APT < 50%
        # -----------------------------------
        if category in ["live", "relive", "sport (live)"] and apt is not None and bt is not None:

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

        # -----------------------------------
        # DEFAULT
        # -----------------------------------
        flags.append(True)
        remarks.append("")

    df["APT_BT_Check_Flag"] = flags
    df["APT_BT_Check_Remark"] = remarks

    return df

def season_monitoring_check(df, start_date, end_date):

    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    date_col = "progr. start (date)"

    # Convert input dates
    start = pd.to_datetime(start_date, errors="coerce")
    end = pd.to_datetime(end_date, errors="coerce")

    flags = []
    remarks = []

    for _, row in df.iterrows():

        prog_date = pd.to_datetime(row.get(date_col), dayfirst=True, errors="coerce")

        if pd.isna(prog_date):
            flags.append(False)
            remarks.append("Invalid programme start date")

        elif prog_date < start or prog_date > end:
            flags.append(False)
            remarks.append(
                f"Date {prog_date.date()} outside monitoring period ({start.date()} to {end.date()})"
            )

        else:
            flags.append(True)
            remarks.append("")

    df["Season_Check_Flag"] = flags
    df["Season_Check_Remark"] = remarks

    return df

def fixture_validation_check(df, fixture_df):

    df.columns = df.columns.str.strip()
    fixture_df.columns = fixture_df.columns.str.strip()

    required_cols = ["event", "matchday", "matchday date", "match"]

    for col in required_cols:
        if col not in df.columns or col not in fixture_df.columns:
            raise ValueError(f"Missing column: {col}")

    for col in required_cols + ["competition"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()
        if col in fixture_df.columns:
            fixture_df[col] = fixture_df[col].astype(str).str.lower().str.strip()

    fixture_set = set()

    for _, row in fixture_df.iterrows():
        key = (
            row["event"],
            row["matchday"],
            row["matchday date"],
            row["match"]
        )
        fixture_set.add(key)

    flags = []
    remarks = []

    for _, row in df.iterrows():

        key = (
            row["event"],
            row["matchday"],
            row["matchday date"],
            row["match"]
        )

        if key in fixture_set:
            flags.append(True)
            remarks.append("")
        else:
            flags.append(False)
            remarks.append("Match/Event not found in Fixture file")

    df["Fixture_Validation_Flag"] = flags
    df["Fixture_Validation_Remark"] = remarks

    return df

def stadium_consistency_check(df):

    df.columns = df.columns.str.strip()

    required_cols = [
        "programme category",
        "matchday date",
        "stadium"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    # Optional columns
    match_col_exists = "match" in df.columns
    team_col_exists = "team" in df.columns

    # ----------------------------
    # CLEAN DATA
    # ----------------------------
    df["programme category"] = df["programme category"].astype(str).str.strip().str.lower()
    df["matchday date"] = df["matchday date"].astype(str).str.strip()
    df["stadium"] = df["stadium"].astype(str).str.strip().str.lower()

    if match_col_exists:
        df["match"] = df["match"].astype(str).str.strip().str.lower()

    if team_col_exists:
        df["team"] = df["team"].astype(str).str.strip().str.lower()

    # ----------------------------
    # FILTER ONLY LIVE PROGRAMS
    # ----------------------------
    live_df = df[df["programme category"].str.contains("live", na=False)].copy()

    # ----------------------------
    # CREATE IDENTIFIER
    # ----------------------------
    def get_identifier(row):
        if match_col_exists and row.get("match"):
            if row["match"] not in ["", "nan"]:
                return row["match"]
        if team_col_exists:
            return row["team"]
        return "unknown"

    live_df["identifier"] = live_df.apply(get_identifier, axis=1)

    # ----------------------------
    # GROUPING LOGIC
    # ----------------------------
    group = live_df.groupby(["identifier", "matchday date"])["stadium"].nunique()

    invalid_groups = group[group > 1].index

    # ----------------------------
    # APPLY FLAG TO ORIGINAL DF
    # ----------------------------
    flag = []
    remark = []

    for _, row in df.iterrows():

        category = str(row["programme category"]).lower()

        # Skip non-live rows
        if "live" not in category:
            flag.append("TRUE")
            remark.append("")
            continue

        # Determine identifier
        identifier = ""

        if match_col_exists and str(row.get("match", "")).strip().lower() not in ["", "nan"]:
            identifier = str(row["match"]).strip().lower()
        elif team_col_exists:
            identifier = str(row.get("team", "")).strip().lower()
        else:
            identifier = "unknown"

        key = (identifier, str(row["matchday date"]).strip())

        if key in invalid_groups:
            flag.append("FALSE")
            remark.append("Multiple stadiums for same match/team on same date")
        else:
            flag.append("TRUE")
            remark.append("")

    df["stadium_consistency_flag"] = flag
    df["stadium_consistency_remark"] = remark

    return df

def event_quality_check(df):

    df.columns = df.columns.str.strip()

    required_cols = ["programme category"]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    # Optional
    bt_col = "BT" if "BT" in df.columns else None

    # Clean
    df["programme category"] = df["programme category"].astype(str).str.lower().str.strip()

    # Allowed categories
    allowed_categories = ["live", "delayed", "highlight", "magazine"]

    flag = []
    remark = []

    for _, row in df.iterrows():

        category = row["programme category"]

        # ----------------------------
        # CATEGORY VALIDATION
        # ----------------------------
        if not any(x in category for x in allowed_categories):
            flag.append("FALSE")
            remark.append("Invalid programme category")
            continue

        # ----------------------------
        # LIVE BT CHECK (BASIC)
        # ----------------------------
        if "live" in category and bt_col:

            try:
                bt_value = float(row[bt_col])
                if bt_value < 60:   # threshold (can adjust)
                    flag.append("FALSE")
                    remark.append("BT too low for live program")
                    continue
            except:
                flag.append("FALSE")
                remark.append("Invalid BT value")
                continue

        # ----------------------------
        # DEFAULT PASS
        # ----------------------------
        flag.append("TRUE")
        remark.append("")

    df["event_quality_flag"] = flag
    df["event_quality_remark"] = remark

    return df

def home_market_check(df):

    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    required_cols = [
        "match",
        "programme category",
        "home/away",
        "country",
        "channel country"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    # ----------------------------
    # CLEAN DATA
    # ----------------------------
    df["match"] = df["match"].astype(str).str.strip().str.lower()
    df["programme category"] = df["programme category"].astype(str).str.strip().str.lower()
    df["home/away"] = df["home/away"].astype(str).str.strip().str.lower()
    df["country"] = df["country"].astype(str).str.strip().str.lower()
    df["channel country"] = df["channel country"].astype(str).str.strip().str.lower()

    # ----------------------------
    # FILTER LIVE + DELAYED
    # ----------------------------
    live_df = df[df["programme category"].str.contains("live|delayed", na=False)].copy()

    # ----------------------------
    # ALL MARKETS COVERED IN MM
    # ----------------------------
    all_markets_in_mm = set(df["channel country"].dropna().unique())

    # ----------------------------
    # MATCH → AVAILABLE MARKETS
    # ----------------------------
    match_market_map = (
        live_df.groupby("match")["channel country"]
        .apply(lambda x: set(x))
        .to_dict()
    )

    # ----------------------------
    # MATCH → HOME COUNTRY
    # ----------------------------
    home_country_map = {}

    for _, row in live_df.iterrows():
        if row["home/away"] == "home":
            home_country_map[row["match"]] = row["country"]

    # ----------------------------
    # VALIDATION
    # ----------------------------
    flags = []
    remarks = []

    for _, row in df.iterrows():

        category = row["programme category"]

        # Skip non-live/delayed
        if not ("live" in category or "delayed" in category):
            flags.append(True)
            remarks.append("")
            continue

        match = row["match"]

        if match not in home_country_map:
            flags.append(True)
            remarks.append("")
            continue

        home_country = home_country_map[match]
        available_markets = match_market_map.get(match, set())

        # ----------------------------
        # CASE 1: Home market present
        # ----------------------------
        if home_country in available_markets:
            flags.append(True)
            remarks.append("")
            continue

        # ----------------------------
        # CASE 2: Market NOT in MM → OK
        # ----------------------------
        if home_country not in all_markets_in_mm:
            flags.append(True)
            remarks.append(f"{home_country} not covered in MM (allowed)")
            continue

        # ----------------------------
        # CASE 3: REAL ISSUE
        # ----------------------------
        flags.append(False)
        remarks.append(f"Missing home market broadcast: {home_country}")

    df["home_market_flag"] = flags
    df["home_market_remark"] = remarks

    return df

def ps_market_channel_check(df, rosco_df):

    df.columns = df.columns.str.strip().str.lower()
    rosco_df.columns = rosco_df.columns.str.strip()

    # Normalize ROSCO
    rosco_df["ChannelCountry"] = rosco_df["ChannelCountry"].astype(str).str.lower().str.strip()
    rosco_df["ChannelName"] = rosco_df["ChannelName"].astype(str).str.lower().str.strip()

    valid_markets = set(rosco_df["ChannelCountry"].dropna().unique())
    valid_channels = set(rosco_df["ChannelName"].dropna().unique())

    flags = []
    remarks = []

    for _, row in df.iterrows():

        market = str(row.get("channel country", "")).lower().strip()
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

    df["PS_Market_Channel_Flag"] = flags
    df["PS_Market_Channel_Remark"] = remarks

    return df

def ps_content_check(df, rosco_df):

    df.columns = df.columns.str.strip().str.lower()
    rosco_df.columns = rosco_df.columns.str.strip()

    df["programme"] = df["programme"].astype(str).str.lower().str.strip()
    rosco_df["ChannelPrograms"] = rosco_df["ChannelPrograms"].astype(str).str.lower().str.strip()

    valid_programs = set(rosco_df["ChannelPrograms"].dropna().unique())

    flags = []
    remarks = []

    for _, row in df.iterrows():

        prog = row.get("programme", "")

        if prog not in valid_programs:
            flags.append(False)
            remarks.append(f"Programme not in ROSCO: {prog}")
        else:
            flags.append(True)
            remarks.append("")

    df["PS_Content_Flag"] = flags
    df["PS_Content_Remark"] = remarks

    return df

def channel_country_mapping_check(df, rosco_path):

    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    # -----------------------------------
    # STEP 1: READ ROSCO FILE
    # -----------------------------------
    rosco_excel = pd.ExcelFile(rosco_path)

    mapping_dict = {}

    for sheet in rosco_excel.sheet_names:

        if "rosco" not in sheet.lower():
            continue

        temp = pd.read_excel(rosco_excel, sheet_name=sheet)
        temp.columns = temp.columns.str.strip().str.lower()

        # Match correct columns
        if "channelname" in temp.columns and "channelcountry" in temp.columns:

            for _, row in temp.iterrows():
                ch_name = normalize_channel(row["channelname"])
                ch_country = str(row["channelcountry"]).strip().lower()

                if ch_name:
                    mapping_dict[ch_name] = ch_country

    # -----------------------------------
    # STEP 2: VALIDATE MM DATA
    # -----------------------------------
    flags = []
    remarks = []

    for _, row in df.iterrows():

        mm_channel_raw = row.get("channel")
        mm_country_raw = row.get("channel country")

        mm_channel = normalize_channel(mm_channel_raw)
        mm_country = str(mm_country_raw).strip().lower()

        # ❌ Channel not found
        if mm_channel not in mapping_dict:
            flags.append(False)
            remarks.append(f"Channel '{mm_channel_raw}' not found in ROSCO mapping")

        # ❌ Country mismatch
        elif mapping_dict[mm_channel] != mm_country:
            flags.append(False)
            remarks.append(
                f"Channel '{mm_channel_raw}' mapped to '{mapping_dict[mm_channel]}' but found in '{mm_country_raw}'"
            )

        # ✅ Valid
        else:
            flags.append(True)
            remarks.append("")

    df["Channel_Country_Check_Flag"] = flags
    df["Channel_Country_Check_Remark"] = remarks

    return df

def mm_bsr_consistency_check(mm_df, bsr_input):

    print(" FINAL MM BSR FUNCTION RUNNING")

    try:
        # LOAD BSR
        if isinstance(bsr_input, str):
            bsr_df = pd.read_excel(bsr_input)
        else:
            bsr_df = bsr_input.copy()

        # CLEAN
        mm_df = mm_df.copy()
        mm_df.columns = mm_df.columns.str.strip().str.lower()
        bsr_df.columns = bsr_df.columns.str.strip().str.lower()

        # CREATE MATCH
        if "home team" in bsr_df.columns and "away team" in bsr_df.columns:
            bsr_df["match"] = (
                bsr_df["home team"].astype(str).str.strip()
                + " vs " +
                bsr_df["away team"].astype(str).str.strip()
            )

        # CLEAN TEXT
        def clean(col):
            return col.astype(str).str.lower().str.strip()

        for col in ["event", "matchday", "competition", "match"]:
            mm_df[col] = clean(mm_df[col])
            bsr_df[col] = clean(bsr_df[col])

        # CREATE KEY
        mm_df["_key"] = mm_df["event"] + "|" + mm_df["matchday"] + "|" + mm_df["match"]
        bsr_df["_key"] = bsr_df["event"] + "|" + bsr_df["matchday"] + "|" + bsr_df["match"]

        # MAP
        bsr_map = bsr_df.set_index("_key")

        flags = []
        remarks = []

        for _, row in mm_df.iterrows():

            key = row["_key"]

            # ❌ Case 1: Missing match
            if key not in bsr_map.index:
                flags.append(False)
                remarks.append("Match not found in BSR file")
                continue

            bsr_row = bsr_map.loc[key]

            # ❌ Case 2: Competition mismatch
            mm_comp = str(row.get("competition", "")).strip()
            bsr_comp = str(bsr_row.get("competition", "")).strip()

            if mm_comp != bsr_comp:
                flags.append(False)
                remarks.append(
                    f"Competition mismatch → MM: {mm_comp} | BSR: {bsr_comp}"
                )
                continue

            # ✅ Case 3: All good
            flags.append(True)
            remarks.append("")

        # FINAL OUTPUT
        mm_df["MM_BSR_Flag"] = flags
        mm_df["MM_BSR_Remark"] = remarks

        # REMOVE INTERNAL COLUMNS
        for col in ["_key", "key"]:
            if col in mm_df.columns:
                mm_df.drop(columns=[col], inplace=True)

        return mm_df

    except Exception as e:
        print("❌ ERROR:", str(e))
        mm_df["MM_BSR_Flag"] = False
        mm_df["MM_BSR_Remark"] = str(e)
        return mm_df

def audience_spot_range_clean_view(df):

    print("🔍 NEW Audience Range Logic Running")

    df.columns = df.columns.str.strip()

    category_col = "programme category"
    channel_col = "channel"
    audience_col = "audience (in mio)"
    spot_col = "spot price"

    output = []

    grouped = df.groupby([category_col, channel_col])

    for (category, channel), group in grouped:

        print(f"Processing: {category} | {channel}")

        group = group.copy()

        group[audience_col] = pd.to_numeric(group[audience_col], errors="coerce")
        group[spot_col] = pd.to_numeric(group[spot_col], errors="coerce")

        median_val = group[audience_col].median()

        if pd.isna(median_val) or median_val == 0:
            continue

        lower = median_val * 0.5
        upper = median_val * 1.5

        for _, row in group.iterrows():

            val = row[audience_col]

            flag = True
            remark = ""

            if pd.notna(val):

                # ✅ convert to viewers
                audience_viewers = int(val * 1_000_000)

                if val > upper:
                    flag = False
                    remark = "Audience > 50% above expected range"

                elif val < lower:
                    flag = False
                    remark = "Audience > 50% below expected range"

            else:
                audience_viewers = None
                flag = False
                remark = "Audience missing"

            output.append({
                "Programme Category": category,
                "Channel": channel,
                "Audience (viewers)": audience_viewers,
                "Spot Price": row[spot_col],
                "Flag": flag,
                "Remark": remark
            })

    print("✅ NEW LOGIC APPLIED")

    return pd.DataFrame(output)

def ea_creation_check(df):

    df.columns = df.columns.str.strip()

    col = "child analysis status"

    flags = []
    remarks = []

    for _, row in df.iterrows():

        val = row.get(col)

        # Check blank / null / empty string
        if pd.isna(val) or str(val).strip() == "":
            flags.append(False)
            remarks.append("EA not created (Child Analysis missing)")
        else:
            flags.append(True)
            remarks.append("")

    df["EA_Creation_Flag"] = flags
    df["EA_Creation_Remark"] = remarks

    return df

def previous_delivery_check(current_df, prev_df):

    # -----------------------------------
    # CLEAN COLUMN NAMES
    # -----------------------------------
    current_df.columns = current_df.columns.str.strip().str.lower()
    prev_df.columns = prev_df.columns.str.strip().str.lower()

    required_cols = ["programme category", "channel", "audience (in mio)", "spot price"]

    for col in required_cols:
        if col not in current_df.columns or col not in prev_df.columns:
            raise ValueError(f"Missing column: {col}")

    # -----------------------------------
    # CONVERT NUMERIC + FIX UNITS
    # -----------------------------------
    for df in [current_df, prev_df]:
        df["audience (in mio)"] = pd.to_numeric(df["audience (in mio)"], errors="coerce")
        df["spot price"] = pd.to_numeric(df["spot price"], errors="coerce")

        # Convert to actual numbers
        df["audience"] = df["audience (in mio)"] * 1_000_000

    # -----------------------------------
    # GROUP DATA
    # -----------------------------------
    curr_grp = current_df.groupby(["programme category", "channel"])
    prev_grp = prev_df.groupby(["programme category", "channel"])

    output = []

    for key, curr_group in curr_grp:

        category, channel = key

        # ---------------- CURRENT VALUES ----------------
        curr_aud = curr_group["audience"]
        curr_sp = curr_group["spot price"]

        curr_med_aud = curr_aud.median()
        curr_med_sp = curr_sp.median()

        # ---------------- PREVIOUS VALUES ----------------
        if key in prev_grp.groups:

            prev_group = prev_grp.get_group(key)

            prev_aud = prev_group["audience"]
            prev_sp = prev_group["spot price"]

            prev_min_aud = prev_aud.min()
            prev_max_aud = prev_aud.max()
            prev_med_aud = prev_aud.median()

            prev_min_sp = prev_sp.min()
            prev_max_sp = prev_sp.max()
            prev_med_sp = prev_sp.median()

        else:
            prev_min_aud = prev_max_aud = prev_med_aud = None
            prev_min_sp = prev_max_sp = prev_med_sp = None

        # -----------------------------------
        # DEFINE RANGE (CORRECT LOGIC)
        # -----------------------------------
        def get_range(min_val, max_val):
            if pd.isna(min_val) or pd.isna(max_val):
                return None, None
            return min_val * 0.5, max_val * 1.5

        aud_lower, aud_upper = get_range(prev_min_aud, prev_max_aud)
        sp_lower, sp_upper = get_range(prev_min_sp, prev_max_sp)

        # -----------------------------------
        # CHECK LOGIC
        # -----------------------------------
        flag = True
        remarks = []

        # Audience check
        if aud_lower is not None and aud_upper is not None:
            if not (aud_lower <= curr_med_aud <= aud_upper):
                flag = False
                remarks.append(
                    f"Audience out of range (Current: {curr_med_aud:.0f}, Expected: {aud_lower:.0f}-{aud_upper:.0f})"
                )

        # Spot price check
        if sp_lower is not None and sp_upper is not None:
            if not (sp_lower <= curr_med_sp <= sp_upper):
                flag = False
                remarks.append(
                    f"Spot Price out of range (Current: {curr_med_sp:.0f}, Expected: {sp_lower:.0f}-{sp_upper:.0f})"
                )

        remark = " | ".join(remarks) if remarks else ""

        # -----------------------------------
        # OUTPUT
        # -----------------------------------
        output.append({
            "Programme Category": category,
            "Channel": channel,

            "Current Audience Median": round(curr_med_aud, 0) if pd.notna(curr_med_aud) else None,
            "Previous Audience Median": round(prev_med_aud, 0) if prev_med_aud else None,

            "Current Spot Price Median": round(curr_med_sp, 0) if pd.notna(curr_med_sp) else None,
            "Previous Spot Price Median": round(prev_med_sp, 0) if prev_med_sp else None,

            "Audience Range": f"{round(aud_lower,0)} - {round(aud_upper,0)}" if aud_lower else "",
            "Spot Price Range": f"{round(sp_lower,0)} - {round(sp_upper,0)}" if sp_lower else "",

            "Flag": flag,
            "Remark": remark
        })

    return pd.DataFrame(output)

def live_delayed_check(df):

    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    required_cols = ["programme category", "match", "bt"]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    flags = []
    remarks = []

    for _, row in df.iterrows():

        category = str(row.get("programme category", "")).lower()
        match = str(row.get("match", "")).strip()
        bt = row.get("bt")

        # Only apply for live / delayed
        if not ("live" in category or "delayed" in category):
            flags.append(True)
            remarks.append("")
            continue

        issues = []

        # Missing match
        if match == "" or match.lower() == "nan":
            issues.append("Match missing for live/delayed entry")

        # Invalid category
        if not any(x in category for x in ["live", "delayed"]):
            issues.append(f"Invalid category for live/delayed: {category}")

        # BT check (basic threshold 90 mins)
        try:
            bt_val = float(bt)
            if bt_val < 90:
                issues.append("BT too low for live/delayed (expected ≥ 90 mins)")
        except:
            issues.append("Invalid BT value")

        # Final decision
        if issues:
            flags.append(False)
            remarks.append(" | ".join(issues))
        else:
            flags.append(True)
            remarks.append("")

    df["Live_Delayed_Check_Flag"] = flags
    df["Live_Delayed_Check_Remark"] = remarks

    return df

def program_analysis_status_check(df):

    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    # Possible column names (handle variations)
    possible_cols = [
        "analysis status",
        "status",
        "mm status",
        "child analysis status"
    ]

    status_col = None
    for col in possible_cols:
        if col in df.columns:
            status_col = col
            break

    if not status_col:
        raise ValueError("No status column found for Program Analysis Status Check")

    flags = []
    remarks = []

    for _, row in df.iterrows():

        status = str(row.get(status_col, "")).strip().lower()

        if status == "done":
            flags.append(True)
            remarks.append("")

        elif status == "ready":
            flags.append(False)
            remarks.append("Status is READY (should be moved to DONE)")

        elif status == "" or status == "nan":
            flags.append(False)
            remarks.append("Status missing")

        else:
            flags.append(False)
            remarks.append(f"Invalid status: {status}")

    df["Program_Status_Flag"] = flags
    df["Program_Status_Remark"] = remarks

    return df