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

def duplicate_aid_final(df):

    df = df.copy()
    df.columns = df.columns.str.strip()

    # Define combination
    group_cols = [
        "programme category",
        "country",
        "channel",
        "programme",
        "progr. start (date)",
        "progr. start (time)"
    ]

    # Create combo id
    df["_combo_id"] = df.groupby(group_cols).ngroup()

    # Count AIDs per combination
    df["_aid_count_per_combo"] = df.groupby(group_cols)["aID (MAT)"].transform("nunique")

    # Count combinations per AID
    df["_combo_count_per_aid"] = df.groupby("aID (MAT)")["_combo_id"].transform("nunique")

    flags = []
    remarks = []

    for _, row in df.iterrows():

        # PRIORITY 1: Same AID used across multiple combinations
        if row["_combo_count_per_aid"] > 1:
            flags.append(False)
            remarks.append(
                f"AID {row['aID (MAT)']} is used across multiple program combinations"
            )

        # PRIORITY 2: Multiple AIDs for same combination
        elif row["_aid_count_per_combo"] > 1:
            flags.append(False)
            remarks.append(
                f"Multiple AIDs assigned to same program combination ({row['programme']} at {row['progr. start (date)']} {row['progr. start (time)']})"
            )

        # 🟢 VALID
        else:
            flags.append(True)
            remarks.append("")

    # Final columns
    df["Duplicate_AID_Check_Flag"] = flags
    df["Duplicate_AID_Check_Remark"] = remarks

    # Drop helper columns
    df.drop(columns=[
        "_combo_id",
        "_aid_count_per_combo",
        "_combo_count_per_aid"
    ], inplace=True)

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

    required_cols = [
        "event",
        "matchday",
        "matchday date",
        "match"
    ]

    optional_col = "competition"
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column in Adapt file: {col}")
        if col not in fixture_df.columns:
            raise ValueError(f"Missing column in Fixture file: {col}")

    for col in required_cols + [optional_col]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()
        if col in fixture_df.columns:
            fixture_df[col] = fixture_df[col].astype(str).str.strip().str.lower()

    fixture_set = set()

    for _, row in fixture_df.iterrows():
        key = (
            row["event"],
            row["matchday"],
            row["matchday date"],
            row["match"],
            row.get("competition", "")
        )
        fixture_set.add(key)

    results = []

    for _, row in df.iterrows():

        key_with_comp = (
            row["event"],
            row["matchday"],
            row["matchday date"],
            row["match"],
            row.get("competition", "")
        )

        key_without_comp = (
            row["event"],
            row["matchday"],
            row["matchday date"],
            row["match"],
            ""
        )

        if key_with_comp in fixture_set or key_without_comp in fixture_set:
            results.append("TRUE")
        else:
            results.append("FALSE")

    df["fixture_validation"] = results

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

    # ----------------------------
    # CLEAN COLUMN NAMES
    # ----------------------------
    df.columns = df.columns.str.strip()

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
    # FILTER LIVE + DELAYED DATA
    # ----------------------------
    live_df = df[df["programme category"].str.contains("live|delayed", na=False)].copy()

    # ----------------------------
    # BUILD MATCH → CHANNEL COUNTRIES MAP
    # ----------------------------
    match_market_map = (
        live_df.groupby("match")["channel country"]
        .apply(lambda x: set(x))
        .to_dict()
    )

    # ----------------------------
    # BUILD MATCH → HOME COUNTRY MAP
    # ----------------------------
    home_country_map = {}

    for _, row in live_df.iterrows():
        if row["home/away"] == "home":
            match = row["match"]
            home_country_map[match] = row["country"]

    # ----------------------------
    # APPLY CHECK
    # ----------------------------
    flag = []
    remark = []

    for _, row in df.iterrows():

        category = row["programme category"]

        # Skip non live/delayed
        if not ("live" in category or "delayed" in category):
            flag.append("TRUE")
            remark.append("")
            continue

        match = row["match"]

        # If no home info → skip safely
        if match not in home_country_map:
            flag.append("TRUE")
            remark.append("")
            continue

        home_country = home_country_map[match]
        available_markets = match_market_map.get(match, set())

        if home_country not in available_markets:
            flag.append("FALSE")
            remark.append(f"Missing home market broadcast: {home_country}")
        else:
            flag.append("TRUE")
            remark.append("")

    # ----------------------------
    # ADD OUTPUT
    # ----------------------------
    df["home_market_flag"] = flag
    df["home_market_remark"] = remark

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

    print("🔍 MM vs BSR check started")

    try:
        # -----------------------------------
        # HELPER: FIND BEST SHEET
        # -----------------------------------
        def pick_best_sheet(excel):
            priority_keywords = ["worksheet", "database", "workfile", "data"]

            for sheet in excel.sheet_names:
                if any(k in sheet.lower() for k in priority_keywords):
                    return sheet

            return excel.sheet_names[0]

        # -----------------------------------
        # HELPER: DETECT HEADER ROW
        # -----------------------------------
        def detect_header_row(file_path, sheet_name):
            preview = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=30)

            keywords = ["event", "match", "home", "away", "competition"]
            best_row = 0
            best_score = 0

            for i, row in preview.iterrows():
                row_values = [str(x).lower() for x in row.values]

                score = sum(any(k in val for val in row_values) for k in keywords)

                if score > best_score:
                    best_score = score
                    best_row = i

            print(f" Header detected at row {best_row} (score={best_score})")
            return best_row

        # -----------------------------------
        # LOAD BSR FILE
        # -----------------------------------
        if isinstance(bsr_input, str):

            print("Loading BSR file:", bsr_input)

            excel = pd.ExcelFile(bsr_input)
            print(" Sheets found:", excel.sheet_names)

            sheet = pick_best_sheet(excel)
            print(" Using sheet:", sheet)

            header_row = detect_header_row(bsr_input, sheet)

            bsr_df = pd.read_excel(bsr_input, sheet_name=sheet, header=header_row)

        else:
            print("📂 Using provided BSR dataframe")
            bsr_df = bsr_input.copy()

        # -----------------------------------
        # CLEAN COLUMN NAMES
        # -----------------------------------
        mm_df = mm_df.copy()
        mm_df.columns = mm_df.columns.str.strip().str.lower()
        bsr_df.columns = bsr_df.columns.astype(str).str.strip()

        print("📊 BSR columns:", list(bsr_df.columns))

        # -----------------------------------
        # FLEXIBLE COLUMN FINDER
        # -----------------------------------
        def find_column(df, keywords):
            for col in df.columns:
                for k in keywords:
                    if k in col.lower():
                        return col
            return None

        # Detect columns
        event_col = find_column(bsr_df, ["event"])
        matchday_col = find_column(bsr_df, ["matchday"])
        competition_col = find_column(bsr_df, ["competition"])
        home_col = find_column(bsr_df, ["home"])
        away_col = find_column(bsr_df, ["away"])

        print("🔍 Column mapping:")
        print("Event:", event_col)
        print("Matchday:", matchday_col)
        print("Competition:", competition_col)
        print("Home:", home_col)
        print("Away:", away_col)

        # -----------------------------------
        # SAFE FALLBACKS (NO CRASH)
        # -----------------------------------
        if not home_col or not away_col:
            print("Home/Away not found → skipping BSR check")
            mm_df["MM_BSR_Flag"] = "Skipped - Invalid BSR"
            mm_df["MM_BSR_Remark"] = "Home/Away columns not found"
            return mm_df

        if not event_col:
            print(" Event column missing")
            bsr_df["event"] = ""
        else:
            bsr_df["event"] = bsr_df[event_col]

        if not matchday_col:
            print(" Matchday missing")
            bsr_df["matchday"] = ""
        else:
            bsr_df["matchday"] = bsr_df[matchday_col]

        if not competition_col:
            print(" Competition missing")
            bsr_df["competition"] = ""
        else:
            bsr_df["competition"] = bsr_df[competition_col]

        # -----------------------------------
        # CREATE MATCH COLUMN
        # -----------------------------------
        bsr_df["match"] = (
            bsr_df[home_col].astype(str).str.strip()
            + " vs " +
            bsr_df[away_col].astype(str).str.strip()
        )

        # -----------------------------------
        # CLEAN FUNCTION
        # -----------------------------------
        def clean(col):
            return col.astype(str).str.lower().str.strip()

        # Clean MM
        mm_df["event"] = clean(mm_df["event"])
        mm_df["matchday"] = clean(mm_df["matchday"])
        mm_df["competition"] = clean(mm_df["competition"])
        mm_df["match"] = clean(mm_df["match"])

        # Clean BSR
        bsr_df["event"] = clean(bsr_df["event"])
        bsr_df["matchday"] = clean(bsr_df["matchday"])
        bsr_df["competition"] = clean(bsr_df["competition"])
        bsr_df["match"] = clean(bsr_df["match"])

        # -----------------------------------
        # CREATE KEYS
        # -----------------------------------
        mm_df["key"] = mm_df["event"] + "|" + mm_df["matchday"] + "|" + mm_df["match"]
        bsr_df["key"] = bsr_df["event"] + "|" + bsr_df["matchday"] + "|" + bsr_df["match"]

        # -----------------------------------
        # INIT FLAG
        # -----------------------------------
        mm_df["MM_BSR_Flag"] = "OK"

        # -----------------------------------
        # CHECKS
        # -----------------------------------
        missing_mask = ~mm_df["key"].isin(bsr_df["key"])
        mm_df.loc[missing_mask, "MM_BSR_Flag"] = "Missing in BSR"

        merged = mm_df.merge(
            bsr_df[["key", "competition"]],
            on="key",
            how="left",
            suffixes=("", "_bsr")
        )

        mismatch_mask = (
            (merged["competition"] != merged["competition_bsr"]) &
            (~merged["competition_bsr"].isna())
        )

        mm_df.loc[mismatch_mask, "MM_BSR_Flag"] = "Mismatch with BSR"

        # -----------------------------------
        # FINAL
        # -----------------------------------
        mm_df["MM_BSR_Remark"] = mm_df["MM_BSR_Flag"]

        print(" MM vs BSR completed successfully")

        return mm_df

    except Exception as e:
        import traceback
        traceback.print_exc()

        print("❌ MM BSR check failed, returning safe output")

        mm_df["MM_BSR_Flag"] = "Error"
        mm_df["MM_BSR_Remark"] = str(e)

        return mm_df

def audience_spot_range_clean_view(df):

    print("🔍 Starting Audience Range Check")

    df.columns = df.columns.str.strip()

    category_col = "programme category"
    channel_col = "channel"
    audience_col = "audience (in mio)"
    spot_col = "spot price"

    output = []

    grouped = df.groupby([category_col, channel_col])

    for (category, channel), group in grouped:

        print(f"\n📊 Processing: {category} | {channel}")

        group = group.copy()

        group[audience_col] = pd.to_numeric(group[audience_col], errors="coerce")
        group[spot_col] = pd.to_numeric(group[spot_col], errors="coerce")

        median_val = group[audience_col].median()

        if pd.isna(median_val) or median_val == 0:
            print("⚠️ Skipping due to invalid median")
            continue

        # ✅ Define range (±50%)
        lower = median_val * 0.5
        upper = median_val * 1.5

        for _, row in group.iterrows():

            val = row[audience_col]

            flag = True
            remark = ""

            if pd.notna(val):

                # ✅ Convert to viewers
                audience_viewers = int(val * 1_000_000)

                if val > upper or val < lower:
                    flag = False
                    remark = "Audience out of expected range for this channel"

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

    print("✅ Completed")

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

    current_df.columns = current_df.columns.str.strip().str.lower()
    prev_df.columns = prev_df.columns.str.strip().str.lower()

    # Required columns
    cols = ["programme category", "channel", "audience (in mio)", "spot price"]

    for col in cols:
        if col not in current_df.columns or col not in prev_df.columns:
            raise ValueError(f"Missing column: {col}")

    # Clean numeric
    for df in [current_df, prev_df]:
        df["audience (in mio)"] = pd.to_numeric(df["audience (in mio)"], errors="coerce")
        df["spot price"] = pd.to_numeric(df["spot price"], errors="coerce")

    # Group
    curr_grp = current_df.groupby(["programme category", "channel"])
    prev_grp = prev_df.groupby(["programme category", "channel"])

    output = []

    for key, curr_group in curr_grp:

        category, channel = key

        curr_median_aud = curr_group["audience (in mio)"].median()
        curr_median_sp = curr_group["spot price"].median()

        if key in prev_grp.groups:
            prev_group = prev_grp.get_group(key)

            prev_median_aud = prev_group["audience (in mio)"].median()
            prev_median_sp = prev_group["spot price"].median()
        else:
            prev_median_aud = None
            prev_median_sp = None

        # --------- DEVIATION ---------
        def calc_dev(curr, prev):
            if pd.isna(curr) or pd.isna(prev) or prev == 0:
                return None
            return (curr - prev) / prev

        aud_dev = calc_dev(curr_median_aud, prev_median_aud)
        sp_dev = calc_dev(curr_median_sp, prev_median_sp)

        flag = True
        remark = ""

        # ---------- RULES ----------
        def check_dev(dev, label):
            if dev is None:
                return None

            if abs(dev) > 1:
                return f"{label} deviation >100%"
            elif abs(dev) > 0.5:
                return f"{label} deviation >50%"
            return None

        issues = []

        aud_issue = check_dev(aud_dev, "Audience")
        sp_issue = check_dev(sp_dev, "Spot Price")

        if aud_issue:
            issues.append(aud_issue)

        if sp_issue:
            issues.append(sp_issue)

        if issues:
            flag = False
            remark = " | ".join(issues)

        output.append({
            "Programme Category": category,
            "Channel": channel,
            "Current Audience Median": curr_median_aud,
            "Previous Audience Median": prev_median_aud,
            "Current Spot Price Median": curr_median_sp,
            "Previous Spot Price Median": prev_median_sp,
            "Flag": flag,
            "Remark": remark
        })

    return pd.DataFrame(output)