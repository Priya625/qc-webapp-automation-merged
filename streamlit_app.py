import streamlit as st
import pandas as pd
import requests
import os
import time
import shutil
import json
import requests  # <-- FIX 1: Added missing import
from typing import Optional, List

BACKEND_BASE_URL = os.environ.get("STREAMLIT_BACKEND_URL", "http://localhost:8000")
BACKEND_URL = BACKEND_BASE_URL + "/api"


# --- Import ALL QC functions from ALL your files ---

# Unified QC Engine (your 9 + 11 checks)
try:
    import qc_checks_1 as qc_general
except Exception as e:
    st.error(f"Error importing qc_checks_1.py: {e}")
    st.stop()

# F1 Validator
try:
    from C_data_processing_f1 import BSRValidator
except Exception as e:
    st.error(f"Error importing C_data_processing_f1.py: {e}")
    st.stop()

# EPL Checks (pre + post)
try:
    import epl_checks
except Exception as e:
    st.error(f"Error importing epl_checks.py: {e}")
    st.stop()


# -------------------- ⚙️ Folder setup --------------------
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------- 🧠 Config Loader --------------------
# Helper function to load the config.json file
@st.cache_data
def load_config():
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
        return config
    except Exception as e:
        st.error(f"FATAL ERROR: Could not load config.json. {e}")
        return None

config = load_config()
if config is None:
    st.stop()

# --- FIX 2: Define the BACKEND_URL for the F1 Tab ---
# This is the address of your *other* (Flask/FastAPI) backend.
# Change "http://localhost:8000" if it runs elsewhere.
BACKEND_URL = "http://localhost:8000"


# -------------------- 🌐 Streamlit UI --------------------
LOGO_PATH_4 = "images/Nielsen_Sports_logo.svg"
# C:/Users/BHRAJG2501/Desktop/Nielsen_Sports_logo.svg

# -------------------- 🌐 Streamlit UI --------------------
st.set_page_config(page_title="NIELSEN QC Automation Portal", layout="wide")
# st.title("  Nielsen Sports ")

try:
    if os.path.exists(LOGO_PATH_4):
        st.image(LOGO_PATH_4, width=150) # Adjust width as needed
    else:
        st.header("pic  ") # Fallback header
except Exception:
    st.header("pic")


# --- Use Tabs for Clear Separation (MODIFIED) ---

home_page_tab, main_qc_tab, laliga_qc_tab, f1_tab , epl_tab= st.tabs([
    " Home Page", 
    " Main QC Automation", 
    " Laliga Specific QC", 
    " F1 Market Specific Checks",
    " EPL Specific Checks"
])

# --- Define all market check keys globally for management ---
all_market_check_keys = {
    # 1. Channel and Territory Review
    "check_latam_espn": "LATAM ESPN Channels: Ecuador and Venezuela missing",
    "check_italy_mexico": "Italy and Mexico: Duplications/consolidations",
    "check_channel4plus1": "Specific Channel Checks: Channel 4+1",
    "check_espn4_bsa": "ESPN 4: Latam channel extract from BSA",
    "check_f1_obligations": "Formula 1 Obligations: Missing channels", # <--- F1 Check
    "apply_duplication_weights": "Apply Market Duplication and Upweight Rules (Germany, SA, UK, Brazil, etc.)",
    "check_session_completeness": "Session Count Check: Flag duplicate/over-reported Qualifying, Race, or Training sessions",
    "impute_program_type": "Impute Program Type: Assign Live/Repeat/Highlights/Support based on time matching",
    "duration_limits": "Duration Limits Check: Flag broadcasts outside 5 minutes to 5 hours (QC)",
    "live_date_integrity": "Live Session Date Integrity: Check Live Race/Quali/Train against fixed schedule date",
    "update_audience_from_overnight": "Audience Upscale Check: Update BSR with higher Max Overnight data", 
    "dup_channel_existence": "Duplication Channel Existence: Check if all target channels are in BSR",

    # 2. Broadcaster/Platform Coverage
    "check_youtube_global": "YOUTUBE: ADD YOUTUBE AS PAN-GLOBAL (CPT 8.14)",
    "check_pan_mena": "Pan MENA: BROADCASTER",
    "check_china_tencent": "China Tencent: BROADCASTER",
    "check_czech_slovakia": "Czech Rep and Slovakia: BROADCASTER",
    "check_ant1_greece": "ANT1+ Greece: BROADCASTER (CPT 3.23)",
    "check_india": "India: BROADCASTER",
    "check_usa_espn": "USA ESPN Mail: BROADCASTER",
    "check_dazn_japan": "DAZN Japan: BROADCASTER",
    "check_aztv": "AZTV / IDMAN TV: BROADCASTER",
    "check_rush_caribbean": "RUSH Caribbean: BROADCASTER",
    
    # 3. Removals and Recreations
    "remove_andorra": "Remove Andorra",
    "remove_serbia": "Remove Serbia",
    "remove_montenegro": "Remove Montenegro",
    "remove_brazil_espn_fox": "Remove any ESPN/Fox from Brazil",
    "remove_switz_canal": "Remove Switzerland Canal+ / ServusTV",
    "remove_viaplay_baltics": "Remove viaplay from Latvia, Lithuania, Poland, and Estonia",
    "recreate_viaplay": "Viaplay: Recreate based on a full market of lives",
    "recreate_disney_latam": "Disney+ Latam: Recreate based on a full market of lives",
}


with home_page_tab:
    # --- Custom CSS for Styling ---
    st.markdown(
        """
        <style>
            /* Ensure the overall background color is applied */
            .stApp {
                background-color:  #DCD2FF; 
            }

            .stApp > header {
                text-align: center;
            }

            .stTabs [data-baseweb="tab-list"] {
                justify-content: center;
                gap: 50px; /* INCREASED GAP for more space between tabs */
            }
            
            
            /* Main Header Styling */
            .header-title {
                color: #0049BE; /* Vibrant Corporate Blue */
                font-size: 3.5em;
                font-weight: 900;
                text-align: center;
                padding-top: 80px; /* <-- INCREASED TOP SPACE */
            }
            .subtitle {
                color:  #259600; 
                font-size: 1.3em;
                text-align: center;
                margin-bottom: 8em; /* <-- INCREASED BOTTOM SPACE */
            }
            
            /* Navigation Section (Hero Container) */
            .nav-container {
                background-color: #FFFFFF; /* White background for the action area */
                padding: 40px 50px;
                border-radius: 15px;
                box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15); /* Stronger shadow */
                margin-bottom: 30px;
                text-align: center;
            }
            .nav-container h3 {
                color: #0047AB;
                font-size: 1.8em;
                margin-bottom: 0.5em;
            }
            .nav-item-list {
                list-style-type: none; 
                padding: -100;
                display: flex; /* Flex layout for horizontal tabs/buttons */
                justify-content: space-around;
                margin-top: 20px;
            }
            .nav-item {
                flex: 1;
                margin: 0 10px;
                padding: 15px 20px;
                border: 2px solid #4D577D;
                border-radius: 8px;
                transition: transform 0.2s, border-color 0.2s;
                text-align: center;
                cursor: pointer;
            }
            .nav-item:hover {
                transform: translateY(-3px);
                border-color: #B30095; /* Blue hover accent */
            }

            /* Capability Cards Styling (3-column layout) */
            .metric-card {
                background-color: #F7F7F9;
                border-bottom: 4px solid var(--accent-color); /* Bottom border accent */
                border-radius: 8px;
                padding: 20px 20px;
                box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08); 
                height: 100%;
                transition: box-shadow 0.3s;
            }
            .metric-card:hover {
                box-shadow: 0 6px 15px rgba(0, 0, 0, 0.15); 
            }
            .metric-card h3 {
                color: #1A5276; 
                font-size: 1.2em;
                font-weight: 700;
                margin-bottom: 0.5em;
            }
            .metric-card p {
                font-size: 0.9em;
                color: #555;
            }
            .stHeader {
                background-color: #E4F0F7; /* Ensures Streamlit headers match background */
            }
            /* Targets the entire file uploader container for subtle background changes */
                div[data-testid="stFileUploader"] {
                    background-color: #EAE4FF; /* Light Lavender Background */
                    padding: 10px;
                    border-radius: 10px;
                }
                /* Targets the actual upload button/text area */
                div[data-testid="stFileUploaderDropzone"] {
                    border: 2px dashed #0049BE; /* Custom Border Color */
                }
        </style>
        """,
        unsafe_allow_html=True
    )

    # --- Header Section (Centered) ---
    st.markdown("<div class='header-title'> Nielsen  Automation Portal</div>", unsafe_allow_html=True)
    st.markdown("<p class='subtitle'>The central hub for data integrity, transformation, and complex market modeling for Sports BSR data.</p>", unsafe_allow_html=True)
    
    # --- 1. Navigation Guide (Central Hero Section) ---
    # st.markdown("<div class='nav-container'>", unsafe_allow_html=True)
    st.markdown("<h3>Modules</h3>", unsafe_allow_html=True)
    # st.markdown("<p style='color: #009DA8;'>Select a tab above  to access core functionality.</p>", unsafe_allow_html=True)
    
    # NOTE: Since we cannot programmatically link to Streamlit tabs via HTML/CSS, 
    # this list is for display only, guiding the user to the top tabs.
    st.markdown(
        """
        <ul class='nav-item-list'>
            <li class='nav-item'>
                <strong>Main QC Automation</strong>
            </li>
            <li class='nav-item'>
                <strong>LaLiga Specific QC</strong>
            </li>
            <li class='nav-item'>
                <strong>F1 Market Specific Checks</strong>
            </li>
        </ul>
        """, unsafe_allow_html=True
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<h3 style='color: #1A5276; text-align: center; margin-top: 30px; margin-bottom: 25px;'>Key System Capabilities</h3>", unsafe_allow_html=True)

    # --- 2. Core Capabilities Cards (STAGGERED GRID LAYOUT) ---
    
    # --- Row 1 ---
    cap_row1_col1, cap_row1_col2 = st.columns(2) 
    
    # Card 1: Traceability & Auditing
    with cap_row1_col1:
        st.markdown(
            """
            <div class='metric-card' style='--accent-color:  #FF5AB4;'>
                <h3>Full Data Traceability</h3>
                <p>Ensures 100% auditability for every change—from initial loading to final weighted output—confirming pipeline integrity at every step.</p>
            </div>
            """, unsafe_allow_html=True
        )

    # Card 2: Upscaling & Reconciliation
    with cap_row1_col2:
        st.markdown(
            """
            <div class='metric-card' style='--accent-color: #D13CBD;'>
                <h3>Audience Upscale & Reconciliation</h3>
                <p>Automatically reconciles BSR audience estimates by overriding estimates with higher, verified maximum figures from Overnight Quick Reports.</p>
            </div>
            """, unsafe_allow_html=True
        )
            
    # --- Row 2 ---
    st.markdown("<div style='margin-top: 25px;'></div>", unsafe_allow_html=True)
    cap_row2_col1, cap_row2_col2 = st.columns(2) 

    # Card 3: Complex Market Modeling
    with cap_row2_col1:
        st.markdown(
            """
            <div class='metric-card' style='--accent-color: #FFC800;'>
                <h3>Complex Market Modeling</h3>
                <p>Applies conditional weighted duplication rules and validates channel existence essential for comprehensive pan-regional data models.</p>
            </div>
            """, unsafe_allow_html=True
        )
    
    # Card 4: F1 Duplication Audit
    with cap_row2_col2:
        st.markdown(
            """
            <div class='metric-card' style='--accent-color: #8CE650;'>
                <h3>F1 Duplication Audit</h3>
                <p>Validates the completeness of all duplication rules by checking if required target channels exist in the destination market's current inventory.</p>
            </div>
            """, unsafe_allow_html=True
        )


    st.markdown("<div style='margin-bottom: 50px;'></div>", unsafe_allow_html=True)


###########################################################
#                    GENERAL QC (9 CHECKS)
###########################################################
with main_qc_tab:
    st.header("🧪 General QC Automation")

    col1, col2 = st.columns(2)
    with col1:
        rosco_file = st.file_uploader("Upload Rosco (.xlsx)", type=["xlsx"], key="g_rosco")
    with col2:
        bsr_file = st.file_uploader("Upload BSR (.xlsx)", type=["xlsx"], key="g_bsr")

    if st.button("▶ Run General QC"):
        if not rosco_file or not bsr_file:
            st.error("Please upload both Rosco and BSR files.")
        else:
            with st.spinner("Running General QC..."):

                # Load config
                col_map = config["column_mappings"]
                rules = config["qc_rules"]
                file_rules = config["file_rules"]
                project_rules = config.get("project_rules", {})

                # Save uploaded files
                rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.name)
                bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.name)

                with open(rosco_path, "wb") as f:
                    f.write(rosco_file.getbuffer())
                with open(bsr_path, "wb") as f:
                    f.write(bsr_file.getbuffer())

                try:
                    # ---- EXACT LOGIC FROM api.py/run_general_qc ----
                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])

                    # Cleaning (avoid deprecated applymap: use replace + string-strip)
                    # Replace NBSP and then strip string columns
                    df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
                    # replace NBSP in string values
                    df = df.replace("\xa0", " ", regex=True)
                    # strip whitespace from object/string columns
                    for c in df.select_dtypes(include=["object"]).columns:
                        df[c] = df[c].astype(str).str.strip().replace("nan", pd.NA)

                    df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

                    # Execution order EXACT AS BACKEND:
                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules)
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules.get("program_category", {}), file_rules)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
                    df = qc_general.domestic_market_check(df, project_rules, col_map["bsr"], debug=False)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])
                    df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules.get("client_check", {}))
                    # df = qc_general.rates_and_ratings_check(df, col_map["bsr"])  # Removed redundant call

                    # Duplicated Market BEFORE overlap/daybreak (as api.py)
                    # The second argument is for the 'macro_path'. In General QC, this is None.
                    # We maintain the argument count as per qc_checks_1.py definition (5 positional, 1 keyword).
                    df = qc_general.duplicated_market_check(df, None, project_rules, col_map, file_rules, debug=False)

                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules.get("overlap_check", {}))

                    # Output
                    output_file = f"General_QC_Result_{os.path.splitext(bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    # Remove tz info if present
                    for col in df.select_dtypes(include=["datetimetz"]).columns:
                        try:
                            if hasattr(df[col].dt, "tz"):
                                df[col] = df[col].dt.tz_convert(None)
                        except Exception:
                            pass

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name=file_rules.get("output_sheet_name", "QC Results"))

                    # Post-processing formatting & summary
                    try:
                        qc_general.color_excel(output_path, df)
                    except Exception as e:
                        st.warning(f"color_excel warning: {e}")
                    try:
                        qc_general.generate_summary_sheet(output_path, df, file_rules)
                    except Exception as e:
                        st.warning(f"generate_summary_sheet warning: {e}")

                    st.success("General QC Completed Successfully")
                    with open(output_path, "rb") as f:
                        st.download_button("Download General QC Result", f, file_name=output_file,
                                          mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                except Exception as e:
                    st.error(f"Error during General QC: {e}")


###########################################################
#                    LALIGA QC (11 CHECKS)
###########################################################
with laliga_qc_tab:
    st.header("⚽ LaLiga QC Automation")

    c1, c2, c3 = st.columns(3)
    with c1:
        ll_rosco = st.file_uploader("Upload Rosco (.xlsx)", type=["xlsx"], key="ll_rosco")
    with c2:
        ll_bsr = st.file_uploader("Upload BSR (.xlsx)", type=["xlsx"], key="ll_bsr")
    with c3:
        ll_macro = st.file_uploader("Upload Macro Duplicator", type=["xlsx", "xlsm", "xlsb"], key="ll_macro")

    if st.button("▶ Run LaLiga QC"):
        if not ll_rosco or not ll_bsr or not ll_macro:
            st.error("Please upload Rosco, BSR & Macro files.")
        else:
            with st.spinner("Running LaLiga QC..."):
                col_map = config["column_mappings"]
                rules = config["qc_rules"]
                project = config["project_rules"]
                file_rules = config["file_rules"]

                rosco_path = os.path.join(UPLOAD_FOLDER, ll_rosco.name)
                bsr_path = os.path.join(UPLOAD_FOLDER, ll_bsr.name)
                macro_path = os.path.join(UPLOAD_FOLDER, ll_macro.name)

                for f, p in [(ll_rosco, rosco_path), (ll_bsr, bsr_path), (ll_macro, macro_path)]:
                    with open(p, "wb") as fh:
                        fh.write(f.getbuffer())

                try:
                    # ---- EXACT LOGIC FROM api.py/run_laliga_qc ----
                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])

                    df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
                    df = df.replace("\xa0", " ", regex=True)
                    for c in df.select_dtypes(include=["object"]).columns:
                        df[c] = df[c].astype(str).str.strip().replace("nan", pd.NA)
                    df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules)
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules.get("overlap_check", {}))
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules.get("program_category", {}), file_rules)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])
                    df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules.get("client_check", {}))

                    df = qc_general.domestic_market_check(df, project, col_map["bsr"], debug=False)
                    df = qc_general.duplicated_market_check(df, macro_path, project, col_map, file_rules, debug=False)

                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules.get("overlap_check", {}))

                    # Output
                    output_file = f"Laliga_QC_Result_{os.path.splitext(ll_bsr.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    for col in df.select_dtypes(include=["datetimetz"]).columns:
                        try:
                            if hasattr(df[col].dt, "tz"):
                                df[col] = df[col].dt.tz_convert(None)
                        except Exception:
                            pass

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name=file_rules.get("output_sheet_name", "Laliga QC Results"))

                    try:
                        qc_general.color_excel(output_path, df)
                        qc_general.generate_summary_sheet(output_path, df, file_rules)
                    except Exception as e:
                        st.warning(f"Postprocessing warning: {e}")

                    st.success("LaLiga QC Completed Successfully")
                    with open(output_path, "rb") as f:
                        st.download_button("Download LaLiga QC Result", f, file_name=output_file,
                                          mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                except Exception as e:
                    st.error(f"Error during LaLiga QC: {e}")


###########################################################
#                F1 MARKET SPECIFIC CHECKS
###########################################################
with f1_tab:
    st.header("🏎️ F1 Market Specific Checks")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        bsr_f1 = st.file_uploader("BSR File (.xlsx)", type=["xlsx"], key="f1_bsr")
    with c2:
        f1_obligation = st.file_uploader("Obligation File (.xlsx)", type=["xlsx"], key="f1_ob")
    with c3:
        f1_overnight = st.file_uploader("Overnight File (.xlsx)", type=["xlsx"), key="f1_on")
    with c4:
        f1_macro = st.file_uploader("Macro File (.xlsx/.xlsm)", type=["xlsm", "xlsx", "xls", "xlsb"], key="f1_macro")

    # simple check list (this can be expanded)
    possible_checks = [
        "check_f1_obligations",
        "check_session_completeness",
        "duration_limits",
        "live_date_integrity",
        "update_audience_from_overnight",
        "dup_channel_existence"
    ]

    st.subheader("Select Checks:")
    selected = []
    for ck in possible_checks:
        if st.checkbox(ck, key=f"ck_{ck}"):
            selected.append(ck)

    if st.button("▶ Run F1 Checks"):
        if not bsr_f1:
            st.error("Upload BSR file.")
        else:
            with st.spinner("Running F1 checks..."):
                bsr_path = os.path.join(UPLOAD_FOLDER, bsr_f1.name)
                with open(bsr_path, "wb") as f:
                    f.write(bsr_f1.getbuffer())

                obligation_path = None
                overnight_path = None
                macro_path = None

                if f1_obligation:
                    obligation_path = os.path.join(UPLOAD_FOLDER, f1_obligation.name)
                    with open(obligation_path, "wb") as f:
                        f.write(f1_obligation.getbuffer())

                if f1_overnight:
                    overnight_path = os.path.join(UPLOAD_FOLDER, f1_overnight.name)
                    with open(overnight_path, "wb") as f:
                        f.write(f1_overnight.getbuffer())

                if f1_macro:
                    macro_path = os.path.join(UPLOAD_FOLDER, f1_macro.name)
                    with open(macro_path, "wb") as f:
                        f.write(f1_macro.getbuffer())

                try:
                    # EXACT backend logic:
                    validator = BSRValidator(
                        bsr_path=bsr_path,
                        obligation_path=obligation_path,
                        overnight_path=overnight_path,
                        macro_path=macro_path
                    )

                    summaries = validator.market_check_processor(selected)
                    df_processed = validator.df

                    if df_processed is None or (hasattr(df_processed, "empty") and df_processed.empty):
                        st.error("Processed DataFrame is empty.")
                    else:
                        output_file = f"F1_Processed_{os.path.splitext(bsr_f1.name)[0]}.xlsx"
                        output_path = os.path.join(OUTPUT_FOLDER, output_file)

                        df_processed.to_excel(output_path, index=False)

                        st.success("F1 Checks Completed Successfully")
                        if summaries:
                            st.subheader("Check Summaries")
                            try:
                                st.dataframe(pd.DataFrame(summaries))
                            except Exception:
                                st.write(summaries)

                        with open(output_path, "rb") as f:
                            st.download_button("Download F1 Processed File", f, file_name=output_file,
                                              mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                except Exception as e:
                    st.error(f"F1 Processing Error: {e}")


###########################################################
#                EPL PRE & POST CHECKS
###########################################################
with epl_tab:
    st.header("🏴 EPL Specific Checks")

    st.subheader("EPL Pre Checks")
    p1, p2, p3 = st.columns(3)
    with p1:
        epl_p_bsr = st.file_uploader("BSR File", type=["xlsx"], key="epl_pre_bsr")
    with p2:
        epl_p_rosco = st.file_uploader("Rosco File", type=["xlsx"], key="epl_pre_rosco")
    with p3:
        epl_p_market = st.file_uploader("Market Dup File", type=["xlsx"], key="epl_pre_market")

    if st.button("▶ Run EPL Pre Checks"):
        if not (epl_p_bsr and epl_p_rosco and epl_p_market):
            st.error("Upload BSR, Rosco, and Market Dup files.")
        else:
            with st.spinner("Running EPL Pre Checks..."):

                bsr_path = os.path.join(UPLOAD_FOLDER, epl_p_bsr.name)
                rosco_path = os.path.join(UPLOAD_FOLDER, epl_p_rosco.name)
                market_path = os.path.join(UPLOAD_FOLDER, epl_p_market.name)

                for f, p in [(epl_p_bsr, bsr_path), (epl_p_rosco, rosco_path), (epl_p_market, market_path)]:
                    with open(p, "wb") as fh:
                        fh.write(f.getbuffer())

                try:
                    df = epl_checks.run_pre_checks(bsr_path, rosco_path, market_path)
                    out_file = "EPL_Pre_Checks.xlsx"
                    out_path = os.path.join(OUTPUT_FOLDER, out_file)
                    df.to_excel(out_path, index=False)

                    st.success("EPL Pre Checks Completed")
                    with open(out_path, "rb") as f:
                        st.download_button("Download EPL Pre Check File", f, file_name=out_file,
                                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                except Exception as e:
                    st.error(f"EPL Pre Check Error: {e}")

    st.write("----")
    st.subheader("EPL Post Checks")
    q1, q2, q3 = st.columns(3)
    with q1:
        epl_post_bsr = st.file_uploader("BSR File", type=["xlsx"], key="epl_post_bsr")
    with q2:
        epl_post_rosco = st.file_uploader("Rosco File", type=["xlsx"], key="epl_post_rosco")
    with q3:
        epl_post_macro = st.file_uploader("Macro File", type=["xlsx"], key="epl_post_macro")

    if st.button("▶ Run EPL Post Checks"):
        if not (epl_post_bsr and epl_post_rosco and epl_post_macro):
            st.error("Upload BSR, Rosco, and Macro files.")
        else:
            with st.spinner("Running EPL Post Checks..."):

                bsr_path = os.path.join(UPLOAD_FOLDER, epl_post_bsr.name)
                rosco_path = os.path.join(UPLOAD_FOLDER, epl_post_rosco.name)
                macro_path = os.path.join(UPLOAD_FOLDER, epl_post_macro.name)

                for f, p in [(epl_post_bsr, bsr_path),
                             (epl_post_rosco, rosco_path),
                             (epl_post_macro, macro_path)]:
                    with open(p, "wb") as fh:
                        fh.write(f.getbuffer())

                try:
                    df = epl_checks.run_post_checks(bsr_path, rosco_path, macro_path)
                    out_file = "EPL_Post_Checks.xlsx"
                    out_path = os.path.join(OUTPUT_FOLDER, out_file)
                    df.to_excel(out_path, index=False)

                    st.success("EPL Post Checks Completed")
                    with open(out_path, "rb") as f:
                        st.download_button("Download EPL Post Check File", f, file_name=out_file,
                                           mime="application/vnd.openxmlformats-officedocument-spreadsheetml.sheet")
                except Exception as e:
                    st.error(f"EPL Post Check Error: {e}")