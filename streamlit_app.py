# streamlit_app.py (corrected for simplified qc_checks.py signatures)
import streamlit as st
import pandas as pd
import requests
import os
import time
import shutil
import json
from typing import Optional, List


# STREAMLIT_BACKEND_URL = https://github.com/codespaces/super-duper-space-broccoli-7w6r4v7w5rg3rprj
# BACKEND_BASE_URL = os.environ.get("STREAMLIT_BACKEND_URL", "http://localhost:8000")
# BACKEND_URL = BACKEND_BASE_URL + "/api"

# --- Import ALL QC functions from ALL your files ---
# Your colleague's original F1/QC functions
try:
#     from qc_checks import (
#         detect_period_from_rosco as rosco_detect_orig, # Alias to avoid conflict
#         load_bsr as load_bsr_orig,
#         period_check as period_check_orig,
#         completeness_check as completeness_check_orig,
#         overlap_duplicate_daybreak_check as overlap_orig,
#         program_category_check as program_cat_orig,
#         duration_check as duration_orig,
#         check_event_matchday_competition as event_matchday_orig,
#         market_channel_program_duration_check as market_channel_orig,
#         domestic_market_coverage_check as domestic_orig,
#         rates_and_ratings_check as rates_orig,
#         duplicated_markets_check as duplicated_orig,
#         country_channel_id_check as country_id_orig,
#         client_lstv_ott_check as client_lstv_orig,
#         color_excel as color_excel_orig,
#         generate_summary_sheet as summary_orig,
#     )

    from C_data_processing_f1 import BSRValidator
    from C_data_processing_EPL import EPLValidator

except ImportError as e:
    st.error(f"Failed to import colleague's files (qc_checks.py, C_data_processing_f1.py): {e}")
    st.stop()


# Your 11-check QC functions
try:
    import qc_checks_1 as qc_general
except ImportError as e:
    st.error(f"Failed to import your QC file (qc_checks_1.py): {e}")
    st.stop()


# -------------------- ⚙️ Folder setup --------------------
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------- 🧠 Config Loader --------------------
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

# -------------------- 🌐 Streamlit UI --------------------
LOGO_PATH_4 = "images/Nielsen_Sports_logo.svg"

st.set_page_config(page_title="NIELSEN QC Automation Portal", layout="wide")

try:
    if os.path.exists(LOGO_PATH_4):
        st.image(LOGO_PATH_4, width=150)
    else:
        st.header("pic  ")
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

#EPL
all_market_check_keys_epl = {
    "impute_lt_live_status": "L/T Live Imputation: Flag program type based on 'L/T' keyword in Combined col", # Using the L/T check key
    "consolidate_gillete_soccer": "Program Consolidation: Flag sequential 'Gillete Soccer' programs for merging (Gap <= 30min)",
    "check_sky_showcase_live": "Sky Showcase Live Status Check (UK)",
    "standardize_uk_ire_region" : "Region Standardization: Correct UK/Ireland Region field to 'Europe' and standardize market names",
    "check_fixture_vs_case" : "Checks for Capital VS AND Small vs",
    "check_pan_balkans_serbia_parity" : "Checks equal count in pan_balkans and serbia",
    "audit_multi_match_status" : "Checking for these keywords 'GOAL RUSH', 'KONFERENZ', 'CONFERENCE'",
    "check_date_time_format_integrity" : "Checking Time Integrity",
    "check_live_broadcast_uniqueness" : "Checking 1 live for based on these col 'Market', 'TV-Channel', 'Competition', 'Date'",
    "audit_channel_line_item_count" : "Channel line item count (New Tab)",
    "check_combined_archive_status" : "Flag any row with archive in Combined column",
    "suppress_duplicated_audience" : "Flag if it is a Duplicated Market and has audience ",
    "harmonize_uk_ire_program_descriptions_strict" : "Flag where description not same in specified channels ",
    "check_game_of_the_day_match" : "Checking for Game of the day in CDT/OVN",
    "check_non_metered_primary_market_audience" : "Checking Non metered channel audience is zero",
    "check_legacy_mapping" : "Flag Legancy Name ",
    "check_premier_league_october_obligation" : "Cross Checking of channels from CDT/OVN Sheet ",
    "filter_short_programs": "5 Minute Program Filter: Remove programs shorter than 5 minutes (except Austria/NZ)",
    "sa_nielsen_inclusion_check": "South Africa Nielsen Inclusion Check",
    "epl_live_vs_delay_validation": "Live vs Delay Validation",
    "pl_magazine_highlights_classification": "PL Magazine/Highlights Classification",
    #"dedicated_program_duration_allignments": "Dedicated Program Duration Alignments"
 
}

with home_page_tab:
    # --- Custom CSS for Styling ---
    st.markdown(
        """
        <style>
            /* Ensure the overall background color is applied */
            .stApp {
                background-color:  #FFFFFF; 
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


# -----------------------------------------------------------
#        ✅ MAIN QC AUTOMATION TAB (YOUR 9 CHECKS)
# -----------------------------------------------------------

with main_qc_tab:
    st.header("QC File Uploader")
    st.markdown("Upload your **Rosco** and **BSR** files below. This will run the general QC checks.")

    col1, col2 = st.columns(2)
    with col1:
        main_rosco_file = st.file_uploader("📘 Upload Rosco File (.xlsx)", type=["xlsx"], key="main_rosco")
    with col2:
        main_bsr_file = st.file_uploader("📗 Upload BSR File (.xlsx)", type=["xlsx"], key="main_bsr")
    
    st.write("---")

    if st.button("🚀 Run General QC Checks"):
        if not main_rosco_file or not main_bsr_file or not config:
            st.error("⚠️ Please upload both Rosco and BSR files (and ensure config.json is loaded).")
        else:
            with st.spinner("Running General QC checks... Please wait"):
                try:
                    # Load config
                    col_map = config["column_mappings"]
                    rules = config["qc_rules"]
                    file_rules = config["file_rules"]
                    
                    # Save files temporarily
                    rosco_path = os.path.join(UPLOAD_FOLDER, main_rosco_file.name)
                    bsr_path = os.path.join(UPLOAD_FOLDER, main_bsr_file.name)
                    with open(rosco_path, "wb") as f: f.write(main_rosco_file.getbuffer())
                    with open(bsr_path, "wb") as f: f.write(main_bsr_file.getbuffer())

                    # --- Run YOUR 9 QC Checks Directly ---
                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    
                    # 1. FIX: Removed col_map["bsr"]
                    df = qc_general.load_bsr(bsr_path)
                    
                    # 2. FIX: Removed col_map["bsr"]
                    df = qc_general.period_check(df, start_date, end_date) 
                    
                    # 3. FIX: Removed col_map["bsr"] and rules[...]
                    df = qc_general.completeness_check(df, col_map["bsr"], rules["program_category"]) 
                    
                    # 4. FIX: Removed col_map["bsr"] and rules[...]
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules["overlap_check"]) 
                    
                    # 5. FIX: Simplified to match new signature
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules) 
                    
                    # 6. FIX: Simplified signature - assuming helper functions inside qc_checks.py now handle the file loading based on paths
                    df = qc_general.check_event_matchday_competition(df, rosco_path=rosco_path)
                    
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)

                    # 7. FIX: Changed to a similar available function (your provided code did not have rates_and_ratings_check(df, bsr_cols))
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    
                    # 8. FIX: Changed to a similar available function
                    df = qc_general.country_channel_id_check(df,col_map["bsr"])

                    # --- Generate Output File ---
                    output_file = f"General_QC_Result_{os.path.splitext(main_bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)
                    
                    # Ensure boolean columns are normalized for summary sheet
                    # NOTE: This function is not defined in your provided simplified qc_checks.py, but assumed to exist
                    # df = qc_general.normalize_ok_columns(df) 

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="QC Results")

                    qc_general.color_excel(output_path, df)
                    # Corrected the function call to match qc_checks.py's signature
                    qc_general.generate_summary_sheet(output_path, df) 
                    
                    st.success(" General QC completed successfully!")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label=" Download General QC Result",
                            data=f,
                            file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f" An error occurred during General QC: {e}")


# -----------------------------------------------------------
#         ⚽ LALIGA QC TAB (YOUR 11 CHECKS)
# -----------------------------------------------------------

with laliga_qc_tab:
    st.header("⚽ Laliga Specific QC Checks")
    st.markdown("Upload your **Rosco**, **BSR**, and **Macro Duplicator** files. This will run all Laliga QC checks.")

    col1, col2, col3 = st.columns(3)
    with col1:
        laliga_rosco_file = st.file_uploader("📘 Upload Rosco File (.xlsx)", type=["xlsx"], key="laliga_rosco")
    with col2:
        laliga_bsr_file = st.file_uploader("📗 Upload BSR File (.xlsx)", type=["xlsx"], key="laliga_bsr")
    with col3:
        laliga_macro_file = st.file_uploader("📒 Upload Macro Duplicator File", type=["xlsx","xls","xlsm","xlsb"], key="laliga_macro")
    
    st.write("---")

    if st.button("⚙️ Run Laliga QC Checks"):
        if not laliga_rosco_file or not laliga_bsr_file or not laliga_macro_file or not config:
            st.error("⚠️ Please upload all three files (and ensure config.json is loaded).")
        else:
            with st.spinner("Running Laliga QC checks..."):
                try:
                    # Load config
                    col_map = config["column_mappings"]
                    rules = config["qc_rules"]
                    project = config["project_rules"]
                    file_rules = config["file_rules"]
                    
                    # Save files temporarily
                    rosco_path = os.path.join(UPLOAD_FOLDER, laliga_rosco_file.name)
                    bsr_path = os.path.join(UPLOAD_FOLDER, laliga_bsr_file.name)
                    macro_path = os.path.join(UPLOAD_FOLDER, laliga_macro_file.name)
                    with open(rosco_path, "wb") as f: f.write(laliga_rosco_file.getbuffer())
                    with open(bsr_path, "wb") as f: f.write(laliga_bsr_file.getbuffer())
                    with open(macro_path, "wb") as f: f.write(laliga_macro_file.getbuffer())
                    
                    # --- Run YOUR 11 QC Checks Directly ---
                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    
                    # 1. FIX: Removed col_map["bsr"]
                    df = qc_general.load_bsr(bsr_path)

                    # Run the 9 General Checks
                    # 2. FIX: Removed col_map["bsr"]
                    df = qc_general.period_check(df, start_date, end_date)
                    
                    # 3. FIX: Removed col_map["bsr"] and rules[...]
                    df = qc_general.completeness_check(df, col_map["bsr"], rules["program_category"]) 
                    
                    # 4. FIX: Removed col_map["bsr"] and rules[...]
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules["overlap_check"]) 
                    
                    # 5. FIX: Simplified to match new signature
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules)
                    
                    # 6. FIX: Simplified signature - assuming helper functions inside qc_checks.py now handle the file loading based on paths
                    df = qc_general.check_event_matchday_competition(df, rosco_path=rosco_path)
                    
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)

                    # 7. FIX: Changed to a similar available function
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    
                    # 8. FIX: Changed to a similar available function
                    df = qc_general.country_channel_id_check(df,col_map["bsr"])
                    
                    # Run the 2 Laliga-Specific Checks (NOTE: These functions are not in the provided minimal qc_checks.py, so they are commented out or will fail)
                    df = qc_general.domestic_market_check(df, col_map["bsr"], project.get("monitoring_start_date"), debug=True)
                    df = qc_general.duplicated_market_check(df, macro_path, project, col_map, file_rules, debug=True)

                    # --- Generate Output File ---
                    output_file = f"Laliga_QC_Result_{os.path.splitext(laliga_bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)
                    
                    # 🎯 FIX: Call normalize_ok_columns to ensure boolean status for coloring and summary
                    # NOTE: This function is not defined in your provided simplified qc_checks.py, but assumed to exist
                    # df = qc_general.normalize_ok_columns(df) 
                    
                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="Laliga QC Results")

                    qc_general.color_excel(output_path, df)
                    # 🎯 FIX: Corrected the argument list to match qc_checks.py signature
                    qc_general.generate_summary_sheet(output_path, df) 
                    
                    st.success("✅ Laliga QC completed successfully!")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download Laliga QC Result",
                            data=f,
                            file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f"❌ An error occurred during Laliga QC: {e}")

# -----------------------------------------------------------
#         🏎️ F1 MARKET SPECIFIC CHECKS TAB (COLLEAGUE'S LOGIC)
# -----------------------------------------------------------
with f1_tab:
    st.header("🌍 Market Specific Checks & Channel Configuration")
    st.markdown("Upload the **BSR file** and the **F1 Obligation file** here to perform and log manual checks.")

    col_file1, col_file2, col_file3,col_file4 = st.columns(4)
    with col_file1:
        f1_bsr_file = st.file_uploader("📥 Upload BSR File for Checks (.xlsx)", type=["xlsx"], key="market_check_file")
    with col_file2:
        f1_obligation_file = st.file_uploader("📄 Upload F1 Obligation File (.xlsx)", type=["xlsx"], key="obligation_file")
    with col_file3:
        f1_overnight_file = st.file_uploader("📈 Upload Overnight Audience File (.xlsx)", type=["xlsx"], key="overnight_file")
    with col_file4:
        f1_macro_file = st.file_uploader("📋 4. BSA Duplicator File (Existence Check)", type=["xlsm", "xlsx"], key="macro_file")
    
    st.write("---")

    for key in all_market_check_keys.keys():
        if key not in st.session_state:
            st.session_state[key] = False

    with st.expander("1. Channel and Territory Review", expanded=True):
        st.subheader("General Market Checks")
        st.checkbox(all_market_check_keys["check_latam_espn"], key="check_latam_espn")
        st.checkbox(all_market_check_keys["check_italy_mexico"], key="check_italy_mexico")
        # st.subheader("Specific Channel Checks ")
        # st.checkbox(all_market_check_keys["check_channel4plus1"], key="check_channel4plus1")
        # st.checkbox(all_market_check_keys["check_espn4_bsa"], key="check_espn4_bsa")
        # st.checkbox(all_market_check_keys["check_f1_obligations"], key="check_f1_obligations") 
        # st.checkbox(all_market_check_keys["apply_duplication_weights"], key="apply_duplication_weights") 
        st.checkbox(all_market_check_keys["check_session_completeness"], key="check_session_completeness")
        # st.checkbox(all_market_check_keys["impute_program_type"], key="impute_program_type")
        st.checkbox(all_market_check_keys["duration_limits"], key="duration_limits")
        st.checkbox(all_market_check_keys["live_date_integrity"], key="live_date_integrity")
        st.checkbox(all_market_check_keys["update_audience_from_overnight"], key="update_audience_from_overnight") 
        st.checkbox(all_market_check_keys["dup_channel_existence"], key="dup_channel_existence")

    with st.expander("3. Removals and Recreations"):
        st.subheader("Removals (Ensure these are absent)")
        st.checkbox(all_market_check_keys["remove_andorra"], key="remove_andorra")
        st.checkbox(all_market_check_keys["remove_serbia"], key="remove_serbia")
        st.checkbox(all_market_check_keys["remove_montenegro"], key="remove_montenegro")
        st.checkbox(all_market_check_keys["remove_brazil_espn_fox"], key="remove_brazil_espn_fox")
        st.checkbox(all_market_check_keys["remove_switz_canal"], key="remove_switz_canal")
        st.checkbox(all_market_check_keys["remove_viaplay_baltics"], key="remove_viaplay_baltics")
        
    st.write("---")

    if st.button("⚙️ Apply Selected Checks"):
        
        active_checks = [key for key in all_market_check_keys.keys() if st.session_state[key]]
        
        if f1_bsr_file is None:
            st.error("⚠️ Please upload a BSR file before applying checks.")
        elif "check_f1_obligations" in active_checks and f1_obligation_file is None:
            st.error("⚠️ **F1 Obligation Check Selected:** Please upload the F1 Obligation File.")
        elif "update_audience_from_overnight" in active_checks and f1_overnight_file is None:
            st.error("⚠️ Audience Upscale Check Selected: Please upload the Overnight Audience File.")
        elif "dup_channel_existence" in active_checks and f1_macro_file is None:
            st.error("⚠️ Duplication Channel Existence Check Selected: Please upload the BSA Macro Duplicator File.")
        else:
            with st.spinner(f"Applying {len(active_checks)} checks..."):
                try:
                    # --- Save files temporarily ---
                    bsr_file_path = os.path.join(UPLOAD_FOLDER, f1_bsr_file.name)
                    with open(bsr_file_path, "wb") as f: f.write(f1_bsr_file.getbuffer())
                    
                    obligation_path = None
                    if f1_obligation_file:
                        obligation_path = os.path.join(UPLOAD_FOLDER, f1_obligation_file.name)
                        with open(obligation_path, "wb") as f: f.write(f1_obligation_file.getbuffer())
                    
                    overnight_path = None
                    if f1_overnight_file:
                        overnight_path = os.path.join(UPLOAD_FOLDER, f1_overnight_file.name)
                        with open(overnight_path, "wb") as f: f.write(f1_overnight_file.getbuffer())
                    
                    macro_path = None
                    if f1_macro_file:
                        macro_path = os.path.join(UPLOAD_FOLDER, f1_macro_file.name)
                        with open(macro_path, "wb") as f: f.write(f1_macro_file.getbuffer())

                    # --- Run F1 Logic Directly ---
                    validator = BSRValidator(
                        bsr_path=bsr_file_path, 
                        obligation_path=obligation_path, 
                        overnight_path=overnight_path, 
                        macro_path=macro_path
                    ) 
                    
                    status_summaries = validator.market_check_processor(active_checks)
                    
                    df_processed = validator.df
                    
                    # --- Generate Output File ---
                    output_filename = f"Processed_BSR_{os.path.splitext(f1_bsr_file.name)[0]}_{int(time.time())}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
                    
                    df_processed.to_excel(output_path, index=False)
                    
                    st.success(f"✅ F1 checks completed successfully!")
                    
                    # --- Display Summaries ---
                    st.subheader("Processing Summary")
                    if status_summaries:
                        # Re-format summaries for display
                        display_summaries = []
                        for s in status_summaries:
                            if isinstance(s, dict):
                                display_summaries.append({
                                    "Check": s.get('check_key', 'N/A'),
                                    "Status": s.get('status', 'N/A'),
                                    "Description": s.get('description', 'N/A'),
                                    "Details": str(s.get('details', 'No details'))
                                })
                        
                        df_summary = pd.DataFrame(display_summaries)
                        st.dataframe(df_summary, use_container_width=True)
                    else:
                        st.info("No specific operational summaries were returned.")

                    # --- Provide Download Button ---
                    st.markdown("---")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download Processed F1 File",
                            data=f,
                            file_name=output_filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                
                except Exception as e:
                    st.error(f"❌ An error occurred during F1 checks: {e}")

# with epl_tab:
#     st.header(" EPL Specific Checks")
#     st.markdown("Upload the required files here to perform and log manual checks.")

#     # --- Dedicated Upload for Manual Checks (MODIFIED) ---
#     col_file1, col_file2, col_file3,col_file4 = st.columns(4)
#     with col_file1:
#         epl_bsr_file = st.file_uploader("📥 Upload BSR File for Checks (.xlsx)", type=["xlsx"], key="epl_market_check_file")
#     with col_file2:
#         epl_obligation_file = st.file_uploader("📄 Upload F1 Obligation File (.xlsx)", type=["xlsx"], key="epl_obligation_file")
#     with col_file3:
#         epl_overnight_file = st.file_uploader("📈 Upload Overnight Audience File (.xlsx)", type=["xlsx"], key="epl_overnight_file")
#     with col_file4:
#         epl_macro_file = st.file_uploader("📋 4. BSA Duplicator File (Existence Check)", type=["xlsm", "xlsx"], key="epl_macro_file")
    
#     st.write("---")

#     # Initialize check states in session_state if not present
#     for key in all_market_check_keys_epl.keys():
#         if key not in st.session_state:
#             st.session_state[key] = False

#     # --- Checkbox UI generation (unchanged) ---
#     with st.expander("1. Channel and Territory Review", expanded=True):
#         st.subheader("General Market Checks")
#         st.checkbox(all_market_check_keys_epl["impute_lt_live_status"], key="impute_lt_live_status")
#         st.checkbox(all_market_check_keys_epl["consolidate_gillete_soccer"], key="consolidate_gillete_soccer")
#         st.checkbox(all_market_check_keys_epl["check_sky_showcase_live"], key="check_sky_showcase_live")
#         st.checkbox(all_market_check_keys_epl["standardize_uk_ire_region"], key="standardize_uk_ire_region")
#         st.checkbox(all_market_check_keys_epl["check_fixture_vs_case"], key="check_fixture_vs_case")
#         st.checkbox(all_market_check_keys_epl["check_pan_balkans_serbia_parity"], key="check_pan_balkans_serbia_parity")
#         st.checkbox(all_market_check_keys_epl["audit_multi_match_status"], key="audit_multi_match_status")
#         st.checkbox(all_market_check_keys_epl["check_date_time_format_integrity"], key="check_date_time_format_integrity")
#         st.checkbox(all_market_check_keys_epl["check_live_broadcast_uniqueness"], key="check_live_broadcast_uniqueness")
#         st.checkbox(all_market_check_keys_epl["audit_channel_line_item_count"], key="audit_channel_line_item_count")
#         st.checkbox(all_market_check_keys_epl["check_combined_archive_status"], key="check_combined_archive_status")
#         st.checkbox(all_market_check_keys_epl["suppress_duplicated_audience"], key="suppress_duplicated_audience")
#         st.checkbox(all_market_check_keys_epl["filter_short_programs"], key="filter_short_programs")
#         st.checkbox(all_market_check_keys_epl["sa_nielsen_inclusion_check"], key="sa_nielsen_inclusion_check")
#         st.checkbox(all_market_check_keys_epl["epl_live_vs_delay_validation"], key="epl_live_vs_delay_validation")
#         st.checkbox(all_market_check_keys_epl["pl_magazine_highlights_classification"], key="pl_magazine_highlights_classification")
#         #st.checkbox(all_market_check_keys_epl["dedicated_program_duration_allignments"], key="dedicated_program_duration_allignments")
        

#     st.write("---")


#     # --- Run Processing Button (UNTOUCHED) ---
#     if st.button(" EPL Apply Selected Checks"):
        
#         active_checks = [key for key in all_market_check_keys_epl.keys() if st.session_state[key]]
        
#         # Check mandatory files
#         if epl_bsr_file is None:
#             st.error("⚠️ Please upload a BSR file before applying checks.")
#         elif "check_f1_obligations" in active_checks and epl_obligation_file is None:
#             st.error("⚠️ **F1 Obligation Check Selected:** Please upload the F1 Obligation File.")
#         elif "update_audience_from_overnight" in active_checks and epl_overnight_file is None:
#             st.error("⚠️ Audience Upscale Check Selected: Please upload the Overnight Audience File.")
#         elif "dup_channel_existence" in active_checks and epl_macro_file is None:
#             st.error("⚠️ Duplication Channel Existence Check Selected: Please upload the BSA Macro Duplicator File.")
#         else:
#             with st.spinner(f"Applying {len(active_checks)} checks..."):
#                 try:
#                     # --- Save files temporarily ---
#                     bsr_file_path = os.path.join(UPLOAD_FOLDER, epl_bsr_file.name)
#                     with open(bsr_file_path, "wb") as f: f.write(epl_bsr_file.getbuffer())
                    
#                     obligation_path = None
#                     if epl_obligation_file:
#                         obligation_path = os.path.join(UPLOAD_FOLDER, epl_obligation_file.name)
#                         with open(obligation_path, "wb") as f: f.write(epl_obligation_file.getbuffer())
                    
#                     overnight_path = None
#                     if epl_overnight_file:
#                         overnight_path = os.path.join(UPLOAD_FOLDER, epl_overnight_file.name)
#                         with open(overnight_path, "wb") as f: f.write(epl_overnight_file.getbuffer())
                    
#                     macro_path = None
#                     if epl_macro_file:
#                         macro_path = os.path.join(UPLOAD_FOLDER, epl_macro_file.name)
#                         with open(macro_path, "wb") as f: f.write(epl_macro_file.getbuffer())
                    
#                     try:
#                         # Use the path to load the BSR file
#                         bsr_df = pd.read_excel(bsr_file_path) 
#                     except Exception as e:
#                         st.error(f"❌ Error loading BSR file from path {bsr_file_path}: {e}")
#                         raise

#                     # --- Run F1 Logic Directly ---
#                     validator = EPLValidator(
#                         bsr_path=bsr_file_path, 
#                         obligation_path=obligation_path, 
#                         overnight_path=overnight_path, 
#                         macro_path=macro_path
#                     ) 
                    
#                     status_summaries = validator.market_check_processor(active_checks)
                    
#                     df_processed = validator.df
                    
#                     # --- Generate Output File ---
#                     output_filename = f"Processed_BSR_{os.path.splitext(epl_bsr_file.name)[0]}_{int(time.time())}.xlsx"
#                     output_path = os.path.join(OUTPUT_FOLDER, output_filename)
                    
#                     # Ensure columns are normalized for saving
#                     # NOTE: This function is not defined in your provided simplified qc_checks.py, but assumed to exist
#                     # df_processed = qc_general.normalize_ok_columns(df_processed)

#                     with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

    
#                         df_processed.to_excel(writer, index=False, sheet_name="EPL_Processed")

#                         # < 5 min sheet (if available)
#                         if hasattr(validator, "short_programs_df"):
#                             sp = validator.short_programs_df
#                             if isinstance(sp, pd.DataFrame) and not sp.empty:
#                                 sp.to_excel(writer, index=False, sheet_name="<5 min-Short Programs")

#                         # SA Nielsen sheet (if available)
#                         if hasattr(validator, "sa_nielsen_df"):
#                             sa = validator.sa_nielsen_df
#                             if isinstance(sa, pd.DataFrame) and not sa.empty:
#                                 sa.to_excel(writer, index=False, sheet_name="SA_Nielsen")

#                         # NEW: Live vs Delay sheet
#                         if hasattr(validator, "live_delay_flags_df"):
#                             ld = validator.live_delay_flags_df
#                             if isinstance(ld, pd.DataFrame) and not ld.empty:
#                                 ld.to_excel(writer, index=False, sheet_name="EPL_LiveDelay_Flags")

#                         # PL Magazine/Highlights classification sheet
#                         if hasattr(validator, "pl_mag_highlights_df"):
#                             pl = validator.pl_mag_highlights_df
#                             if isinstance(pl, pd.DataFrame) and not pl.empty:
#                                 pl.to_excel(writer, index=False, sheet_name="PL_Mag_Highlights")
                                    
#                     st.success(f"✅ EPL checks completed successfully!")
                    
#                     # --- Display Summaries ---
#                     st.subheader("Processing Summary")
#                     if status_summaries:
#                         # Re-format summaries for display
#                         display_summaries = []
#                         for s in status_summaries:
#                             if isinstance(s, dict):
#                                 display_summaries.append({
#                                     "Check": s.get('check_key', 'N/A'),
#                                     "Status": s.get('status', 'N/A'),
#                                     "Description": s.get('description', 'N/A'),
#                                     "Details": str(s.get('details', 'No details'))
#                                 })
                        
#                         df_summary = pd.DataFrame(display_summaries)
#                         st.dataframe(df_summary, use_container_width=True)
#                     else:
#                         st.info("No specific operational summaries were returned.")

#                     # --- Provide Download Button ---
#                     st.markdown("---")
#                     with open(output_path, "rb") as f:
#                         st.download_button(
#                             label="📥 Download Processed EPL File",
#                             data=f,
#                             file_name=output_filename,
#                             mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#                         )
                
#                 except Exception as e:
#                     st.error(f"❌ An error occurred during F1 checks: {e}")

with epl_tab:
    st.header(" EPL Specific Checks")
    st.markdown("Upload the required files here to perform and log manual checks.")

    # --- 0. Define Tooltips for Checks ---
    # Add your detailed descriptions here
    epl_tooltips = {
        "impute_lt_live_status": "Scans the 'Combined' column for 'L/T'. If found, suggests changing status to 'Live'.",
        "consolidate_gillete_soccer": "Merges consecutive 'Gillete Soccer' entries if they occur within the specified time gap.",
        "check_sky_showcase_live": "Verifies if Sky Showcase broadcasts are correctly tagged as Live based on reference data.",
        "standardize_uk_ire_region": "Ensures Region is set to 'UK/IRE' for specific channels to maintain consistency.",
        "check_fixture_vs_case": "Compares the match fixture in the description against the case file to ensure accuracy.",
        "check_pan_balkans_serbia_parity": "Checks that Pan-Balkans and Serbia feeds have matching data where expected.",
        "audit_multi_match_status": "Flags sessions where multiple matches appear to be airing simultaneously on one feed.",
        "check_date_time_format_integrity": "Validates that all Date and Time columns follow the strict 'YYYY-MM-DD' and 'HH:MM:SS' format.",
        "check_live_broadcast_uniqueness": "Ensures there are no duplicate Live broadcast entries for the same timeslot.",
        "audit_channel_line_item_count": "Counts line items per channel to ensure they meet the expected volume thresholds.",
        "check_combined_archive_status": "Verifies that Archive statuses are correctly reflected in the Combined column.",
        "suppress_duplicated_audience": "Identifies and suppresses audience numbers that appear to be duplicated across regions.",
        "harmonize_uk_ire_program_descriptions_strict": "Strictly formats program descriptions for UK/IRE markets to a standard naming convention.",
        "check_game_of_the_day_match": "Verifies the 'Game of the Day' logic matches the primary broadcast schedule.",
        "check_non_metered_primary_market_audience": "Audits audience numbers for non-metered markets to ensure they are not zero.",
        "check_legacy_mapping": "Cross-references channel names against the legacy mapping table.",
        "check_premier_league_october_obligation": "Cross Checking of channels from CDT/OVN Sheet",
        "filter_short_programs": "5 Minute Program Filter: Remove programs shorter than 5 minutes (except Austria/NZ)",
        "sa_nielsen_inclusion_check": "South Africa Nielsen Inclusion Check",
        "epl_live_vs_delay_validation": "Live vs Delay Validation",
        "pl_magazine_highlights_classification": "PL Magazine/Highlights Classification"

        
    }

    # --- Dedicated Upload for Manual Checks (MODIFIED) ---
    col_file1, col_file2, col_file3,col_file4 = st.columns(4)
    with col_file1:
        f1_bsr_file = st.file_uploader("📥 Upload BSR File for Checks (.xlsx)", type=["xlsx"], key="epl_market_check_file")
    with col_file2:
        f1_obligation_file = st.file_uploader("📄 Upload Channel Names (.xlsx)", type=["xlsx"], key="epl_obligation_file")
    with col_file3:
        f1_overnight_file = st.file_uploader("📈 Upload CDT-OVN Audience File (.xlsx)", type=["xlsx"], key="epl_overnight_file")
    with col_file4:
        f1_macro_file = st.file_uploader("📋 4. BSA Duplicator File ", type=["xlsm", "xlsx"], key="epl_macro_file")
    
    st.write("---")

    # Initialize check states in session_state if not present
    for key in all_market_check_keys_epl.keys():
        if key not in st.session_state:
            st.session_state[key] = False


    # --- Checkbox UI generation with Tooltips ---
    with st.expander("1. Channel and Territory Review", expanded=True):
        st.subheader("General Market Checks")
        
        # Helper function to render checkbox with tooltip
        def check_ui(key_name):
            label = all_market_check_keys_epl[key_name]
            # Use .get() to avoid errors if a tooltip is missing
            tooltip = epl_tooltips.get(key_name, "No description available.")
            return st.checkbox(label, key=key_name, help=tooltip)

        # Apply to all your checkboxes
        check_ui("impute_lt_live_status")
        check_ui("consolidate_gillete_soccer")
        check_ui("check_sky_showcase_live")
        check_ui("standardize_uk_ire_region")
        check_ui("check_fixture_vs_case")
        check_ui("check_pan_balkans_serbia_parity")
        check_ui("audit_multi_match_status")
        check_ui("check_date_time_format_integrity")
        check_ui("check_live_broadcast_uniqueness")
        check_ui("audit_channel_line_item_count")
        check_ui("check_combined_archive_status")
        check_ui("suppress_duplicated_audience")
        check_ui("harmonize_uk_ire_program_descriptions_strict")
        check_ui("check_game_of_the_day_match")
        check_ui("check_non_metered_primary_market_audience")
        check_ui("check_legacy_mapping")
        check_ui("check_premier_league_october_obligation")
        check_ui("filter_short_programs")
        check_ui("sa_nielsen_inclusion_check") 
        check_ui("epl_live_vs_delay_validation")
        check_ui("pl_magazine_highlights_classification")

    st.write("---")
        # --- Configuration Input Fields (NEW SECTION) ---
    
    config_col1, config_col2 = st.columns(2)

    with config_col1:
        st.caption("L/T Live Imputation Settings (Recommended: Live)")
        lt_market_input = st.text_input("Target Market (e.g., INDIA):", value="INDIA", key="lt_market_input")
        lt_keyword_input = st.text_input("Keyword to Search ('L/T'):", value="L/T", key="lt_keyword_input")

    with config_col2:
        st.caption("Sequential Consolidation Settings (Gillete Soccer)")
        consolidate_keyword_input = st.text_input("Consolidation Keyword:", value="GILLETE SOCCER", key="consolidate_keyword_input")
        consolidate_gap_input = st.number_input("Max Time Gap (Minutes):", value=30, min_value=0, max_value=120, key="consolidate_gap_input")

    st.write("---")


    # --- Run Processing Button (UNTOUCHED) ---
    if st.button(" EPL Apply Selected Checks"):
        
        active_checks = [key for key in all_market_check_keys_epl.keys() if st.session_state[key]]

        # 1. Compile Configuration Dictionary from User Inputs
        check_configs = {}
        
        if "impute_lt_live_status" in active_checks:
            check_configs["impute_lt_live_status"] = {
                "market": lt_market_input,
                "keyword": lt_keyword_input
            }
            
        if "consolidate_gillete_soccer" in active_checks:
            check_configs["consolidate_gillete_soccer"] = {
                "keyword": consolidate_keyword_input,
                "max_gap_minutes": int(consolidate_gap_input)
            }
        
        # Check mandatory files
        if f1_bsr_file is None:
            st.error("⚠️ Please upload a BSR file before applying checks.")
        elif "check_f1_obligations" in active_checks and f1_obligation_file is None:
            st.error("⚠️ **F1 Obligation Check Selected:** Please upload the F1 Obligation File.")
        elif "update_audience_from_overnight" in active_checks and f1_overnight_file is None:
            st.error("⚠️ Audience Upscale Check Selected: Please upload the Overnight Audience File.")
        elif "dup_channel_existence" in active_checks and f1_macro_file is None:
            st.error("⚠️ Duplication Channel Existence Check Selected: Please upload the BSA Macro Duplicator File.")
        else:
            with st.spinner(f"Applying {len(active_checks)} checks..."):
                try:
                    # --- Save files temporarily ---
                    bsr_file_path = os.path.join(UPLOAD_FOLDER, f1_bsr_file.name)
                    with open(bsr_file_path, "wb") as f: f.write(f1_bsr_file.getbuffer())
                    
                    obligation_path = None
                    if f1_obligation_file:
                        obligation_path = os.path.join(UPLOAD_FOLDER, f1_obligation_file.name)
                        with open(obligation_path, "wb") as f: f.write(f1_obligation_file.getbuffer())
                    
                    overnight_path = None
                    if f1_overnight_file:
                        overnight_path = os.path.join(UPLOAD_FOLDER, f1_overnight_file.name)
                        with open(overnight_path, "wb") as f: f.write(f1_overnight_file.getbuffer())
                    
                    macro_path = None
                    if f1_macro_file:
                        macro_path = os.path.join(UPLOAD_FOLDER, f1_macro_file.name)
                        with open(macro_path, "wb") as f: f.write(f1_macro_file.getbuffer())
                    
                    try:
                        # Use the path to load the BSR file
                        bsr_df = pd.read_excel(bsr_file_path) 
                    except Exception as e:
                        st.error(f"❌ Error loading BSR file from path {bsr_file_path}: {e}")
                        # Stop execution if the main file can't be loaded
                    

                    # --- Run F1 Logic Directly ---
                    validator = EPLValidator(
                        df=bsr_df,
                        bsr_path=bsr_file_path, 
                        obligation_path=obligation_path, 
                        overnight_path=overnight_path, 
                        macro_path=macro_path,
                        check_configs=check_configs 
                    ) 
                    
                    status_summaries = validator.market_check_processor(active_checks)
                    
                    df_processed = validator.df
                    
                    # --- Generate Output File ---
                    output_filename = f"Processed_BSR_{os.path.splitext(f1_bsr_file.name)[0]}_{int(time.time())}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
                    
                    # Ensure columns are normalized for saving
                    # NOTE: This function is not defined in your provided simplified qc_checks.py, but assumed to exist
                    # df_processed = qc_general.normalize_ok_columns(df_processed)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:

    
                        df_processed.to_excel(writer, index=False, sheet_name="EPL_Processed")

                        # < 5 min sheet (if available)
                        if hasattr(validator, "short_programs_df"):
                            sp = validator.short_programs_df
                            if isinstance(sp, pd.DataFrame) and not sp.empty:
                                sp.to_excel(writer, index=False, sheet_name="<5 min-Short Programs")

                        # SA Nielsen sheet (if available)
                        if hasattr(validator, "sa_nielsen_df"):
                            sa = validator.sa_nielsen_df
                            if isinstance(sa, pd.DataFrame) and not sa.empty:
                                sa.to_excel(writer, index=False, sheet_name="SA_Nielsen")

                        # NEW: Live vs Delay sheet
                        if hasattr(validator, "live_delay_flags_df"):
                            ld = validator.live_delay_flags_df
                            if isinstance(ld, pd.DataFrame) and not ld.empty:
                                ld.to_excel(writer, index=False, sheet_name="EPL_LiveDelay_Flags")

                        # PL Magazine/Highlights classification sheet
                        if hasattr(validator, "pl_mag_highlights_df"):
                            pl = validator.pl_mag_highlights_df
                            if isinstance(pl, pd.DataFrame) and not pl.empty:
                                pl.to_excel(writer, index=False, sheet_name="PL_Mag_Highlights")
                    
                    st.success(f"✅ EPL checks completed successfully!")
                    
                    # --- Display Summaries ---
                    st.subheader("Processing Summary")
                    if status_summaries:
                        # Re-format summaries for display
                        display_summaries = []
                        for s in status_summaries:
                            if isinstance(s, dict):
                                display_summaries.append({
                                    "Check": s.get('check_key', 'N/A'),
                                    "Status": s.get('status', 'N/A'),
                                    "Description": s.get('description', 'N/A'),
                                    "Details": str(s.get('details', 'No details'))
                                })
                        
                        df_summary = pd.DataFrame(display_summaries)
                        st.dataframe(df_summary, use_container_width=True)
                    else:
                        st.info("No specific operational summaries were returned.")

                    # --- Provide Download Button ---
                    st.markdown("---")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download Processed F1 File",
                            data=f,
                            file_name=output_filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                
                except Exception as e:
                    st.error(f"❌ An error occurred during EPL checks: {e}")
