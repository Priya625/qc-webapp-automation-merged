import streamlit as st
import pandas as pd
import requests
import os
import time
import shutil
import json
from typing import Optional, List

BACKEND_BASE_URL = os.environ.get("STREAMLIT_BACKEND_URL", "http://localhost:8000")
BACKEND_URL = BACKEND_BASE_URL + "/api"


# --- Import ALL QC functions from ALL your files ---

# Your colleague's original F1/QC functions
try:
    from qc_checks import (
        detect_period_from_rosco as rosco_detect_orig, # Alias to avoid conflict
        load_bsr as load_bsr_orig,
        period_check as period_check_orig,
        completeness_check as completeness_check_orig,
        overlap_duplicate_daybreak_check as overlap_orig,
        program_category_check as program_cat_orig,
        duration_check as duration_orig,
        check_event_matchday_competition as event_matchday_orig,
        market_channel_program_duration_check as market_channel_orig,
        domestic_market_coverage_check as domestic_orig,
        rates_and_ratings_check as rates_orig,
        duplicated_markets_check as duplicated_orig,
        country_channel_id_check as country_id_orig,
        client_lstv_ott_check as client_lstv_orig,
        color_excel as color_excel_orig,
        generate_summary_sheet as summary_orig,
    )
    from C_data_processing_f1 import BSRValidator
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


# -----------------------------------------------------------
#        ✅ MAIN QC AUTOMATION TAB (YOUR 9 CHECKS)
# -----------------------------------------------------------

with main_qc_tab:
    st.header("QC File Uploader")
    st.markdown("Upload your **Rosco** and **BSR** files below. This will run the 9 general QC checks.")

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
            with st.spinner("Running General QC checks... Please wait ⏳"):
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
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])
                    
                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules["program_category"])
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules["overlap_check"])
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])
                    df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules["client_check"])

                    # --- Generate Output File ---
                    output_file = f"General_QC_Result_{os.path.splitext(main_bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="QC Results")

                    qc_general.color_excel(output_path, df)
                    qc_general.generate_summary_sheet(output_path, df, file_rules)
                    
                    st.success("✅ General QC completed successfully!")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download General QC Result",
                            data=f,
                            file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f"❌ An error occurred during General QC: {e}")


# -----------------------------------------------------------
#         ⚽ LALIGA QC TAB (YOUR 11 CHECKS)
# -----------------------------------------------------------

with laliga_qc_tab:
    st.header("⚽ Laliga Specific QC Checks")
    st.markdown("Upload your **Rosco**, **BSR**, and **Macro Duplicator** files. This will run all 11 QC checks.")

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
            with st.spinner("Running all 11 Laliga QC checks..."):
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
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])

                    # Run the 9 General Checks
                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules["program_category"])
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules["overlap_check"])
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])
                    df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules["client_check"])
                    
                    # Run the 2 Laliga-Specific Checks
                    df = qc_general.domestic_market_check(df, project, col_map["bsr"], debug=True)
                    df = qc_general.duplicated_market_check(df, macro_path, project, col_map, file_rules, debug=True)

                    # --- Generate Output File ---
                    output_file = f"Laliga_QC_Result_{os.path.splitext(laliga_bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="Laliga QC Results")

                    qc_general.color_excel(output_path, df)
                    qc_general.generate_summary_sheet(output_path, df, file_rules)
                    
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
    st.header(" Formula 1 Specific Checks")
    st.markdown("Upload the required files here to perform and log manual checks.")

    # --- Dedicated Upload for Manual Checks (MODIFIED) ---
    col_file1, col_file2, col_file3,col_file4 = st.columns(4) # <-- Increase columns to 3
    with col_file1:
        market_check_file = st.file_uploader("📥 Upload BSR File for Checks (.xlsx)", type=["xlsx"], key="market_check_file")
    with col_file2:
        obligation_file = st.file_uploader("📄 Upload F1 Obligation File (.xlsx)", type=["xlsx"], key="obligation_file")
    with col_file3: # <-- NEW UPLOADER
        overnight_file = st.file_uploader("📈 Upload Overnight Audience File (.xlsx)", type=["xlsx"], key="overnight_file") # <-- NEW
    with col_file4: # <-- NEW UPLOADER
        macro_file = st.file_uploader("📋 4. BSA Duplicator File", type=["xlsm", "xlsx"], key="macro_file") # <-- NEW
    
    st.write("---")

    # Initialize check states in session_state if not present
    for key in all_market_check_keys.keys():
        if key not in st.session_state:
            st.session_state[key] = False

    # --- Checkbox UI generation (unchanged) ---
    with st.expander("1. Channel and Territory Review", expanded=True):
        st.subheader("General Market Checks")
        st.checkbox(all_market_check_keys["check_latam_espn"], key="check_latam_espn")
        st.checkbox(all_market_check_keys["check_italy_mexico"], key="check_italy_mexico")
        
        st.subheader("Specific Channel Checks (against uploaded file)")
        # st.checkbox(all_market_check_keys["check_channel4plus1"], key="check_channel4plus1")
        # st.checkbox(all_market_check_keys["check_espn4_bsa"], key="check_espn4_bsa")
        st.checkbox(all_market_check_keys["check_f1_obligations"], key="check_f1_obligations") # <--- F1 Check
        # st.checkbox(all_market_check_keys["apply_duplication_weights"], key="apply_duplication_weights") # <--- F1 Check
        st.checkbox(all_market_check_keys["check_session_completeness"], key="check_session_completeness")
        # st.checkbox(all_market_check_keys["impute_program_type"], key="impute_program_type")
        st.checkbox(all_market_check_keys["duration_limits"], key="duration_limits")
        st.checkbox(all_market_check_keys["live_date_integrity"], key="live_date_integrity")
        st.checkbox(all_market_check_keys["update_audience_from_overnight"], key="update_audience_from_overnight") # <-- NEW
        
        st.checkbox(all_market_check_keys["dup_channel_existence"], key="dup_channel_existence") # <-- NEW CHECKBOX

    # ... (rest of the checkboxes remain here) ...
    # with st.expander("2. Broadcaster/Platform Coverage (BROADCASTER/GLOBAL)"):
    #     st.subheader("Global/Platform Adds")
    #     st.checkbox(all_market_check_keys["check_youtube_global"], key="check_youtube_global")
        
    #     st.subheader("Individual Broadcaster Confirmations")
    #     st.checkbox(all_market_check_keys["check_pan_mena"], key="check_pan_mena")
    #     st.checkbox(all_market_check_keys["check_china_tencent"], key="check_china_tencent")
    #     st.checkbox(all_market_check_keys["check_czech_slovakia"], key="check_czech_slovakia")
    #     st.checkbox(all_market_check_keys["check_ant1_greece"], key="check_ant1_greece")
    #     st.checkbox(all_market_check_keys["check_india"], key="check_india")
    #     st.checkbox(all_market_check_keys["check_usa_espn"], key="check_usa_espn")
    #     st.checkbox(all_market_check_keys["check_dazn_japan"], key="check_dazn_japan")
    #     st.checkbox(all_market_check_keys["check_aztv"], key="check_aztv")
    #     st.checkbox(all_market_check_keys["check_rush_caribbean"], key="check_rush_caribbean")


    with st.expander("3. Removals and Recreations"):
        st.subheader("Removals (Ensure these are absent)")
        st.checkbox(all_market_check_keys["remove_andorra"], key="remove_andorra")
        st.checkbox(all_market_check_keys["remove_serbia"], key="remove_serbia")
        st.checkbox(all_market_check_keys["remove_montenegro"], key="remove_montenegro")
        st.checkbox(all_market_check_keys["remove_brazil_espn_fox"], key="remove_brazil_espn_fox")
        st.checkbox(all_market_check_keys["remove_switz_canal"], key="remove_switz_canal")
        st.checkbox(all_market_check_keys["remove_viaplay_baltics"], key="remove_viaplay_baltics")

        # st.subheader("Recreations (Check for full market coverage)")
        # st.checkbox(all_market_check_keys["recreate_viaplay"], key="recreate_viaplay")
        # st.checkbox(all_market_check_keys["recreate_disney_latam"], key="recreate_disney_latam")
        
    st.write("---")


    # --- Run Processing Button (UNTOUCHED) ---
    if st.button(" Apply Selected Checks"):
        
        active_checks = [key for key in all_market_check_keys.keys() if st.session_state[key]]
        
        # Check mandatory files
        if market_check_file is None:
            st.error("⚠️ Please upload a BSR file before applying checks.")
        elif "check_f1_obligations" in active_checks and obligation_file is None:
            st.error("⚠️ **F1 Obligation Check Selected:** Please upload the F1 Obligation File.")
        elif "update_audience_from_overnight" in active_checks and overnight_file is None: # <-- NEW CHECK
            st.error("⚠️ Audience Upscale Check Selected: Please upload the Overnight Audience File.") # <-- NEW ERROR MESSAGE
        elif "dup_channel_existence" in active_checks and macro_file is None: # <-- NEW DEPENDENCY CHECK
            st.error("⚠️ Duplication Channel Existence Check Selected: Please upload the BSA Macro Duplicator File.")
        else:
            with st.spinner(f"Applying {len(active_checks)} checks on the backend..."):
                
                # 2. Prepare files for backend
                files = {
                    'bsr_file': (market_check_file.name, market_check_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                }
                
                # CONDITIONAL ADDITION OF OBLIGATION FILE
                if obligation_file:
                    files['obligation_file'] = (obligation_file.name, obligation_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

                # CONDITIONAL ADDITION OF OVERNIGHT FILE <--- NEW LOGIC
                if overnight_file:
                    files['overnight_file'] = (overnight_file.name, overnight_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

                if macro_file: # <-- ADD NEW FILE TO REQUEST
                    files['macro_file'] = (macro_file.name, macro_file.getbuffer(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

                # Send active checks as form data
                data = {'checks': active_checks} 

                try:
                    # 3. Call the backend endpoint
                    response = requests.post(
                        f"{BACKEND_URL}/market_check_and_process", 
                        files=files, 
                        data=data,
                        timeout=600
                    )

                    if response.status_code == 200:
                        # 4. Success: Handle the JSON response (unchanged)
                        try:
                            result_json = response.json()
                            summaries = result_json.get("summaries", [])
                            download_url_suffix = result_json.get("download_url")
                            message = result_json.get("message", "Processing complete.")
                            
                            # Construct the full download URL using the base URL
                            full_download_url = f"http://localhost:8000{download_url_suffix}"

                            st.success(f"✅ Checks completed successfully! {message}")
                            
                            # --- Display Summaries ---
                            st.subheader("Processing Summary")
                            if summaries:
                                # ... (summary display logic unchanged) ...
                                df_summary = pd.DataFrame(summaries)
                                
                                df_summary_display = df_summary.copy()

                                if 'details' in df_summary.columns:
                                    
                                    df_summary_display['Market'] = df_summary['details'].apply(
                                        lambda d: d.get('market_affected', d.get('markets_context', 'Global/N/A'))
                                    )
                                    
                                    def get_change_count(d):
                                        if 'rows_removed' in d: return d['rows_removed']
                                        if 'total_issues_flagged' in d: return d['total_issues_flagged']
                                        if 'rows_added' in d: return d['rows_added']
                                        if 'broadcasters_missing' in d: return d['broadcasters_missing'] 
                                        return 0
                                        
                                    df_summary_display['Change Count'] = df_summary['details'].apply(get_change_count)
                                    
                                    df_summary_display = df_summary_display.rename(columns={
                                        "description": "Operation", 
                                        "status": "Status"
                                    })
                                    
                                    df_summary_display = df_summary_display[[
                                        'Status', 
                                        'Operation', 
                                        'Market', 
                                        'Change Count', 
                                        'check_key'
                                    ]].set_index('check_key')
                                else:
                                    df_summary_display = df_summary_display.rename(columns={
                                        "description": "Operation", 
                                        "status": "Status"
                                    })
                                    if 'check_key' in df_summary_display.columns:
                                            df_summary_display = df_summary_display[['Status', 'Operation', 'check_key']].set_index('check_key')
                                            
                                st.dataframe(df_summary_display, use_container_width=True)
                                
                                # --- Display Duplicates Dataframe (UNCHANGED) ---
                                dupe_summary = next((s for s in summaries if s.get('check_key') == 'check_italy_mexico' and s['details'].get('duplicate_data')), None)
                                
                                if dupe_summary and dupe_summary['details']['duplicate_data']:
                                    duplicate_data = dupe_summary['details']['duplicate_data']
                                    st.subheader("⚠️ Duplicate Rows Found and Consolidated (Italy/Mexico)")
                                    
                                    duplicates_df = pd.DataFrame(duplicate_data)
                                    st.dataframe(duplicates_df, use_container_width=True)
                                    st.caption(
                                        f"The table above shows {len(duplicates_df)} rows involved in the duplicate sets (including the one kept). "
                                        f"**{dupe_summary['details'].get('rows_removed', 0)}** rows were removed."
                                    )

                            else:
                                st.info("No specific operational summaries were returned.")

                            # --- Provide Download Button (UNCHANGED) ---
                            if download_url_suffix:
                                st.markdown("---")
                                st.markdown(
                                    f'### 📥 Download Processed File <a href="{full_download_url}" download>Click Here to Download</a>',
                                    unsafe_allow_html=True
                                )
                            else:
                                st.warning("Processed file download link was not generated. Check backend logs.")

                        except (requests.JSONDecodeError, KeyError) as e:
                            st.error(f"❌ Failed to parse JSON response from backend. Error: {e}")
                        
                    else:
                        # 5. Handle Backend Error
                        try:
                            error_detail = response.json().get("detail", "Unknown error occurred during check execution.")
                        except requests.JSONDecodeError:
                            error_detail = response.text
                        st.error(f"❌ Backend Processing Error ({response.status_code}): {error_detail}")

                except requests.exceptions.RequestException as e:
                    st.error(f"❌ Connection Error: Could not reach the backend. Error: {e}")

with epl_tab:
    # Use st.title, st.header, or st.markdown for clear visual separation
    st.header("EPL Specific Checks")
    
    # Display the "Work in Progress" message
    st.info("⚠️ **Work in Progress:** This tab is currently under development. Please check back later for the available checks and automation features for the English Premier League.")
    
    # You can optionally add a placeholder or a brief roadmap
    st.subheader("Expected Features:")
    st.markdown("""
    * Verification of market types specific to the EPL (e.g., specific outrights).
    * Check for correct team names and player mappings.
    * Automated checks for specific data fields.
    """)
    
    st.markdown("---")
    st.markdown("Thank you for your patience!")