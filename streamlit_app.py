# streamlit_app.py (corrected)
import streamlit as st
import pandas as pd
import os
import time
import json
from typing import Optional, List

BACKEND_BASE_URL = os.environ.get("STREAMLIT_BACKEND_URL", "http://localhost:8000")
BACKEND_URL = BACKEND_BASE_URL + "/api"

# --- Import QC modules ---
try:
    import qc_checks as qc_general
    from C_data_processing_f1 import BSRValidator
    from C_data_processing_EPL import EPLValidator
except ImportError as e:
    st.error(f"Failed to import QC modules: {e}")
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
            return json.load(f)
    except Exception as e:
        st.error(f"FATAL ERROR: Could not load config.json. {e}")
        return None

config = load_config()
if config is None:
    st.stop()

# convenience
file_rules = config.get("file_rules", {})
col_map_all = config.get("column_mappings", {})
qc_rules = config.get("qc_rules", {})
project_rules = config.get("project_rules", {})

# -------------------- 🌐 Streamlit UI --------------------
LOGO_PATH_4 = "images/Nielsen_Sports_logo.svg"
st.set_page_config(page_title="NIELSEN QC Automation Portal", layout="wide")

try:
    if os.path.exists(LOGO_PATH_4):
        st.image(LOGO_PATH_4, width=150)
    else:
        st.header(" ")
except Exception:
    st.header(" ")

# Tabs
home_page_tab, main_qc_tab, laliga_qc_tab, f1_tab, epl_tab = st.tabs([
    " Home Page",
    " Main QC Automation",
    " Laliga Specific QC",
    " F1 Market Specific Checks",
    " EPL Specific Checks"
])

# --- Market check keys (same as your config)
all_market_check_keys = {
    "check_latam_espn": "LATAM ESPN Channels: Ecuador and Venezuela missing",
    "check_italy_mexico": "Italy and Mexico: Duplications/consolidations",
    "check_channel4plus1": "Specific Channel Checks: Channel 4+1",
    "check_espn4_bsa": "ESPN 4: Latam channel extract from BSA",
    "check_f1_obligations": "Formula 1 Obligations: Missing channels",
    "apply_duplication_weights": "Apply Market Duplication and Upweight Rules (Germany, SA, UK, Brazil, etc.)",
    "check_session_completeness": "Session Count Check: Flag duplicate/over-reported Qualifying, Race, or Training sessions",
    "impute_program_type": "Impute Program Type: Assign Live/Repeat/Highlights/Support based on time matching",
    "duration_limits": "Duration Limits Check: Flag broadcasts outside 5 minutes to 5 hours (QC)",
    "live_date_integrity": "Live Session Date Integrity: Check Live Race/Quali/Train against fixed schedule date",
    "update_audience_from_overnight": "Audience Upscale Check: Update BSR with higher Max Overnight data",
    "dup_channel_existence": "Duplication Channel Existence: Check if all target channels are in BSR",
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
    "remove_andorra": "Remove Andorra",
    "remove_serbia": "Remove Serbia",
    "remove_montenegro": "Remove Montenegro",
    "remove_brazil_espn_fox": "Remove any ESPN/Fox from Brazil",
    "remove_switz_canal": "Remove Switzerland Canal+ / ServusTV",
    "remove_viaplay_baltics": "Remove viaplay from Latvia, Lithuania, Poland, and Estonia",
    "recreate_viaplay": "Viaplay: Recreate based on a full market of lives",
    "recreate_disney_latam": "Disney+ Latam: Recreate based on a full market of lives",
}

all_market_check_keys_epl = {
    "impute_lt_live_status": "L/T Live Imputation: Flag program type based on 'L/T' keyword in Combined col",
    "consolidate_gillete_soccer": "Program Consolidation: Flag sequential 'Gillete Soccer' programs for merging (Gap <= 30min)",
    "check_sky_showcase_live": "Sky Showcase Live Status Check (UK)",
    "standardize_uk_ire_region": "Region Standardization: Correct UK/Ireland Region field to 'Europe' and standardize market names",
    "check_fixture_vs_case": "Checks for Capital VS AND Small vs",
    "check_pan_balkans_serbia_parity": "Checks equal count in pan_balkans and serbia",
    "audit_multi_match_status": "Checking for these keywords 'GOAL RUSH', 'KONFERENZ', 'CONFERENCE'",
    "check_date_time_format_integrity": "Checking Time Integrity",
    "check_live_broadcast_uniqueness": "Checking 1 live for based on these col 'Market', 'TV-Channel', 'Competition', 'Date'",
    "audit_channel_line_item_count": "Channel line item count (New Tab)",
    "check_combined_archive_status": "Flag any row with archive in Combined column",
    "suppress_duplicated_audience": "Flag if it is a Duplicated Market and has audience ",
    "filter_short_programs": "5 Minute Program Filter: Remove programs shorter than 5 minutes (except Austria/NZ)",
    "sa_nielsen_inclusion_check": "South Africa Nielsen Inclusion Check"
}

# ---------- HOME PAGE ----------
with home_page_tab:
    st.markdown("<div style='text-align:center; padding: 20px 0;'><h1>Nielsen Automation Portal</h1></div>", unsafe_allow_html=True)
    st.markdown("Central hub for data integrity, transformation, and market modelling for Sports BSR data.")

# -----------------------------------------------------------
#        ✅ MAIN QC AUTOMATION TAB
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
        if not main_rosco_file or not main_bsr_file:
            st.error("⚠️ Please upload both Rosco and BSR files.")
        else:
            with st.spinner("Running General QC checks..."):
                try:
                    col_map = col_map_all
                    rules = qc_rules
                    file_rules_local = file_rules

                    # save uploaded files
                    rosco_path = os.path.join(UPLOAD_FOLDER, main_rosco_file.name)
                    bsr_path = os.path.join(UPLOAD_FOLDER, main_bsr_file.name)
                    with open(rosco_path, "wb") as f:
                        f.write(main_rosco_file.getbuffer())
                    with open(bsr_path, "wb") as f:
                        f.write(main_bsr_file.getbuffer())

                    # Run QC pipeline
                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])

                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules["program_category"])
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules.get("overlap_check", {}))
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules_local)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules_local)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules_local)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])

                    # Normalize OK columns for coloring and summary
                    df = qc_general.normalize_ok_columns(df)

                    # Output file
                    output_file = f"{file_rules_local.get('output_prefix','QC_Result_')}General_QC_Result_{os.path.splitext(main_bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name=file_rules_local.get("output_sheet_name", "QC Results"))

                    # Apply coloring and summary
                    try:
                        qc_general.color_excel(output_path, df)
                    except Exception as e:
                        st.warning(f"Coloring step failed: {e}")
                    try:
                        qc_general.generate_summary_sheet(output_path, df)
                    except Exception as e:
                        st.warning(f"Summary sheet generation failed: {e}")

                    st.success("✅ General QC completed successfully!")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download General QC Result",
                            data=f,
                            file_name=os.path.basename(output_path),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f" An error occurred during General QC: {e}")

# -----------------------------------------------------------
#         ⚽ LALIGA QC TAB
# -----------------------------------------------------------
with laliga_qc_tab:
    st.header("⚽ Laliga Specific QC Checks")
    st.markdown("Upload your **Rosco**, **BSR**, and **Macro Duplicator** files. This will run Laliga QC checks.")

    col1, col2, col3 = st.columns(3)
    with col1:
        laliga_rosco_file = st.file_uploader("📘 Upload Rosco File (.xlsx)", type=["xlsx"], key="laliga_rosco")
    with col2:
        laliga_bsr_file = st.file_uploader("📗 Upload BSR File (.xlsx)", type=["xlsx"], key="laliga_bsr")
    with col3:
        laliga_macro_file = st.file_uploader("📒 Upload Macro Duplicator File", type=["xlsx","xls","xlsm","xlsb"], key="laliga_macro")

    st.write("---")

    if st.button("⚙️ Run Laliga QC Checks"):
        if not laliga_rosco_file or not laliga_bsr_file or not laliga_macro_file:
            st.error("⚠️ Please upload Rosco, BSR and Macro files.")
        else:
            with st.spinner("Running Laliga QC checks..."):
                try:
                    col_map = col_map_all
                    rules = qc_rules
                    project = project_rules
                    file_rules_local = file_rules

                    # save files
                    rosco_path = os.path.join(UPLOAD_FOLDER, laliga_rosco_file.name)
                    bsr_path = os.path.join(UPLOAD_FOLDER, laliga_bsr_file.name)
                    macro_path = os.path.join(UPLOAD_FOLDER, laliga_macro_file.name)
                    with open(rosco_path, "wb") as f: f.write(laliga_rosco_file.getbuffer())
                    with open(bsr_path, "wb") as f: f.write(laliga_bsr_file.getbuffer())
                    with open(macro_path, "wb") as f: f.write(laliga_macro_file.getbuffer())

                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])

                    # general checks
                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules["program_category"])
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"], rules.get("overlap_check", {}))
                    df = qc_general.program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules_local)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules_local)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules_local)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])

                    # Laliga-specific
                    df = qc_general.domestic_market_check(df, col_map["bsr"], project.get("monitoring_start_date"), debug=True)
                    df = qc_general.duplicated_market_check(df, macro_path, project, col_map, file_rules_local, debug=True)

                    # normalize & save
                    df = qc_general.normalize_ok_columns(df)
                    output_file = f"{file_rules_local.get('output_prefix','QC_Result_')}Laliga_QC_Result_{os.path.splitext(laliga_bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="Laliga QC Results")

                    try:
                        qc_general.color_excel(output_path, df)
                    except Exception:
                        pass
                    try:
                        qc_general.generate_summary_sheet(output_path, df)
                    except Exception:
                        pass

                    st.success("✅ Laliga QC completed successfully!")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download Laliga QC Result",
                            data=f,
                            file_name=os.path.basename(output_path),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                except Exception as e:
                    st.error(f"❌ An error occurred during Laliga QC: {e}")

# -----------------------------------------------------------
#         🏎️ F1 MARKET SPECIFIC CHECKS TAB
# -----------------------------------------------------------
with f1_tab:
    st.header("🌍 Market Specific Checks & Channel Configuration")
    st.markdown("Upload the **BSR file** and F1-supporting files here to perform market checks.")

    col_file1, col_file2, col_file3, col_file4 = st.columns(4)
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
        st.checkbox(all_market_check_keys["check_session_completeness"], key="check_session_completeness")
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
        active_checks = [k for k in all_market_check_keys.keys() if st.session_state.get(k)]
        if f1_bsr_file is None:
            st.error("⚠️ Please upload a BSR file before applying checks.")
        elif "check_f1_obligations" in active_checks and f1_obligation_file is None:
            st.error("⚠️ F1 Obligation Check Selected: Please upload the obligation file.")
        elif "update_audience_from_overnight" in active_checks and f1_overnight_file is None:
            st.error("⚠️ Audience Upscale Check Selected: Please upload the Overnight Audience File.")
        elif "dup_channel_existence" in active_checks and f1_macro_file is None:
            st.error("⚠️ Duplication Channel Existence Check Selected: Please upload the Macro file.")
        else:
            with st.spinner(f"Applying {len(active_checks)} checks..."):
                try:
                    bsr_file_path = os.path.join(UPLOAD_FOLDER, f1_bsr_file.name)
                    with open(bsr_file_path, "wb") as f:
                        f.write(f1_bsr_file.getbuffer())

                    obligation_path = None
                    if f1_obligation_file:
                        obligation_path = os.path.join(UPLOAD_FOLDER, f1_obligation_file.name)
                        with open(obligation_path, "wb") as f:
                            f.write(f1_obligation_file.getbuffer())

                    overnight_path = None
                    if f1_overnight_file:
                        overnight_path = os.path.join(UPLOAD_FOLDER, f1_overnight_file.name)
                        with open(overnight_path, "wb") as f:
                            f.write(f1_overnight_file.getbuffer())

                    macro_path = None
                    if f1_macro_file:
                        macro_path = os.path.join(UPLOAD_FOLDER, f1_macro_file.name)
                        with open(macro_path, "wb") as f:
                            f.write(f1_macro_file.getbuffer())

                    validator = BSRValidator(
                        bsr_path=bsr_file_path,
                        obligation_path=obligation_path,
                        overnight_path=overnight_path,
                        macro_path=macro_path
                    )

                    status_summaries = validator.market_check_processor(active_checks)
                    df_processed = getattr(validator, "df", pd.DataFrame())

                    # normalize and save
                    df_processed = qc_general.normalize_ok_columns(df_processed)
                    output_filename = f"Processed_BSR_{os.path.splitext(f1_bsr_file.name)[0]}_{int(time.time())}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
                    df_processed.to_excel(output_path, index=False)

                    st.success("✅ F1 checks completed successfully!")
                    if status_summaries:
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
                        st.info("No operational summaries returned.")

                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download Processed F1 File",
                            data=f,
                            file_name=os.path.basename(output_path),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                except Exception as e:
                    st.error(f"❌ An error occurred during F1 checks: {e}")

# -----------------------------------------------------------
#         EPL TAB
# -----------------------------------------------------------
with epl_tab:
    st.header("EPL Specific Checks")
    st.markdown("Upload the required files here to perform and log manual checks.")

    col_file1, col_file2, col_file3, col_file4 = st.columns(4)
    with col_file1:
        epl_bsr_file = st.file_uploader("📥 Upload BSR File for Checks (.xlsx)", type=["xlsx"], key="epl_market_check_file")
    with col_file2:
        epl_obligation_file = st.file_uploader("📄 Upload Obligation File (.xlsx)", type=["xlsx"], key="epl_obligation_file")
    with col_file3:
        epl_overnight_file = st.file_uploader("📈 Upload Overnight Audience File (.xlsx)", type=["xlsx"], key="epl_overnight_file")
    with col_file4:
        epl_macro_file = st.file_uploader("📋 BSA Duplicator File", type=["xlsm", "xlsx"], key="epl_macro_file")

    st.write("---")

    for key in all_market_check_keys_epl.keys():
        if key not in st.session_state:
            st.session_state[key] = False

    with st.expander("1. Channel and Territory Review", expanded=True):
        st.subheader("General Market Checks")
        for k, label in all_market_check_keys_epl.items():
            st.checkbox(label, key=k)

    st.write("---")

    if st.button("EPL Apply Selected Checks"):
        active_checks = [k for k in all_market_check_keys_epl.keys() if st.session_state.get(k)]
        if epl_bsr_file is None:
            st.error("⚠️ Please upload a BSR file before applying checks.")
        else:
            with st.spinner(f"Applying {len(active_checks)} checks..."):
                try:
                    bsr_file_path = os.path.join(UPLOAD_FOLDER, epl_bsr_file.name)
                    with open(bsr_file_path, "wb") as f:
                        f.write(epl_bsr_file.getbuffer())

                    obligation_path = None
                    if epl_obligation_file:
                        obligation_path = os.path.join(UPLOAD_FOLDER, epl_obligation_file.name)
                        with open(obligation_path, "wb") as f:
                            f.write(epl_obligation_file.getbuffer())

                    overnight_path = None
                    if epl_overnight_file:
                        overnight_path = os.path.join(UPLOAD_FOLDER, epl_overnight_file.name)
                        with open(overnight_path, "wb") as f:
                            f.write(epl_overnight_file.getbuffer())

                    macro_path = None
                    if epl_macro_file:
                        macro_path = os.path.join(UPLOAD_FOLDER, epl_macro_file.name)
                        with open(macro_path, "wb") as f:
                            f.write(epl_macro_file.getbuffer())

                    # quick load check
                    try:
                        _ = pd.read_excel(bsr_file_path, nrows=3)
                    except Exception as e:
                        st.error(f"❌ Error loading BSR file: {e}")
                        raise

                    validator = EPLValidator(
                        bsr_path=bsr_file_path,
                        obligation_path=obligation_path,
                        overnight_path=overnight_path,
                        macro_path=macro_path
                    )

                    status_summaries = validator.market_check_processor(active_checks)
                    df_processed = getattr(validator, "df", pd.DataFrame())

                    # normalize and save
                    df_processed = qc_general.normalize_ok_columns(df_processed)
                    output_filename = f"Processed_BSR_{os.path.splitext(epl_bsr_file.name)[0]}_{int(time.time())}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_filename)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df_processed.to_excel(writer, index=False, sheet_name="EPL_Processed")
                        if hasattr(validator, "short_programs_df"):
                            sp = validator.short_programs_df
                            if isinstance(sp, pd.DataFrame) and not sp.empty:
                                sp.to_excel(writer, index=False, sheet_name="<5 min-Short Programs")
                        if hasattr(validator, "sa_nielsen_df"):
                            sa = validator.sa_nielsen_df
                            if isinstance(sa, pd.DataFrame) and not sa.empty:
                                sa.to_excel(writer, index=False, sheet_name="SA_Nielsen")

                    st.success("✅ EPL checks completed successfully!")

                    if status_summaries:
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
                        st.info("No operational summaries returned.")

                    with open(output_path, "rb") as f:
                        st.download_button(
                            label="📥 Download Processed EPL File",
                            data=f,
                            file_name=os.path.basename(output_path),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                except Exception as e:
                    st.error(f"❌ An error occurred during EPL checks: {e}")