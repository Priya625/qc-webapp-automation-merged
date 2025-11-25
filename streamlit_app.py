###############################################
#  STREAMLIT QC AUTOMATION PORTAL (Option A)
#  FULL LOCAL EXECUTION (NO API CALLS)
#  MIRRORS API.PY EXACTLY FOR:
#     1) General QC
#     2) Laliga QC
#     3) F1 Market Checks
#     4) EPL Pre/Post Checks
###############################################

import os
import json
import shutil
import time
from typing import Optional, List

import pandas as pd
import streamlit as st

# -------------------- IMPORT QC MODULES --------------------

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

# -------------------- Folder Setup --------------------

BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# -------------------- Load config.json --------------------

@st.cache_data
def load_config():
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Fatal Error: Could not load config.json → {e}")
        return None

config = load_config()
if config is None:
    st.stop()


# -------------------- Streamlit UI --------------------

st.set_page_config(page_title="Nielsen QC Automation Portal", layout="wide")

LOGO_PATH = "images/Nielsen_Sports_logo.svg"
try:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, width=140)
    else:
        st.title("Nielsen QC Automation Portal")
except:
    st.title("Nielsen QC Automation Portal")

# Tabs
home_tab, general_tab, laliga_tab, f1_tab, epl_tab = st.tabs([
    "🏠 Home",
    "🧪 General QC (9-Checks)",
    "⚽ LaLiga QC (11-Checks)",
    "🏎️ F1 Market Checks",
    "🏴 EPL Checks"
])


###########################################################
#                    HOME TAB
###########################################################
with home_tab:
    st.header("Welcome to the Nielsen Sports QC Automation Portal")
    st.markdown("""
    This tool runs **all QC pipelines locally**, exactly as in your backend `api.py`.
    - No API calls  
    - Same logic  
    - Same outputs  
    - Faster execution  
    """)


###########################################################
#                    GENERAL QC (9 CHECKS)
###########################################################
with general_tab:
    st.header("🧪 General QC Automation (9 Checks)")

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

                with open(rosco_path, "wb") as f: f.write(rosco_file.getbuffer())
                with open(bsr_path, "wb") as f: f.write(bsr_file.getbuffer())

                try:
                    # ---- EXACT LOGIC FROM api.py/run_general_qc ----
                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])

                    # Cleaning (same as api.py)
                    df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
                    df = df.applymap(lambda x: str(x).replace("\xa0", " ").strip()
                                     if isinstance(x, str) else x)
                    df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

                    # Execution order EXACT AS BACKEND:
                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules)
                    df = qc_general.program_category_check(bsr_path, df, col_map,
                                                          rules.get("program_category", {}), file_rules)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
                    df = qc_general.domestic_market_check(df, project_rules, col_map["bsr"], debug=False)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])
                    df = qc_general.client_lstv_ott_check(df, col_map["bsr"], rules.get("client_check", {}))
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])  # backend does this 2x

                    # Duplicated Market BEFORE overlap/daybreak
                    df = qc_general.duplicated_market_check(
                        df, None, project_rules, col_map, file_rules, debug=False)

                    df = qc_general.overlap_duplicate_daybreak_check(
                        df, col_map["bsr"], rules.get("overlap_check", {})
                    )

                    # Output
                    output_file = f"General_QC_Result_{os.path.splitext(bsr_file.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False,
                                    sheet_name=file_rules.get("output_sheet_name", "QC Results"))

                    # Additional formatting
                    try:
                        qc_general.color_excel(output_path, df)
                        qc_general.generate_summary_sheet(output_path, df, file_rules)
                    except Exception:
                        pass

                    st.success("General QC Completed Successfully")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            "Download General QC Result", f, file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                except Exception as e:
                    st.error(f"Error during General QC: {e}")


###########################################################
#                    LALIGA QC (11 CHECKS)
###########################################################
with laliga_tab:
    st.header("⚽ LaLiga QC Automation (11 Checks)")

    c1, c2, c3 = st.columns(3)
    with c1:
        ll_rosco = st.file_uploader("Upload Rosco (.xlsx)", type=["xlsx"], key="ll_rosco")
    with c2:
        ll_bsr = st.file_uploader("Upload BSR (.xlsx)", type=["xlsx"], key="ll_bsr")
    with c3:
        ll_macro = st.file_uploader("Upload Macro Duplicator", type=["xlsx", "xlsm"], key="ll_macro")

    if st.button("▶ Run LaLiga QC"):
        if not ll_rosco or not ll_bsr or not ll_macro:
            st.error("Please upload Rosco, BSR & Macro files.")
        else:
            with st.spinner("Running LaLiga QC..."):

                # Load config
                col_map = config["column_mappings"]
                rules = config["qc_rules"]
                project = config["project_rules"]
                file_rules = config["file_rules"]

                # Save files
                rosco_path = os.path.join(UPLOAD_FOLDER, ll_rosco.name)
                bsr_path = os.path.join(UPLOAD_FOLDER, ll_bsr.name)
                macro_path = os.path.join(UPLOAD_FOLDER, ll_macro.name)

                for f, p in [(ll_rosco, rosco_path), (ll_bsr, bsr_path), (ll_macro, macro_path)]:
                    with open(p, "wb") as fh: fh.write(f.getbuffer())

                try:
                    # ---- EXACT LOGIC FROM api.py/run_laliga_qc ----

                    start_date, end_date = qc_general.detect_period_from_rosco(rosco_path)
                    df = qc_general.load_bsr(bsr_path, col_map["bsr"])

                    df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
                    df = df.applymap(lambda x: str(x).replace("\xa0", " ").strip()
                                     if isinstance(x, str) else x)
                    df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

                    df = qc_general.period_check(df, start_date, end_date, col_map["bsr"])
                    df = qc_general.completeness_check(df, col_map["bsr"], rules)
                    df = qc_general.overlap_duplicate_daybreak_check(df, col_map["bsr"],
                                                                     rules.get("overlap_check", {}))
                    df = qc_general.program_category_check(bsr_path, df, col_map,
                                                           rules.get("program_category", {}), file_rules)
                    df = qc_general.check_event_matchday_competition(df, bsr_path, col_map, file_rules)
                    df = qc_general.market_channel_consistency_check(df, rosco_path, col_map, file_rules)
                    df = qc_general.rates_and_ratings_check(df, col_map["bsr"])
                    df = qc_general.country_channel_id_check(df, col_map["bsr"])
                    df = qc_general.client_lstv_ott_check(df, col_map["bsr"],
                                                          rules.get("client_check", {}))
                    df = qc_general.domestic_market_check(df, project, col_map["bsr"], debug=False)

                    df = qc_general.duplicated_market_check(df, macro_path, project,
                                                            col_map, file_rules, debug=False)

                    df = qc_general.overlap_duplicate_daybreak_check(
                        df, col_map["bsr"], rules.get("overlap_check", {}))

                    # Output
                    output_file = f"Laliga_QC_Result_{os.path.splitext(ll_bsr.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False,
                                    sheet_name=file_rules.get("output_sheet_name", "Laliga QC Results"))

                    try:
                        qc_general.color_excel(output_path, df)
                        qc_general.generate_summary_sheet(output_path, df, file_rules)
                    except:
                        pass

                    st.success("LaLiga QC Completed Successfully")
                    with open(output_path, "rb") as f:
                        st.download_button(
                            "Download Laliga QC Result", f, file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

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
        f1_overnight = st.file_uploader("Overnight File (.xlsx)", type=["xlsx"], key="f1_on")
    with c4:
        f1_macro = st.file_uploader("Macro File (.xlsx)", type=["xlsx"], key="f1_macro")

    # simple check list
    possible_checks = [
        "check_f1_obligations",
        "check_session_completeness",
        "duration_limits",
        "live_date_integrity",
        "update_audience_from_overnight",
        "dup_channel_existence"
    ]

    st.subheader("Select Checks:")
    selected = [c for c in possible_checks if st.checkbox(c)]

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

                    if df_processed.empty:
                        st.error("Processed DataFrame is empty.")
                        st.stop()

                    output_file = f"F1_Processed_{os.path.splitext(bsr_f1.name)[0]}.xlsx"
                    output_path = os.path.join(OUTPUT_FOLDER, output_file)

                    df_processed.to_excel(output_path, index=False)

                    st.success("F1 Checks Completed Successfully")
                    if summaries:
                        st.subheader("Check Summaries")
                        st.dataframe(pd.DataFrame(summaries))

                    with open(output_path, "rb") as f:
                        st.download_button(
                            "Download F1 Processed File", f, file_name=output_file,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

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
                    with open(p, "wb") as fh: fh.write(f.getbuffer())

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
                    with open(p, "wb") as fh: fh.write(f.getbuffer())

                try:
                    df = epl_checks.run_post_checks(bsr_path, rosco_path, macro_path)
                    out_file = "EPL_Post_Checks.xlsx"
                    out_path = os.path.join(OUTPUT_FOLDER, out_file)
                    df.to_excel(out_path, index=False)

                    st.success("EPL Post Checks Completed")
                    with open(out_path, "rb") as f:
                        st.download_button("Download EPL Post Check File", f, file_name=out_file,
                                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                except Exception as e:
                    st.error(f"EPL Post Check Error: {e}")