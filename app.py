import streamlit as st
import pandas as pd
import io
import re
import os
import plotly.express as px
from datetime import datetime, timedelta

# --- CONFIGURATION ---
st.set_page_config(page_title="Early Warning Dashboard", layout="wide")

# --- RELATIVE PATHS (Cloud Friendly) ---
# This looks for files in the 'assets' folder inside your project
AURA_PATH = "assets/List of Channel - AURA.xlsx"
MANDATORY_PATH = "assets/BSA Mandatory Channel List.xlsx"

# --- HELPER FUNCTIONS ---
def parse_custom_date(date_str):
    if isinstance(date_str, datetime): return date_str
    s = str(date_str).strip()
    s_clean = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', s, flags=re.IGNORECASE)
    for fmt in ('%d %b %Y', '%Y-%m-%d', '%d-%m-%Y', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%Y/%m/%d'):
        try: return datetime.strptime(s_clean, fmt)
        except ValueError: continue
    return None

def extract_monitoring_period(df_info):
    monitor_row = df_info[df_info.iloc[:, 0].astype(str).str.contains("Monitoring Periods", na=False)]
    if not monitor_row.empty:
        dates = re.findall(r'\d{4}-\d{2}-\d{2}', str(monitor_row.iloc[0, 1]))
        if len(dates) >= 2:
            return datetime.strptime(dates[0], '%Y-%m-%d'), datetime.strptime(dates[1], '%Y-%m-%d')
    return None, None

def clean_name_strict(name):
    if pd.isna(name): return ""
    s = str(name).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def clean_name_lenient(name):
    if pd.isna(name): return ""
    s = str(name)
    s = re.sub(r"\(.*?\)|\[.*?\]", "", s)
    s = re.split(r"[-–—]", s)[0]
    s = re.sub(r"[^0-9a-zA-Z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def clean_market(m): 
    if pd.isna(m): return ""
    return str(m).strip().lower()

def clean_id(val): 
    if pd.isna(val): return ""
    s = str(val).strip()
    return s[:-2] if s.endswith(".0") else s

def style_dataframe(df):
    def color_cells(val):
        s = str(val).strip()
        if s == "Not in BSA": return 'font-style: italic; color: #F35390; background-color: white'
        elif s == "FLAG: Not in BSA": return 'font-style: italic; color: white; background-color: #F35390'
        elif "Processing Gaps" in s or "Processing/Gaps" in s: return 'color: white; background-color: #8F2E07'
        elif "Not in Aura" in s or "Missing in Both" in s: return 'color: white; background-color: #990033'
        elif "No Schedule" in s: return 'background-color: #FFC000; color: black'
        elif "Partial Schedule" in s: return 'color: white; background-color: #0F39B1'
        elif "Scheduled" in s or "OK" == s: return 'background-color: #22786A; color: white'
        elif "CRITICAL" == s: return 'color: white; background-color: #15A60A; font-weight: bold'
        elif "Non-Critical" == s: return 'color: white; background-color: #941C8E'
        return ''
    return df.style.map(color_cells)

def smart_multiselect(label, options, key, default=None):
    all_opt = ["Select All"] + sorted([str(o) for o in options if pd.notna(o)])
    if key not in st.session_state:
        st.session_state[key] = default if default else ["Select All"]
    if f"reset_{key}" in st.session_state and st.session_state[f"reset_{key}"]:
        st.session_state[key] = ["Select All"]
        st.session_state[f"reset_{key}"] = False
    selection = st.multiselect(label, all_opt, key=key)
    if "Select All" in selection: return [o for o in all_opt if o != "Select All"]
    return selection

# --- UI HEADER ---
st.title("📺 BSA/ROSCO/AURA - Early Warning Dashboard")

# --- SIDEBAR: AUTO-LOADER ---
with st.sidebar:
    st.header("📂 Data Sources")
    
    # 1. Aura Master Auto-Load
    aura_file_obj = None
    if os.path.exists(AURA_PATH):
        st.success("AURA Master: Loaded from Repo ✅")
        aura_file_obj = AURA_PATH
    else:
        st.warning("⚠️ 'assets/List of Channel - AURA.xlsx' not found.")
        aura_file_obj = st.file_uploader("Upload AURA Master", type=['xlsx'], key="aura_up")

    # 2. Mandatory List Auto-Load
    mandatory_file_obj = None
    if os.path.exists(MANDATORY_PATH):
        st.success("Mandatory List: Loaded from Repo ✅")
        mandatory_file_obj = MANDATORY_PATH
    else:
        st.warning("⚠️ 'assets/BSA Mandatory Channel List.xlsx' not found.")
        mandatory_file_obj = st.file_uploader("Upload Mandatory List", type=['xlsx'], key="mand_up")

st.divider()

# --- MAIN UPLOADER AREA ---
i_c1, i_c2 = st.columns(2)
with i_c1:
    st.caption("Consolidated BSA File (Required)")
    bsa_file = st.file_uploader("Upload BSA", type=['xlsx'], key="b_up", label_visibility="collapsed")
with i_c2:
    st.caption("Rosco File (Optional for Comparison)")
    rosco_file = st.file_uploader("Upload Rosco", type=['xlsx'], key="r_up", label_visibility="collapsed")

st.divider()

if bsa_file:
    if not aura_file_obj:
        st.error("Please ensure AURA Master file is present in 'assets' folder or uploaded.")
        st.stop()
    
    try:
        # Load BSA Data
        df_bsa_raw = pd.read_excel(bsa_file)
        
        bsa_chan_c = next(c for c in df_bsa_raw.columns if "channel" in str(c).lower() and "id" not in str(c).lower())
        bsa_mkt_c = next(c for c in df_bsa_raw.columns if "market" in str(c).lower())
        bsa_id_c = next((c for c in df_bsa_raw.columns if "channel" in str(c).lower() and "id" in str(c).lower()), None)
        
        # DEDUPLICATION
        df_bsa_raw.drop_duplicates(subset=[bsa_mkt_c, bsa_chan_c], inplace=True)

        # Load Mandatory List
        mandatory_set = set()
        if mandatory_file_obj:
            df_m_list = pd.read_excel(mandatory_file_obj, sheet_name="BSA_Channel_List")
            mandatory_set = set(df_m_list['Channel Name'].apply(clean_name_strict))

        # Date Columns
        bsa_date_cols = [col for col in df_bsa_raw.columns if parse_custom_date(col) is not None]
        bsa_dates_sorted = sorted(bsa_date_cols, key=lambda x: parse_custom_date(x))
        default_start = parse_custom_date(bsa_dates_sorted[0]) if bsa_dates_sorted else datetime.now()
        default_end = parse_custom_date(bsa_dates_sorted[-1]) if bsa_dates_sorted else datetime.now()
        
        tab1, tab2, tab3, tab4 = st.tabs(["📊 BSA Consolidated View", "📉 Trend Tracker", "🛡️ Mandatory Audit", "📋 Rosco Comparison View"])

        # --- TAB 1: BSA CONSOLIDATED VIEW ---
        with tab1:
            st.write("### Consolidated BSA Status")
            results_bsa = []
            for _, row in df_bsa_raw.iterrows():
                cn = str(row[bsa_chan_c])
                mkt = str(row[bsa_mkt_c])
                cid = str(row[bsa_id_c]) if bsa_id_c else ""
                
                is_crit = "CRITICAL" if clean_name_strict(cn) in mandatory_set else "Non-Critical"
                
                row_statuses = [str(row[d]).lower() for d in bsa_date_cols]
                if any("processing gaps" in s for s in row_statuses): final_s = "FLAG: Processing Gaps"
                elif all("no schedule" in s for s in row_statuses) and row_statuses: final_s = "FLAG: No Schedule"
                elif any("no schedule" in s for s in row_statuses): final_s = "FLAG: Partial Schedule"
                else: final_s = "OK"
                
                r_data = {"TV Channel": cn, "Market": mkt, "Channel ID": cid, "Critical Channel": is_crit, "Final Status": final_s}
                for d in bsa_date_cols: r_data[d] = row[d]
                results_bsa.append(r_data)
            
            df_bsa_view = pd.DataFrame(results_bsa)
            
            if st.button("🔄 Reset BSA Filters"):
                for k in ["b_mkt", "b_chan", "b_crit", "b_stat"]: st.session_state[f"reset_{k}"] = True
                st.session_state["b_crit"] = ["CRITICAL"]
                st.rerun()

            with st.expander("Filter Panel", expanded=True):
                b1, b2, b3, b4 = st.columns(4)
                with b1: f_mkt = smart_multiselect("Market", df_bsa_view['Market'].unique(), "b_mkt")
                with b2: f_chan = smart_multiselect("Channel", df_bsa_view['TV Channel'].unique(), "b_chan")
                with b3: f_crit = smart_multiselect("Critical?", df_bsa_view['Critical Channel'].unique(), "b_crit", default=["CRITICAL"])
                with b4: f_stat = smart_multiselect("Status", df_bsa_view['Final Status'].unique(), "b_stat")
                d1, d2 = st.columns(2)
                b_start = d1.date_input("Start Date", value=default_start, key="b_start")
                b_end = d2.date_input("End Date", value=default_end, key="b_end")

            if f_mkt: df_bsa_view = df_bsa_view[df_bsa_view['Market'].isin(f_mkt)]
            if f_chan: df_bsa_view = df_bsa_view[df_bsa_view['TV Channel'].isin(f_chan)]
            if f_crit: df_bsa_view = df_bsa_view[df_bsa_view['Critical Channel'].isin(f_crit)]
            if f_stat: df_bsa_view = df_bsa_view[df_bsa_view['Final Status'].isin(f_stat)]

            active_bsa_dates = [d for d in bsa_date_cols if b_start <= parse_custom_date(d).date() <= b_end]
            cols_to_show = ["TV Channel", "Market", "Channel ID", "Critical Channel", "Final Status"] + active_bsa_dates

            if not df_bsa_view.empty:
                df_bsa_view.index = range(1, len(df_bsa_view) + 1)
                st.divider()
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("TOTAL CHANNELS", len(df_bsa_view))
                m2.metric("PROCESSING GAPS", len(df_bsa_view[df_bsa_view['Final Status'].str.contains("Processing Gaps", na=False)]))
                m3.metric("NO SCHEDULE", len(df_bsa_view[df_bsa_view['Final Status'].str.contains("No Schedule", na=False)]))
                m4.metric("SCHEDULED (OK)", len(df_bsa_view[df_bsa_view['Final Status'] == "OK"]))
                st.divider()
                st.dataframe(style_dataframe(df_bsa_view[cols_to_show]), width='stretch')

        # --- TAB 2: TREND TRACKER ---
        with tab2:
            st.write("### Daily Status Trends (BSA Data)")
            chart_records = []
            for d_col in active_bsa_dates:
                ds = parse_custom_date(d_col).strftime('%d %b')
                day_data = df_bsa_view[d_col].astype(str).str.lower()
                counts = {
                    "Scheduled": len(day_data[day_data.str.contains("scheduled", na=False)]),
                    "Processing Gaps": len(day_data[day_data.str.contains("processing gaps", na=False)]),
                    "No Schedule": len(day_data[day_data.str.contains("no schedule", na=False)])
                }
                for stat, val in counts.items():
                    if val > 0: chart_records.append({"Date": ds, "Status": stat, "Count": val})
            df_chart = pd.DataFrame(chart_records)
            if not df_chart.empty:
                c_map = {"Scheduled": "#22786A", "Processing Gaps": "#8F2E07", "No Schedule": "#FFC000"}
                fig = px.bar(df_chart, x="Date", y="Count", color="Status", color_discrete_map=c_map, barmode="group")
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No data available for the selected range.")

        # --- TAB 3: MANDATORY AUDIT ---
        with tab3:
            st.write("### Mandatory Channel Check")
            df_bsa_raw['SearchKey'] = df_bsa_raw[bsa_chan_c].astype(str).apply(clean_name_strict)
            bsa_lookup = set(df_bsa_raw['SearchKey'])
            
            if mandatory_file_obj:
                df_m_list = pd.read_excel(mandatory_file_obj, sheet_name="BSA_Channel_List")
                res_m = []
                for _, row in df_m_list.iterrows():
                    cn = str(row['Channel Name'])
                    is_found = clean_name_strict(cn) in bsa_lookup
                    res_m.append({"Channel": cn, "Found in BSA?": is_found, "Status": "OK" if is_found else "MISSING"})
                df_m_view = pd.DataFrame(res_m)
                df_m_view.index = range(1, len(df_m_view) + 1)
                st.dataframe(style_dataframe(df_m_view), width='stretch')
            else:
                st.warning("Mandatory List not available.")

        # --- TAB 4: ROSCO COMPARISON VIEW ---
        with tab4:
            if rosco_file:
                xls_rosco = pd.ExcelFile(rosco_file)
                df_info = pd.read_excel(xls_rosco, sheet_name=next(s for s in xls_rosco.sheet_names if "Info" in s))
                start_scope, end_scope = extract_monitoring_period(df_info)
                df_rosco_sheet = pd.read_excel(xls_rosco, sheet_name=next(s for s in xls_rosco.sheet_names if "Monitoring" in s))
                
                df_aura_raw = pd.read_excel(aura_file_obj)
                aura_mkt_col = next(c for c in df_aura_raw.columns if "market" in str(c).lower())
                aura_name_col = next(c for c in df_aura_raw.columns if "channel" in str(c).lower() and "id" not in str(c).lower())
                aura_id_col = next(c for c in df_aura_raw.columns if "channel" in str(c).lower() and "id" in str(c).lower())
                aura_set = set()
                aura_id_map = {}
                for _, r in df_aura_raw.iterrows():
                    k = (clean_market(r[aura_mkt_col]), clean_name_lenient(r[aura_name_col]))
                    aura_set.add(k)
                    aura_id_map[k] = clean_id(r.get(aura_id_col, ""))

                df_bsa_raw['MarketNameKey'] = df_bsa_raw[bsa_mkt_c].astype(str).apply(clean_market) + "|" + df_bsa_raw[bsa_chan_c].astype(str).apply(clean_name_lenient)
                bsa_region_lookup = df_bsa_raw.drop_duplicates(subset=['MarketNameKey']).set_index('MarketNameKey').to_dict('index')
                
                bsa_id_lookup = {}
                if bsa_id_c:
                    temp_id = df_bsa_raw.dropna(subset=[bsa_id_c]).drop_duplicates(subset=[bsa_id_c])
                    bsa_id_lookup = temp_id.set_index(temp_id[bsa_id_c].apply(clean_id)).to_dict('index')

                matching_dates = [c for c in bsa_date_cols if start_scope <= parse_custom_date(c) <= end_scope]

                results_rosco = []
                for _, row in df_rosco_sheet.iterrows():
                    cn, ct = str(row['ChannelName']), str(row['ChannelCountry'])
                    cl, ml = clean_name_lenient(cn), clean_market(ct)
                    aid = aura_id_map.get((ml, cl), "")
                    in_aura = (ml, cl) in aura_set
                    
                    fnd, brow = False, None
                    if aid and aid in bsa_id_lookup: fnd, brow = True, bsa_id_lookup[aid]
                    elif f"{ml}|{cl}" in bsa_region_lookup: fnd, brow = True, bsa_region_lookup[f"{ml}|{cl}"]

                    r_r = {"Channel": cn, "Market": ct, "IN AURA": in_aura, "IN BSA": fnd}
                    for d in matching_dates:
                        r_r[d] = str(brow.get(d, "Not in BSA")).strip() if fnd else "Not in BSA"
                    
                    if not fnd: final_s = "CRITICAL: Missing in Both" if not in_aura else "FLAG: Not in BSA"
                    else:
                        statuses = [str(r_r[d]).lower() for d in matching_dates]
                        if statuses.count("no schedule") == len(matching_dates): final_s = "FLAG: Found (No Schedules)"
                        elif any("processing gaps" in s for s in statuses): final_s = "FLAG: Processing Gaps"
                        elif not in_aura: final_s = "CRITICAL: Not in Aura"
                        else: final_s = "OK"
                    r_r["Final Status"] = final_s
                    results_rosco.append(r_r)
                
                df_rosco_final = pd.DataFrame(results_rosco)
                
                st.write("### Rosco Comparison & Trends")
                if st.button("🔄 Reset Rosco Filters"): 
                    for k in ["r_mkt", "r_chan"]: st.session_state[f"reset_{k}"] = True
                    st.rerun()
                
                with st.expander("Rosco Filters", expanded=True):
                    rf1, rf2 = st.columns(2)
                    with rf1: r_mkt = smart_multiselect("Market", df_rosco_final['Market'].unique(), "r_mkt")
                    with rf2: r_chan = smart_multiselect("Channel", df_rosco_final['Channel'].unique(), "r_chan")
                    rd1, rd2 = st.columns(2)
                    r_start = rd1.date_input("Start", value=start_scope.date(), key="r_start")
                    r_end = rd2.date_input("End", value=end_scope.date(), key="r_end")

                df_r_view = df_rosco_final.copy()
                if r_mkt: df_r_view = df_r_view[df_r_view['Market'].isin(r_mkt)]
                if r_chan: df_r_view = df_r_view[df_r_view['Channel'].isin(r_chan)]
                
                active_r_dates = [c for c in matching_dates if r_start <= parse_custom_date(c).date() <= r_end]
                
                if not df_r_view.empty:
                    df_r_view.index = range(1, len(df_r_view) + 1)
                    st.divider()
                    rm1, rm2, rm3, rm4 = st.columns(4)
                    rm1.metric("ROSCO CHANNELS", len(df_r_view))
                    rm2.metric("NOT IN BOTH", len(df_r_view[df_r_view['Final Status'].str.contains("Missing in Both")]))
                    rm3.metric("IN BSA", len(df_r_view[df_r_view['IN BSA'] == True]))
                    rm4.metric("IN AURA", len(df_r_view[df_r_view['IN AURA'] == True]))
                    st.divider()
                    st.dataframe(style_dataframe(df_r_view[["Channel", "Market", "IN AURA", "IN BSA"] + active_r_dates + ["Final Status"]]), width='stretch')
                    st.write("#### Daily Trends")
                    chart_records = []
                    for d_col in active_r_dates:
                        ds = parse_custom_date(d_col).strftime('%d %b')
                        day_data = df_r_view[d_col].str.lower()
                        counts = {
                            "Scheduled": len(day_data[~day_data.str.contains("no schedule|processing gaps|not in bsa", na=False)]),
                            "Processing Gaps": len(day_data[day_data.str.contains("processing gaps", na=False)]),
                            "No Schedule": len(day_data[day_data.str.contains("no schedule", na=False)]),
                            "Not in BSA": len(day_data[day_data.str.contains("not in bsa", na=False)])
                        }
                        for stat, val in counts.items():
                            if val > 0: chart_records.append({"Date": ds, "Status": stat, "Count": val})
                    df_chart = pd.DataFrame(chart_records)
                    if not df_chart.empty:
                        c_map = {"Scheduled": "#22786A", "Processing Gaps": "#8F2E07", "No Schedule": "#FFC000", "Not in BSA": "#F35390"}
                        fig = px.bar(df_chart, x="Date", y="Count", color="Status", color_discrete_map=c_map, barmode="group")
                        st.plotly_chart(fig, width='stretch')
            else:
                st.warning("⚠️ Upload a Rosco file to enable this comparison view.")
    except Exception as e: st.error(f"Logic Error: {e}")