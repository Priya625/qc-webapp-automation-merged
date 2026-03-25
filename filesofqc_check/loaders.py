import pandas as pd
import re
import os
from .common import DATE_FORMAT

def detect_period_from_rosco(rosco_path):
    df = pd.read_excel(rosco_path, header=None)
    label_col = df.iloc[:, 1].astype(str)
    period_row_mask = label_col.str.contains(
        "monitoring period", case=False, na=False
    )
    if not period_row_mask.any():
        raise ValueError("Missing monitoring period label in Column B of Rosco")
    row_idx = period_row_mask.idxmax()
    if df.shape[1] <= 2:
        raise ValueError(f"Missing monitoring period, please fill cell C{row_idx + 1} of Rosco")
    user_input_text = str(df.iloc[row_idx, 2]).strip()
    if not user_input_text or user_input_text.lower() == "nan":
        raise ValueError(f"Missing monitoring period in cell C{row_idx + 1} of Rosco")
    found = re.findall(r"\d{4}-\d{2}-\d{2}", user_input_text)
    if len(found) < 2:
        raise ValueError(f"Invalid date format in cell C{row_idx + 1}. Expected two dates (YYYY-MM-DD).")
    start_date = pd.to_datetime(found[0], format=DATE_FORMAT).date()
    end_date   = pd.to_datetime(found[1], format=DATE_FORMAT).date()
    if start_date > end_date:
        raise ValueError(f"Invalid monitoring period in cell C{row_idx + 1}: start date is after end date")
    return start_date, end_date

def detect_header_row_in_sheet(bsr_path, sheet_name):
    df_sample = pd.read_excel(
        bsr_path,
        sheet_name=sheet_name,
        header=None,
        nrows=200
    )
    for i, row in df_sample.iterrows():
        row_str = " ".join(row.dropna().astype(str)).lower()
        if "region" in row_str and "market" in row_str and "broadcaster" in row_str:
            return i
        if "date" in row_str and ("utc" in row_str or "gmt" in row_str):
            return i
    raise ValueError(f"Header not found in sheet '{sheet_name}'")

def load_bsr(bsr_path):
    xl = pd.ExcelFile(bsr_path)
    allowed_sheets = {"worksheet", "database"}
    target_sheet = None
    for sheet in xl.sheet_names:
        if sheet.strip().lower() in allowed_sheets:
            target_sheet = sheet
            break
    if not target_sheet:
        raise ValueError(f"No valid sheet ('Worksheet' or 'Database') found in {os.path.basename(bsr_path)}")
    header_row = detect_header_row_in_sheet(bsr_path, target_sheet)
    df = pd.read_excel(bsr_path, sheet_name=target_sheet, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    return df