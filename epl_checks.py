import pandas as pd

def run_pre_checks(bsr_path, rosco_path, market_dup_path):
    # Read your files
    df_bsr = pd.read_excel(bsr_path)
    df_rosco = pd.read_excel(rosco_path)
    df_market = pd.read_excel(market_dup_path)

    # TODO: Add your EPL pre-check logic
    df_bsr["EPL_PRE_CHECK_FLAG"] = "OK"

    return df_bsr


def run_post_checks(bsr_path, rosco_path, macro_path):
    df_bsr = pd.read_excel(bsr_path)
    df_rosco = pd.read_excel(rosco_path)
    df_macro = pd.read_excel(macro_path)

    # TODO: Add your EPL post-check logic
    df_bsr["EPL_POST_CHECK_FLAG"] = "OK"

    return df_bsr