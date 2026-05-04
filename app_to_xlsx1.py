import csv
import re
import io
import bcrypt
import pandas as pd
import streamlit as st
import xlsxwriter
from datetime import date, datetime
from supabase import create_client, Client

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

TRAILING_COLS        = 12
COORD_RE             = re.compile(r'^-?\d{1,3}\.\d{4,}$')
CHUNK_SIZE           = 100_000
SMALL_FILE_THRESHOLD = 50 * 1024 * 1024  # 50MB — below this, no chunking needed

OUTPUT_COLS = [
    'Cabinet', 'NAP ID', 'Discovered When', 'PLA ID', 'Tech',
    'Ports Assigned', 'Ports Reserved', 'Ports Total', 'UTILIZATION',
    'Latitude', 'Longitude',
    'SALES_AREA', 'TERRITORY', 'BRGY_NAME', 'CITY_NAME',
    'PROVINCE_NAME', 'LOCATION TAGGING',
]

YELLOW_COLS = {
    'Cabinet', 'NAP ID', 'Discovered When',
    'Ports Assigned', 'Ports Reserved', 'Ports Total',
    'UTILIZATION', 'Latitude', 'Longitude',
}
GREEN_COLS = {
    'SALES_AREA', 'TERRITORY', 'BRGY_NAME',
    'CITY_NAME', 'PROVINCE_NAME', 'LOCATION TAGGING',
}

# ─────────────────────────────────────────────────────────────────────────────
# CABINET TECH LOOKUP
# ─────────────────────────────────────────────────────────────────────────────

CABINET_TECH_LOOKUP = {
    'BGN_001_GPONA_01': 'GPON',
    'BNG_001_GPONA_01': 'GPON',
    'BNY_001_GPONA_01': 'GPON',
    'CMN_001_GPONA_01': 'GPON',
    'CMP_001_GPONA_01': 'GPON',
    'CRA_001_GPONA_01': 'GPON',
    'CRN_001_GPONA_01': 'GPON',
    'CRN_002_GPONA_01': 'GPON',
    'CRN_003_GPONA_01': 'GPON',
    'CRN_004_GPONA_01': 'GPON',
    'CTL_001_GPONA_01': 'GPON',
    'CTT_001_GPONA_01': 'GPON',
    'CTT_002_GPONA_01': 'GPON',
    'CTT_002_GPONA_02': 'GPON',
    'CTT_003_GPONA_01': 'GPON',
    'CTT_004_GPONA_01': 'GPON',
    'CTT_005_GPONA_01': 'GPON',
    'CTT_006_GPONA_01': 'GPON',
    'CTT_007_GPONA_01': 'GPON',
    'DIG701-M': 'ADSL/VDSL',
    'DIG702-M': 'VDSL',
    'DIG703-M': 'VDSL',
    'DIG704-M': 'ADSL/VDSL',
    'DIG705-M': 'VDSL',
    'DIG_001_GPONA_01': 'GPON',
    'DIG_001_GPONA_02': 'GPON',
    'DIG_002_GPONA_01': 'GPON',
    'DIG_003_GPONA_01': 'GPON',
    'DIG_004_GPONA_01': 'GPON',
    'DIG_004_GPONA_02': 'GPON',
    'DIG_005_GPONA_01': 'GPON',
    'DIG_007_GPONA_01': 'GPON',
    'DIG_008_GPONA_01': 'GPON',
    'DIG_008_GPONA_02': 'GPON',
    'DIG_009_GPONA_01': 'GPON',
    'DIG_701_GPONA_01': 'GPON',
    'DIG_701_HYB_01': 'GPON',
    'DIG_702_GPONA_01': 'GPON',
    'DIG_702_GPONA_02': 'GPON',
    'DIG_704_GPONA_01': 'GPON',
    'DIG_704_HYB_01': 'GPON',
    'DIG_705_HYB_01': 'GPON',
    'DONMAR-LSA01-001-MIN1804': 'GPON',
    'DOS_001_GPONA_01': 'GPON',
    'DOS_001_GPONA_02': 'GPON',
    'DOS_002_GPONA_01': 'GPON',
    'DVO009-M': 'VDSL',
    'DVO01M01-M': 'VDSL',
    'DVO102-M': 'VDSL',
    'DVO600-M': 'ADSL/VDSL',
    'DVO601-M': 'ADSL',
    'DVO603-M': 'ADSL',
    'DVO607-M': 'ADSL/VDSL',
    'DVO608-M': 'ADSL/VDSL',
    'DVO615-M': 'VDSL',
    'DVO702-M': 'VDSL',
    'DVO704-M': 'ADSL',
    'DVO706-M': 'ADSL/VDSL',
    'DVO708-M': 'ADSL/VDSL',
    'DVO709-M': 'ADSL/VDSL',
    'DVO710-M': 'VDSL',
    'DVO711-M': 'VDSL',
    'DVO712-M': 'ADSL',
    'DVO717-M': 'VDSL',
    'DVO728-M': 'ADSL',
    'DVO729-M': 'ADSL',
    'DVO733-M': 'VDSL',
    'DVO736-M': 'VDSL',
    'DVO738-M': 'ADSL',
    'DVO739-M': 'ADSL/VDSL',
    'DVO740-M': 'VDSL',
    'DVO741-M': 'VDSL',
    'DVO742-M': 'VDSL',
    'DVO745-M': 'VDSL',
    'DVO746-M': 'VDSL',
    'DVO748-M': 'VDSL',
    'DVO749-M': 'VDSL',
    'DVO_001_GPONA_02': 'GPON',
    'DVO_002_GPONA_01': 'GPON',
    'DVO_002_GPONA_02': 'GPON',
    'DVO_002_GPONA_03': 'GPON',
    'DVO_003_GPONA_01': 'GPON',
    'DVO_003_GPONA_02': 'GPON',
    'DVO_005_GPONA_01': 'GPON',
    'DVO_005_GPONA_02': 'GPON',
    'DVO_006_GPONA_01': 'GPON',
    'DVO_007_GPONA_01': 'GPON',
    'DVO_007_GPONA_02': 'GPON',
    'DVO_008_GPONA_01': 'GPON',
    'DVO_009_GPONA_01': 'GPON',
    'DVO_010_GPONA_01': 'GPON',
    'DVO_011_GPONA_01': 'GPON',
    'DVO_011_GPONA_02': 'GPON',
    'DVO_013_GPONA_01': 'GPON',
    'DVO_014_GPONA_01': 'GPON',
    'DVO_014_GPONA_02': 'GPON',
    'DVO_016_GPONA_01': 'GPON',
    'DVO_016_GPONA_02': 'GPON',
    'DVO_018_GPONA_01': 'GPON',
    'DVO_019_GPONA_01': 'GPON',
    'DVO_020_GPONA_01': 'GPON',
    'DVO_021_GPONA_01': 'GPON',
    'DVO_022_GPONA_01': 'GPON',
    'DVO_023_GPONA_01': 'GPON',
    'DVO_024_GPONA_01': 'GPON',
    'DVO_024_GPONA_02': 'GPON',
    'DVO_025_GPONA_01': 'GPON',
    'DVO_026_GPONA_01': 'GPON',
    'DVO_027_GPONA_01': 'GPON',
    'DVO_028_GPONA_01': 'GPON',
    'DVO_028_GPONA_02': 'GPON',
    'DVO_029_GPONA_01': 'GPON',
    'DVO_030_GPONA_01': 'GPON',
    'DVO_031_GPONA_01': 'GPON',
    'DVO_032_GPONA_01': 'GPON',
    'DVO_032_GPONA_02': 'GPON',
    'DVO_033_GPONA_01': 'GPON',
    'DVO_034_GPONA_01': 'GPON',
    'DVO_035_GPONA_01': 'GPON',
    'DVO_036_GPONA_01': 'GPON',
    'DVO_038_GPONA_01': 'GPON',
    'DVO_040_GPONA_01': 'GPON',
    'DVO_041_GPONA_01': 'GPON',
    'DVO_047_GPONA_01': 'GPON',
    'DVO_048_GPONA_01': 'GPON',
    'DVO_049_GPONA_01': 'GPON',
    'DVO_049_GPONA_02': 'GPON',
    'DVO_051_GPONA_01': 'GPON',
    'DVO_052_GPONA_01': 'GPON',
    'DVO_055_GPONA_01': 'GPON',
    'DVO_055_GPONA_02': 'GPON',
    'DVO_060_GPONA_01': 'GPON',
    'DVO_061_GPONA_01': 'GPON',
    'DVO_061_GPONA_02': 'GPON',
    'DVO_061_GPONA_03': 'GPON',
    'DVO_062_GPONA_01': 'GPON',
    'DVO_063_GPONA_01': 'GPON',
    'DVO_064_GPONA_01': 'GPON',
    'DVO_065_GPONA_01': 'GPON',
    'DVO_067_GPONA_01': 'GPON',
    'DVO_068_GPONA_01': 'GPON',
    'DVO_068_GPONA_02': 'GPON',
    'DVO_069_GPONA_01': 'GPON',
    'DVO_070_GPONA_01': 'GPON',
    'DVO_072_GPONA_01': 'GPON',
    'DVO_072_GPONA_02': 'GPON',
    'DVO_074_GPONA_01': 'GPON',
    'DVO_075_GPONA_01': 'GPON',
    'DVO_076_GPONA_01': 'GPON',
    'DVO_077_GPONA_01': 'GPON',
    'DVO_078_GPONA_01': 'GPON',
    'DVO_079_GPONA_01': 'GPON',
    'DVO_080_GPONA_01': 'GPON',
    'DVO_081_GPONA_01': 'GPON',
    'DVO_082_GPONA_01': 'GPON',
    'DVO_083_GPONA_01': 'GPON',
    'DVO_085_GPONA_01': 'GPON',
    'DVO_086_GPONA_01': 'GPON',
    'DVO_088_GPONA_01': 'GPON',
    'DVO_089_GPONA_01': 'GPON',
    'DVO_090_GPONA_01': 'GPON',
    'DVO_091_GPONA_01': 'GPON',
    'DVO_092_GPONA_01': 'GPON',
    'DVO_093_GPONA_01': 'GPON',
    'DVO_094_GPONA_01': 'GPON',
    'DVO_095_GPONA_01': 'GPON',
    'DVO_096_GPONA_01': 'GPON',
    'DVO_105_GPONA_01': 'GPON',
    'DVO_608_GPONA_01': 'GPON',
    'DVO_708_GPONA_01': 'GPON',
    'DVO_733_GPONA_01': 'GPON',
    'DVO_733_GPONA_02': 'GPON',
    'DVO_741_HYB_01': 'GPON',
    'DVO_742_HYB_01': 'GPON',
    'DVO_745_GPONA_01': 'GPON',
    'DVO_745_HYB_01': 'GPON',
    'DVO_910_GPONA_01': 'GPON',
    'DVO_918_GPONA_01': 'GPON',
    'ESR_001_GPONA_01': 'GPON',
    'ESR_001_GPONA_02': 'GPON',
    'ESR_002_GPONA_01': 'GPON',
    'ESR_002_GPONA_02': 'GPON',
    'GSN602-M': 'VDSL',
    'GSN604-M': 'VDSL',
    'GSN605-M': 'ADSL/VDSL',
    'GSN705-M': 'VDSL',
    'GSN706-M': 'ADSL/VDSL',
    'GSN707-M': 'VDSL',
    'GSN_001_GPONA_01': 'GPON',
    'GSN_002_GPONA_01': 'GPON',
    'GSN_002_GPONA_02': 'GPON',
    'GSN_003_GPONA_01': 'GPON',
    'GSN_003_GPONA_02': 'GPON',
    'GSN_004_GPONA_01': 'GPON',
    'GSN_004_GPONA_02': 'GPON',
    'GSN_005_GPONA_01': 'GPON',
    'GSN_005_GPONA_02': 'GPON',
    'GSN_006_GPONA_01': 'GPON',
    'GSN_006_GPONA_02': 'GPON',
    'GSN_008_GPONA_01': 'GPON',
    'GSN_009_GPONA_01': 'GPON',
    'GSN_010_GPONA_01': 'GPON',
    'GSN_012_GPONA_01': 'GPON',
    'GSN_014_GPONA_01': 'GPON',
    'GSN_015_GPONA_01': 'GPON',
    'GSN_016_GPONA_01': 'GPON',
    'GSN_016_GPONA_02': 'GPON',
    'GSN_017_GPONA_01': 'GPON',
    'GSN_019_GPONA_01': 'GPON',
    'GSN_602_GPONA_01': 'GPON',
    'GSN_602_HYB_01': 'GPON',
    'GSN_604_GPONA_01': 'GPON',
    'GSN_604_HYB_01': 'GPON',
    'GSN_910_GPONA_01': 'GPON',
    'GSN_911_GPONA_01': 'GPON',
    'GSN_911_GPONA_02': 'GPON',
    'GUL_001_GPONA_01': 'GPON',
    'ISU_001_GPONA_01': 'GPON',
    'KBC_001_GPONA_01': 'GPON',
    'KBC_001_GPONA_02': 'GPON',
    'KPW_001_GPONA_01': 'GPON',
    'KRN702-M': 'ADSL',
    'KRN_001_GPONA_01': 'GPON',
    'KRN_002_GPONA_01': 'GPON',
    'KRN_003_GPONA_01': 'GPON',
    'LBU_001_GPONA_01': 'GPON',
    'LPN_001_GPONA_01': 'GPON',
    'MAI_001_GPONA_01': 'GPON',
    'MAT_001_GPONA_01': 'GPON',
    'MCO_001_GPONA_01': 'GPON',
    'MDS_001_GPONA_01': 'GPON',
    'MDS_003_GPONA_01': 'GPON',
    'MLN_001_GPONA_01': 'GPON',
    'MLN_001_GPONA_02': 'GPON',
    'MLU_001_GPONA_01': 'GPON',
    'MLU_002_GPONA_01': 'GPON',
    'MNK_001_GPONA_01': 'GPON',
    'MON_001_GPONA_01': 'GPON',
    'MRU_001_GPONA_01': 'GPON',
    'MTI701-M': 'ADSL/VDSL',
    'MTI702-M': 'VDSL',
    'MTI_001_GPONA_01': 'GPON',
    'MTI_001_GPONA_02': 'GPON',
    'MTI_002_GPONA_01': 'GPON',
    'MTI_003_GPONA_01': 'GPON',
    'MTI_005_GPONA_01': 'GPON',
    'MTI_006_GPONA_01': 'GPON',
    'MTI_701_GPONA_01': 'GPON',
    'MTI_702_GPONA_01': 'GPON',
    'MTL_001_GPONA_01': 'GPON',
    'NBN_001_GPONA_01': 'GPON',
    'NBN_001_GPONA_02': 'GPON',
    'PANABO-LSA01-001-MIN856': 'GPON',
    'PDA_001_GPONA_01': 'GPON',
    'PGK_001_GPONA_01': 'GPON',
    'PGK_002_GPONA_01': 'GPON',
    'PIK_001_GPONA_01': 'GPON',
    'PLM701-M': 'VDSL',
    'PLM702-M': 'VDSL',
    'PLM_001_GPONA_01': 'GPON',
    'PLM_002_GPONA_01': 'GPON',
    'PLM_003_GPONA_01': 'GPON',
    'PLM_004_GPONA_01': 'GPON',
    'PLM_005_GPONA_01': 'GPON',
    'PLM_702_HYB_01': 'GPON',
    'PNB702-M': 'VDSL',
    'PNB703-M': 'VDSL',
    'PNB_001_GPONA_01': 'GPON',
    'PNB_001_GPONA_02': 'GPON',
    'PNB_002_GPONA_01': 'GPON',
    'PNB_003_GPONA_01': 'GPON',
    'PNB_004_GPONA_01': 'GPON',
    'PNB_005_GPONA_01': 'GPON',
    'PNB_006_GPONA_01': 'GPON',
    'PNB_007_GPONA_01': 'GPON',
    'PNB_701_GPONA_01': 'GPON',
    'PNB_702_GPONA_01': 'GPON',
    'PNB_703_HYB_01': 'GPON',
    'PNT_001_GPONA_01': 'GPON',
    'PRN_001_GPONA_01': 'GPON',
    'PRN_002_GPONA_01': 'GPON',
    'SCD_001_GPONA_01': 'GPON',
    'SCD_002_GPONA_01': 'GPON',
    'SCD_003_GPONA_01': 'GPON',
    'SCD_004_GPONA_01': 'GPON',
    'SFA_001_GPONA_01': 'GPON',
    'STM_001_GPONA_01': 'GPON',
    'TAN_001_GPONA_01': 'GPON',
    'TCR_002_GPONA_01': 'GPON',
    'TCR_002_GPONA_02': 'GPON',
    'TCR_002_GPONA_03': 'GPON',
    'TGM702-M': 'ADSL/VDSL',
    'TGM704-M': 'VDSL',
    'TGM705-M': 'VDSL',
    'TGM_001_GPONA_01': 'GPON',
    'TGM_002_GPONA_01': 'GPON',
    'TGM_002_GPONA_02': 'GPON',
    'TGM_003_GPONA_01': 'GPON',
    'TGM_003_GPONA_02': 'GPON',
    'TGM_004_GPONA_01': 'GPON',
    'TGM_005_GPONA_01': 'GPON',
    'TGM_006_GPONA_01': 'GPON',
    'TGM_006_GPONA_02': 'GPON',
    'TGM_007_GPONA_01': 'GPON',
    'TGM_007_GPONA_02': 'GPON',
    'TGM_008_GPONA_01': 'GPON',
    'TGM_009_GPONA_01': 'GPON',
    'TGM_010_GPONA_01': 'GPON',
    'TGM_011_GPONA_01': 'GPON',
    'TGM_011_GPONA_02': 'GPON',
    'TGM_012_GPONA_01': 'GPON',
    'TGM_014_GPONA_01': 'GPON',
    'TGM_014_GPONA_02': 'GPON',
    'TGM_017_GPONA_01': 'GPON',
    'TGM_702_GPONA_01': 'GPON',
    'TGM_704_GPONA_01': 'GPON',
    'TGM_705_GPONA_01': 'GPON',
    'TUL_001_GPONA_01': 'GPON',
    'TUP_001_GPONA_01': 'GPON',
}

# ─────────────────────────────────────────────────────────────────────────────
# PLA ID LOOKUP
# ─────────────────────────────────────────────────────────────────────────────

PLA_ID_LOOKUP = {
    'BGN_001': 'MIN210',
    'BGN_001_GPONA_01': 'MIN210',
    'BNG_001': 'MIN754',
    'BNG_001_GPONA_01': 'MIN754',
    'BNY_001': 'MIN212',
    'BNY_001_GPONA_01': 'MIN212',
    'CMN_001': 'MIN380',
    'CMN_001_GPONA_01': 'MIN380',
    'CMP_001': 'MIN816',
    'CMP_001_GPONA_01': 'MIN816',
    'CRA_001': 'MIN5',
    'CRA_001_GPONA_01': 'MIN5',
    'CRN_001': 'MIN131',
    'CRN_001_GPONA_01': 'MIN131',
    'CRN_002': 'MIN136',
    'CRN_002_GPONA_01': 'MIN136',
    'CRN_003': 'MIN227',
    'CRN_003_GPONA_01': 'MIN227',
    'CRN_004': 'MIN3167',
    'CRN_004_GPONA_01': 'MIN3167',
    'CTL_001': 'MIN215',
    'CTL_001_GPONA_01': 'MIN215',
    'CTT_001': 'MIN1761',
    'CTT_001_GPONA_01': 'MIN1761',
    'CTT_002': 'MIN269',
    'CTT_002_GPONA_01': 'MIN269',
    'CTT_002_GPONA_02': 'MIN269',
    'CTT_003': 'MIN288',
    'CTT_003_GPONA_01': 'MIN288',
    'CTT_004': 'MIN859',
    'CTT_004_GPONA_01': 'MIN859',
    'CTT_005': 'MIN283',
    'CTT_005_GPONA_01': 'MIN283',
    'CTT_006': 'MIN1529',
    'CTT_006_GPONA_01': 'MIN1529',
    'CTT_007': 'MIN1530',
    'CTT_007_GPONA_01': 'MIN1530',
    'DIG701': 'MIN1160',
    'DIG701-M': 'MIN1160',
    'DIG702': 'MIN1161',
    'DIG702-M': 'MIN1161',
    'DIG703': 'MIN1162',
    'DIG703-M': 'MIN1162',
    'DIG704': 'MIN1163',
    'DIG704-M': 'MIN1163',
    'DIG705': 'MIN1164',
    'DIG705-M': 'MIN1164',
    'DIG_001': 'MIN820',
    'DIG_001_GPONA_01': 'MIN820',
    'DIG_001_GPONA_02': 'MIN820',
    'DIG_002': 'MIN1611',
    'DIG_002_GPONA_01': 'MIN1611',
    'DIG_003': 'MIN1492',
    'DIG_003_GPONA_01': 'MIN1492',
    'DIG_004': 'MIN156',
    'DIG_004_GPONA_01': 'MIN156',
    'DIG_004_GPONA_02': 'MIN156',
    'DIG_005': 'MIN188',
    'DIG_005_GPONA_01': 'MIN188',
    'DIG_007': 'MIN1582',
    'DIG_007_GPONA_01': 'MIN1582',
    'DIG_008': 'MIN1152',
    'DIG_008_GPONA_01': 'MIN1152',
    'DIG_008_GPONA_02': 'MIN1152',
    'DIG_009': 'MIN1081',
    'DIG_009_GPONA_01': 'MIN1081',
    'DIG_701': 'MIN1160',
    'DIG_701_GPONA_01': 'MIN1160',
    'DIG_701_HYB_01': 'MIN1160',
    'DIG_702': 'MIN1161',
    'DIG_702_GPONA_01': 'MIN1161',
    'DIG_702_GPONA_02': 'MIN1161',
    'DIG_704': 'MIN1163',
    'DIG_704_GPONA_01': 'MIN1163',
    'DIG_704_HYB_01': 'MIN1163',
    'DIG_705': 'MIN1164',
    'DIG_705_HYB_01': 'MIN1164',
    'DONMAR-LSA01-001': 'MIN1804',
    'DONMAR-LSA01-001-MIN1804': 'MIN1804',
    'DOS_001': 'MIN279',
    'DOS_001_GPONA_01': 'MIN279',
    'DOS_001_GPONA_02': 'MIN279',
    'DOS_002': 'MIN282',
    'DOS_002_GPONA_01': 'MIN282',
    'DVO009': 'MIN2532',
    'DVO009-M': 'MIN2532',
    'DVO01M01': 'MIN2009',
    'DVO01M01-M': 'MIN2009',
    'DVO102': 'MIN1377',
    'DVO102-M': 'MIN1377',
    'DVO600': 'MIN832',
    'DVO600-M': 'MIN832',
    'DVO601': 'MIN1015',
    'DVO601-M': 'MIN1015',
    'DVO603': 'MIN1016',
    'DVO603-M': 'MIN1016',
    'DVO607': 'MIN1022',
    'DVO607-M': 'MIN1022',
    'DVO608': 'MIN1023',
    'DVO608-M': 'MIN1023',
    'DVO615': 'MIN1024',
    'DVO615-M': 'MIN1024',
    'DVO702': 'MIN1165',
    'DVO702-M': 'MIN1165',
    'DVO704': 'MIN1166',
    'DVO704-M': 'MIN1166',
    'DVO706': 'MIN1167',
    'DVO706-M': 'MIN1167',
    'DVO708': 'MIN1739',
    'DVO708-M': 'MIN1739',
    'DVO709': 'MIN1275',
    'DVO709-M': 'MIN1275',
    'DVO710': 'MIN1168',
    'DVO710-M': 'MIN1168',
    'DVO711': 'MIN1169',
    'DVO711-M': 'MIN1169',
    'DVO712': 'MIN1276',
    'DVO712-M': 'MIN1276',
    'DVO717': 'MIN1170',
    'DVO717-M': 'MIN1170',
    'DVO728': 'MIN1174',
    'DVO728-M': 'MIN1174',
    'DVO729': 'MIN1173',
    'DVO729-M': 'MIN1173',
    'DVO733': 'MIN1176',
    'DVO733-M': 'MIN1176',
    'DVO736': 'MIN1178',
    'DVO736-M': 'MIN1178',
    'DVO738': 'MIN1179',
    'DVO738-M': 'MIN1179',
    'DVO739': 'MIN1180',
    'DVO739-M': 'MIN1180',
    'DVO740': 'MIN1181',
    'DVO740-M': 'MIN1181',
    'DVO741': 'MIN1182',
    'DVO741-M': 'MIN1182',
    'DVO742': 'MIN1346',
    'DVO742-M': 'MIN1346',
    'DVO745': 'MIN1403',
    'DVO745-M': 'MIN1403',
    'DVO746': 'MIN1444',
    'DVO746-M': 'MIN1444',
    'DVO748': 'MIN2857',
    'DVO748-M': 'MIN2857',
    'DVO749': 'MIN2860',
    'DVO749-M': 'MIN2860',
    'DVO_001': 'MIN2009',
    'DVO_001_GPONA_02': 'MIN2009',
    'DVO_002': 'MIN1053',
    'DVO_002_GPONA_01': 'MIN1053',
    'DVO_002_GPONA_02': 'MIN1053',
    'DVO_002_GPONA_03': 'MIN1053',
    'DVO_003': 'MIN832',
    'DVO_003_GPONA_01': 'MIN832',
    'DVO_003_GPONA_02': 'MIN832',
    'DVO_005': 'MIN691',
    'DVO_005_GPONA_01': 'MIN691',
    'DVO_005_GPONA_02': 'MIN691',
    'DVO_006': 'MIN697',
    'DVO_006_GPONA_01': 'MIN697',
    'DVO_007': 'MIN1110',
    'DVO_007_GPONA_01': 'MIN1110',
    'DVO_007_GPONA_02': 'MIN1110',
    'DVO_008': 'MIN1228',
    'DVO_008_GPONA_01': 'MIN1228',
    'DVO_009': 'MIN2532',
    'DVO_009_GPONA_01': 'MIN2532',
    'DVO_010': 'MIN1061',
    'DVO_010_GPONA_01': 'MIN1061',
    'DVO_011': 'MIN154',
    'DVO_011_GPONA_01': 'MIN154',
    'DVO_011_GPONA_02': 'MIN154',
    'DVO_013': 'MIN4680',
    'DVO_013_GPONA_01': 'MIN4680',
    'DVO_014': 'MIN1057',
    'DVO_014_GPONA_01': 'MIN1057',
    'DVO_014_GPONA_02': 'MIN1057',
    'DVO_016': 'MIN174',
    'DVO_016_GPONA_01': 'MIN174',
    'DVO_016_GPONA_02': 'MIN174',
    'DVO_018': 'MIN1034',
    'DVO_018_GPONA_01': 'MIN1034',
    'DVO_019': 'MIN187',
    'DVO_019_GPONA_01': 'MIN187',
    'DVO_020': 'MIN1100',
    'DVO_020_GPONA_01': 'MIN1100',
    'DVO_021': 'MIN2',
    'DVO_021_GPONA_01': 'MIN2',
    'DVO_022': 'MIN1522',
    'DVO_022_GPONA_01': 'MIN1522',
    'DVO_023': 'MIN1055',
    'DVO_023_GPONA_01': 'MIN1055',
    'DVO_024': 'MIN696',
    'DVO_024_GPONA_01': 'MIN696',
    'DVO_024_GPONA_02': 'MIN696',
    'DVO_025': 'MIN190',
    'DVO_025_GPONA_01': 'MIN190',
    'DVO_026': 'MIN165',
    'DVO_026_GPONA_01': 'MIN165',
    'DVO_027': 'MIN1084',
    'DVO_027_GPONA_01': 'MIN1084',
    'DVO_028': 'MIN1054',
    'DVO_028_GPONA_01': 'MIN1054',
    'DVO_028_GPONA_02': 'MIN1054',
    'DVO_029': 'MIN616',
    'DVO_029_GPONA_01': 'MIN616',
    'DVO_030': 'MIN1428',
    'DVO_030_GPONA_01': 'MIN1428',
    'DVO_031': 'MIN2457',
    'DVO_031_GPONA_01': 'MIN2457',
    'DVO_032': 'MIN2424',
    'DVO_032_GPONA_01': 'MIN2424',
    'DVO_032_GPONA_02': 'MIN2424',
    'DVO_033': 'MIN1140',
    'DVO_033_GPONA_01': 'MIN1140',
    'DVO_034': 'MIN207',
    'DVO_034_GPONA_01': 'MIN207',
    'DVO_035': 'MIN1074',
    'DVO_035_GPONA_01': 'MIN1074',
    'DVO_036': 'MIN2433',
    'DVO_036_GPONA_01': 'MIN2433',
    'DVO_038': 'MIN1114',
    'DVO_038_GPONA_01': 'MIN1114',
    'DVO_040': 'MIN1151',
    'DVO_040_GPONA_01': 'MIN1151',
    'DVO_041': 'MIN1298',
    'DVO_041_GPONA_01': 'MIN1298',
    'DVO_047': 'MIN1388',
    'DVO_047_GPONA_01': 'MIN1388',
    'DVO_048': 'MIN162',
    'DVO_048_GPONA_01': 'MIN162',
    'DVO_049': 'MIN169',
    'DVO_049_GPONA_01': 'MIN169',
    'DVO_049_GPONA_02': 'MIN169',
    'DVO_051': 'MIN1985',
    'DVO_051_GPONA_01': 'MIN1985',
    'DVO_052': 'MIN617',
    'DVO_052_GPONA_01': 'MIN617',
    'DVO_055': 'MIN686',
    'DVO_055_GPONA_01': 'MIN686',
    'DVO_055_GPONA_02': 'MIN686',
    'DVO_060': 'MIN694',
    'DVO_060_GPONA_01': 'MIN694',
    'DVO_061': 'MIN1804',
    'DVO_061_GPONA_01': 'MIN1804',
    'DVO_061_GPONA_02': 'MIN1804',
    'DVO_061_GPONA_03': 'MIN1804',
    'DVO_062': 'MIN3826',
    'DVO_062_GPONA_01': 'MIN3826',
    'DVO_063': 'MIN3292',
    'DVO_063_GPONA_01': 'MIN3292',
    'DVO_064': 'MIN3291',
    'DVO_064_GPONA_01': 'MIN3291',
    'DVO_065': 'MIN168',
    'DVO_065_GPONA_01': 'MIN168',
    'DVO_067': 'MIN1227',
    'DVO_067_GPONA_01': 'MIN1227',
    'DVO_068': 'MIN1208',
    'DVO_068_GPONA_01': 'MIN1208',
    'DVO_068_GPONA_02': 'MIN1208',
    'DVO_069': 'MIN690',
    'DVO_069_GPONA_01': 'MIN690',
    'DVO_070': 'MIN192',
    'DVO_070_GPONA_01': 'MIN192',
    'DVO_072': 'MIN205',
    'DVO_072_GPONA_01': 'MIN205',
    'DVO_072_GPONA_02': 'MIN205',
    'DVO_074': 'MIN604',
    'DVO_074_GPONA_01': 'MIN604',
    'DVO_075': 'MIN191',
    'DVO_075_GPONA_01': 'MIN191',
    'DVO_076': 'MIN1601',
    'DVO_076_GPONA_01': 'MIN1601',
    'DVO_077': 'MIN1698',
    'DVO_077_GPONA_01': 'MIN1698',
    'DVO_078': 'MIN1699',
    'DVO_078_GPONA_01': 'MIN1699',
    'DVO_079': 'MIN1609',
    'DVO_079_GPONA_01': 'MIN1609',
    'DVO_080': 'MIN1603',
    'DVO_080_GPONA_01': 'MIN1603',
    'DVO_081': 'MIN202',
    'DVO_081_GPONA_01': 'MIN202',
    'DVO_082': 'MIN1078',
    'DVO_082_GPONA_01': 'MIN1078',
    'DVO_083': 'MIN619',
    'DVO_083_GPONA_01': 'MIN619',
    'DVO_085': 'MIN2430',
    'DVO_085_GPONA_01': 'MIN2430',
    'DVO_086': 'MIN1108',
    'DVO_086_GPONA_01': 'MIN1108',
    'DVO_088': 'MIN909',
    'DVO_088_GPONA_01': 'MIN909',
    'DVO_089': 'MIN698',
    'DVO_089_GPONA_01': 'MIN698',
    'DVO_090': 'MIN650',
    'DVO_090_GPONA_01': 'MIN650',
    'DVO_091': 'MIN1309',
    'DVO_091_GPONA_01': 'MIN1309',
    'DVO_092': 'MIN1684',
    'DVO_092_GPONA_01': 'MIN1684',
    'DVO_093': 'MIN195',
    'DVO_093_GPONA_01': 'MIN195',
    'DVO_094': 'MIN2449',
    'DVO_094_GPONA_01': 'MIN2449',
    'DVO_095': 'MIN1659',
    'DVO_095_GPONA_01': 'MIN1659',
    'DVO_096': 'MIN2468',
    'DVO_096_GPONA_01': 'MIN2468',
    'DVO_105': 'MIN4725',
    'DVO_105_GPONA_01': 'MIN4725',
    'DVO_608': 'MIN1023',
    'DVO_608_GPONA_01': 'MIN1023',
    'DVO_708': 'MIN1739',
    'DVO_708_GPONA_01': 'MIN1739',
    'DVO_733': 'MIN1176',
    'DVO_733_GPONA_01': 'MIN1176',
    'DVO_733_GPONA_02': 'MIN1176',
    'DVO_741': 'MIN1182',
    'DVO_741_HYB_01': 'MIN1182',
    'DVO_742': 'MIN1346',
    'DVO_742_HYB_01': 'MIN1346',
    'DVO_745': 'MIN1403',
    'DVO_745_GPONA_01': 'MIN1403',
    'DVO_745_HYB_01': 'MIN1403',
    'DVO_910': 'MIN1924',
    'DVO_910_GPONA_01': 'MIN1924',
    'DVO_918': 'MIN1873',
    'DVO_918_GPONA_01': 'MIN1873',
    'ESR_001': 'MIN2234',
    'ESR_001_GPONA_01': 'MIN2234',
    'ESR_001_GPONA_02': 'MIN2234',
    'ESR_002': 'MIN655',
    'ESR_002_GPONA_01': 'MIN655',
    'ESR_002_GPONA_02': 'MIN655',
    'GSN602': 'MIN1035',
    'GSN602-M': 'MIN1035',
    'GSN604': 'MIN1037',
    'GSN604-M': 'MIN1037',
    'GSN605': 'MIN1038',
    'GSN605-M': 'MIN1038',
    'GSN705': 'MIN758',
    'GSN705-M': 'MIN758',
    'GSN706': 'MIN1277',
    'GSN706-M': 'MIN1277',
    'GSN707': 'MIN1278',
    'GSN707-M': 'MIN1278',
    'GSN_001': 'MIN758',
    'GSN_001_GPONA_01': 'MIN758',
    'GSN_002': 'MIN756',
    'GSN_002_GPONA_01': 'MIN756',
    'GSN_002_GPONA_02': 'MIN756',
    'GSN_003': 'MIN759',
    'GSN_003_GPONA_01': 'MIN759',
    'GSN_003_GPONA_02': 'MIN759',
    'GSN_004': 'MIN1548',
    'GSN_004_GPONA_01': 'MIN1548',
    'GSN_004_GPONA_02': 'MIN1548',
    'GSN_005': 'MIN438',
    'GSN_005_GPONA_01': 'MIN438',
    'GSN_005_GPONA_02': 'MIN438',
    'GSN_006': 'MIN783',
    'GSN_006_GPONA_01': 'MIN783',
    'GSN_006_GPONA_02': 'MIN783',
    'GSN_008': 'MIN766',
    'GSN_008_GPONA_01': 'MIN766',
    'GSN_009': 'MIN764',
    'GSN_009_GPONA_01': 'MIN764',
    'GSN_010': 'MIN426',
    'GSN_010_GPONA_01': 'MIN426',
    'GSN_012': 'MIN407',
    'GSN_012_GPONA_01': 'MIN407',
    'GSN_014': 'MIN430',
    'GSN_014_GPONA_01': 'MIN430',
    'GSN_015': 'MIN1547',
    'GSN_015_GPONA_01': 'MIN1547',
    'GSN_016': 'MIN1446',
    'GSN_016_GPONA_01': 'MIN1446',
    'GSN_016_GPONA_02': 'MIN1446',
    'GSN_017': 'MIN423',
    'GSN_017_GPONA_01': 'MIN423',
    'GSN_019': 'MIN428',
    'GSN_019_GPONA_01': 'MIN428',
    'GSN_602': 'MIN1035',
    'GSN_602_GPONA_01': 'MIN1035',
    'GSN_602_HYB_01': 'MIN1035',
    'GSN_604': 'MIN1037',
    'GSN_604_GPONA_01': 'MIN1037',
    'GSN_604_HYB_01': 'MIN1037',
    'GSN_910': 'MIN1853',
    'GSN_910_GPONA_01': 'MIN1853',
    'GSN_911': 'MIN1562',
    'GSN_911_GPONA_01': 'MIN1562',
    'GSN_911_GPONA_02': 'MIN1562',
    'GUL_001': 'MIN2205',
    'GUL_001_GPONA_01': 'MIN2205',
    'ISU_001': 'MIN446',
    'ISU_001_GPONA_01': 'MIN446',
    'KBC_001': 'MIN744',
    'KBC_001_GPONA_01': 'MIN744',
    'KBC_001_GPONA_02': 'MIN744',
    'KPW_001': 'MIN834',
    'KPW_001_GPONA_01': 'MIN834',
    'KRN702': 'MIN1359',
    'KRN702-M': 'MIN1359',
    'KRN_001': 'MIN763',
    'KRN_001_GPONA_01': 'MIN763',
    'KRN_002': 'MIN1070',
    'KRN_002_GPONA_01': 'MIN1070',
    'KRN_003': 'MIN1612',
    'KRN_003_GPONA_01': 'MIN1612',
    'LBU_001': 'MIN383',
    'LBU_001_GPONA_01': 'MIN383',
    'LPN_001': 'MIN699',
    'LPN_001_GPONA_01': 'MIN699',
    'MAI_001': 'MIN1263',
    'MAI_001_GPONA_01': 'MIN1263',
    'MAT_001': 'MIN167',
    'MAT_001_GPONA_01': 'MIN167',
    'MCO_001': 'MIN667',
    'MCO_001_GPONA_01': 'MIN667',
    'MDS_001': 'MIN845',
    'MDS_001_GPONA_01': 'MIN845',
    'MDS_003': 'MIN1299',
    'MDS_003_GPONA_01': 'MIN1299',
    'MLN_001': 'MIN1272',
    'MLN_001_GPONA_01': 'MIN1272',
    'MLN_001_GPONA_02': 'MIN1272',
    'MLU_001': 'MIN397',
    'MLU_001_GPONA_01': 'MIN397',
    'MLU_002': 'MIN873',
    'MLU_002_GPONA_01': 'MIN873',
    'MNK_001': 'MIN669',
    'MNK_001_GPONA_01': 'MIN669',
    'MON_001': 'MIN851',
    'MON_001_GPONA_01': 'MIN851',
    'MRU_001': 'MIN126',
    'MRU_001_GPONA_01': 'MIN126',
    'MTI701': 'MIN1464',
    'MTI701-M': 'MIN1464',
    'MTI702': 'MIN1465',
    'MTI702-M': 'MIN1465',
    'MTI_001': 'MIN216',
    'MTI_001_GPONA_01': 'MIN216',
    'MTI_001_GPONA_02': 'MIN216',
    'MTI_002': 'MIN2269',
    'MTI_002_GPONA_01': 'MIN2269',
    'MTI_003': 'MIN2780',
    'MTI_003_GPONA_01': 'MIN2780',
    'MTI_005': 'MIN238',
    'MTI_005_GPONA_01': 'MIN238',
    'MTI_006': 'MIN1783',
    'MTI_006_GPONA_01': 'MIN1783',
    'MTI_701': 'MIN1464',
    'MTI_701_GPONA_01': 'MIN1464',
    'MTI_702': 'MIN1465',
    'MTI_702_GPONA_01': 'MIN1465',
    'MTL_001': 'MIN745',
    'MTL_001_GPONA_01': 'MIN745',
    'NBN_001': 'MIN670',
    'NBN_001_GPONA_01': 'MIN670',
    'NBN_001_GPONA_02': 'MIN670',
    'PANABO-LSA01-001': 'MIN856',
    'PANABO-LSA01-001-MIN856': 'MIN856',
    'PDA_001': 'MIN689',
    'PDA_001_GPONA_01': 'MIN689',
    'PGK_001': 'MIN390',
    'PGK_001_GPONA_01': 'MIN390',
    'PGK_002': 'MIN377',
    'PGK_002_GPONA_01': 'MIN377',
    'PIK_001': 'MIN857',
    'PIK_001_GPONA_01': 'MIN857',
    'PLM701': 'MIN1375',
    'PLM701-M': 'MIN1375',
    'PLM702': 'MIN1374',
    'PLM702-M': 'MIN1374',
    'PLM_001': 'MIN767',
    'PLM_001_GPONA_01': 'MIN767',
    'PLM_002': 'MIN410',
    'PLM_002_GPONA_01': 'MIN410',
    'PLM_003': 'MIN414',
    'PLM_003_GPONA_01': 'MIN414',
    'PLM_004': 'MIN1843',
    'PLM_004_GPONA_01': 'MIN1843',
    'PLM_005': 'MIN1066',
    'PLM_005_GPONA_01': 'MIN1066',
    'PLM_702': 'MIN1374',
    'PLM_702_HYB_01': 'MIN1374',
    'PNB702': 'MIN1340',
    'PNB702-M': 'MIN1340',
    'PNB703': 'MIN1195',
    'PNB703-M': 'MIN1195',
    'PNB_001': 'MIN229',
    'PNB_001_GPONA_01': 'MIN229',
    'PNB_001_GPONA_02': 'MIN229',
    'PNB_002': 'MIN658',
    'PNB_002_GPONA_01': 'MIN658',
    'PNB_003': 'MIN148',
    'PNB_003_GPONA_01': 'MIN148',
    'PNB_004': 'MIN135',
    'PNB_004_GPONA_01': 'MIN135',
    'PNB_005': 'MIN856',
    'PNB_005_GPONA_01': 'MIN856',
    'PNB_006': 'MIN1211',
    'PNB_006_GPONA_01': 'MIN1211',
    'PNB_007': 'MIN143',
    'PNB_007_GPONA_01': 'MIN143',
    'PNB_701': 'MIN1185',
    'PNB_701_GPONA_01': 'MIN1185',
    'PNB_702': 'MIN1340',
    'PNB_702_GPONA_01': 'MIN1340',
    'PNB_703': 'MIN1195',
    'PNB_703_HYB_01': 'MIN1195',
    'PNT_001': 'MIN672',
    'PNT_001_GPONA_01': 'MIN672',
    'PRN_001': 'MIN711',
    'PRN_001_GPONA_01': 'MIN711',
    'PRN_002': 'MIN710',
    'PRN_002_GPONA_01': 'MIN710',
    'SCD_001': 'MIN861',
    'SCD_001_GPONA_01': 'MIN861',
    'SCD_002': 'MIN176',
    'SCD_002_GPONA_01': 'MIN176',
    'SCD_003': 'MIN679',
    'SCD_003_GPONA_01': 'MIN679',
    'SCD_004': 'MIN817',
    'SCD_004_GPONA_01': 'MIN817',
    'SFA_001': 'MIN286',
    'SFA_001_GPONA_01': 'MIN286',
    'STM_001': 'MIN676',
    'STM_001_GPONA_01': 'MIN676',
    'TAN_001': 'MIN437',
    'TAN_001_GPONA_01': 'MIN437',
    'TCR_002': 'MIN862',
    'TCR_002_GPONA_01': 'MIN862',
    'TCR_002_GPONA_02': 'MIN862',
    'TCR_002_GPONA_03': 'MIN862',
    'TGM702': 'MIN1344',
    'TGM702-M': 'MIN1344',
    'TGM704': 'MIN1341',
    'TGM704-M': 'MIN1341',
    'TGM705': 'MIN1284',
    'TGM705-M': 'MIN1284',
    'TGM_001': 'MIN1721',
    'TGM_001_GPONA_01': 'MIN1721',
    'TGM_002': 'MIN1538',
    'TGM_002_GPONA_01': 'MIN1538',
    'TGM_002_GPONA_02': 'MIN1538',
    'TGM_003': 'MIN1226',
    'TGM_003_GPONA_01': 'MIN1226',
    'TGM_003_GPONA_02': 'MIN1226',
    'TGM_004': 'MIN1763',
    'TGM_004_GPONA_01': 'MIN1763',
    'TGM_005': 'MIN677',
    'TGM_005_GPONA_01': 'MIN677',
    'TGM_006': 'MIN228',
    'TGM_006_GPONA_01': 'MIN228',
    'TGM_006_GPONA_02': 'MIN228',
    'TGM_007': 'MIN1780',
    'TGM_007_GPONA_01': 'MIN1780',
    'TGM_007_GPONA_02': 'MIN1780',
    'TGM_008': 'MIN2484',
    'TGM_008_GPONA_01': 'MIN2484',
    'TGM_009': 'MIN149',
    'TGM_009_GPONA_01': 'MIN149',
    'TGM_010': 'MIN138',
    'TGM_010_GPONA_01': 'MIN138',
    'TGM_011': 'MIN142',
    'TGM_011_GPONA_01': 'MIN142',
    'TGM_011_GPONA_02': 'MIN142',
    'TGM_012': 'MIN222',
    'TGM_012_GPONA_01': 'MIN222',
    'TGM_014': 'MIN1652',
    'TGM_014_GPONA_01': 'MIN1652',
    'TGM_014_GPONA_02': 'MIN1652',
    'TGM_017': 'MIN152',
    'TGM_017_GPONA_01': 'MIN152',
    'TGM_702': 'MIN1344',
    'TGM_702_GPONA_01': 'MIN1344',
    'TGM_704': 'MIN1341',
    'TGM_704_GPONA_01': 'MIN1341',
    'TGM_705': 'MIN1284',
    'TGM_705_GPONA_01': 'MIN1284',
    'TUL_001': 'MIN396',
    'TUL_001_GPONA_01': 'MIN396',
    'TUP_001': 'MIN773',
    'TUP_001_GPONA_01': 'MIN773',
}

# ─────────────────────────────────────────────────────────────────────────────
# SALES AREA + PROVINCE LOOKUP
# ─────────────────────────────────────────────────────────────────────────────

NAP_AREA_LOOKUP = {
    'BGN': 'AREA 7-1', 'BNY': 'AREA 7-1', 'CMP': 'AREA 7-1',
    'CRA': 'AREA 7-1', 'CRN': 'AREA 7-1', 'CTL': 'AREA 7-1',
    'DIG': 'AREA 7-1', 'DONMAR': 'AREA 7-1', 'DVO': 'AREA 7-1',
    'MAI': 'AREA 7-1', 'MAT': 'AREA 7-1', 'MCO': 'AREA 7-1',
    'MNK': 'AREA 7-1', 'MON': 'AREA 7-1', 'MTI': 'AREA 7-1',
    'NBN': 'AREA 7-1', 'PANABO': 'AREA 7-1', 'PDA': 'AREA 7-1',
    'PNB': 'AREA 7-1', 'PNT': 'AREA 7-1', 'SCD': 'AREA 7-1',
    'STM': 'AREA 7-1', 'TGM': 'AREA 7-1', 'LPN': 'AREA 7-1',
    'CMN': 'AREA 7-3', 'CTT': 'AREA 7-3', 'DOS': 'AREA 7-3',
    'KBC': 'AREA 7-3', 'KPW': 'AREA 7-3', 'LBU': 'AREA 7-3',
    'MDS': 'AREA 7-3', 'MLN': 'AREA 7-3', 'MTL': 'AREA 7-3',
    'PRN': 'AREA 7-3', 'SFA': 'AREA 7-3', 'TUL': 'AREA 7-3',
    'PGK': 'AREA 7-3', 'PIK': 'AREA 7-3', 'GUL': 'AREA 7-3',
    'BNG': 'AREA 7-4', 'ESR': 'AREA 7-4', 'GSN': 'AREA 7-4',
    'ISU': 'AREA 7-4', 'KRN': 'AREA 7-4', 'MLU': 'AREA 7-4',
    'PLM': 'AREA 7-4', 'TAN': 'AREA 7-4', 'TCR': 'AREA 7-4',
    'TUP': 'AREA 7-4',
    'MRU': 'AREA 7-1', 'PANABOL': 'AREA 7-1', 'DONMARL': 'AREA 7-1',
}

NAP_PROVINCE_LOOKUP = {
    'BGN': 'DAVAO ORIENTAL', 'BNY': 'DAVAO ORIENTAL', 'CMP': 'COMPOSTELA VALLEY',
    'CRA': 'DAVAO ORIENTAL', 'CRN': 'DAVAO DEL NORTE', 'CTL': 'DAVAO ORIENTAL',
    'DIG': 'DAVAO DEL SUR', 'DONMAR': 'DAVAO DEL SUR', 'DVO': 'DAVAO DEL SUR',
    'MAI': 'COMPOSTELA VALLEY', 'MAT': 'DAVAO DEL SUR', 'MCO': 'COMPOSTELA VALLEY',
    'MNK': 'COMPOSTELA VALLEY', 'MON': 'COMPOSTELA VALLEY', 'MTI': 'DAVAO ORIENTAL',
    'NBN': 'COMPOSTELA VALLEY', 'PANABO': 'DAVAO DEL NORTE', 'PDA': 'DAVAO DEL SUR',
    'PNB': 'DAVAO DEL NORTE', 'PNT': 'COMPOSTELA VALLEY', 'SCD': 'DAVAO DEL SUR',
    'STM': 'DAVAO DEL NORTE', 'TGM': 'DAVAO DEL NORTE', 'LPN': 'DAVAO ORIENTAL',
    'CMN': 'COTABATO (NORTH COTABATO)', 'CTT': 'COTABATO CITY (NOT A PROVINCE)',
    'DOS': 'MAGUINDANAO', 'KBC': 'COTABATO (NORTH COTABATO)',
    'KPW': 'COTABATO (NORTH COTABATO)', 'LBU': 'COTABATO (NORTH COTABATO)',
    'MDS': 'COTABATO (NORTH COTABATO)', 'MLN': 'COTABATO (NORTH COTABATO)',
    'MTL': 'COTABATO (NORTH COTABATO)', 'PRN': 'MAGUINDANAO',
    'SFA': 'MAGUINDANAO', 'TUL': 'COTABATO (NORTH COTABATO)',
    'PGK': 'COTABATO (NORTH COTABATO)', 'PIK': 'COTABATO (NORTH COTABATO)',
    'BNG': 'SOUTH COTABATO', 'ESR': 'SULTAN KUDARAT', 'GSN': 'SOUTH COTABATO',
    'ISU': 'SULTAN KUDARAT', 'KRN': 'SOUTH COTABATO', 'MLU': 'SARANGANI',
    'PLM': 'SOUTH COTABATO', 'TAN': 'SOUTH COTABATO', 'TCR': 'SULTAN KUDARAT',
    'TUP': 'SOUTH COTABATO', 'GUL': 'MAGUINDANAO', 'MRU': 'COMPOSTELA VALLEY',
    'PANABOL': 'DAVAO DEL NORTE', 'DONMARL': 'DAVAO DEL SUR',
}

# ─────────────────────────────────────────────────────────────────────────────
# PREFIX → TERRITORY
# ─────────────────────────────────────────────────────────────────────────────

PREFIX_TERRITORY = {
    'BGN': 'TERRITORY 7', 'BNG': 'TERRITORY 7', 'BNY': 'TERRITORY 7',
    'CMN': 'TERRITORY 7', 'CMP': 'TERRITORY 7', 'CRA': 'TERRITORY 7',
    'CRN': 'TERRITORY 7', 'CTL': 'TERRITORY 7', 'CTT': 'TERRITORY 7',
    'DIG': 'TERRITORY 7', 'DVO': 'TERRITORY 7', 'DOS': 'TERRITORY 7',
    'ESR': 'TERRITORY 7', 'GSN': 'TERRITORY 7', 'ISU': 'TERRITORY 7',
    'KBC': 'TERRITORY 7', 'KPW': 'TERRITORY 7', 'KRN': 'TERRITORY 7',
    'LBU': 'TERRITORY 7', 'LPN': 'TERRITORY 7', 'MAI': 'TERRITORY 7',
    'MAT': 'TERRITORY 7', 'MCO': 'TERRITORY 7', 'MDS': 'TERRITORY 7',
    'MLN': 'TERRITORY 7', 'MLU': 'TERRITORY 7', 'MNK': 'TERRITORY 7',
    'MON': 'TERRITORY 7', 'MTI': 'TERRITORY 7', 'MTL': 'TERRITORY 7',
    'NBN': 'TERRITORY 7', 'PANABO': 'TERRITORY 7', 'DONMAR': 'TERRITORY 7',
    'PDA': 'TERRITORY 7', 'PGK': 'TERRITORY 7', 'PIK': 'TERRITORY 7',
    'PLM': 'TERRITORY 7', 'PNB': 'TERRITORY 7', 'PNT': 'TERRITORY 7',
    'PRN': 'TERRITORY 7', 'SCD': 'TERRITORY 7', 'SFA': 'TERRITORY 7',
    'STM': 'TERRITORY 7', 'TAN': 'TERRITORY 7', 'TCR': 'TERRITORY 7',
    'TGM': 'TERRITORY 7', 'TUL': 'TERRITORY 7', 'TUP': 'TERRITORY 7',
    'GUL': 'TERRITORY 7', 'MRU': 'TERRITORY 7',
    'PANABOL': 'TERRITORY 7', 'DONMARL': 'TERRITORY 7',
}

SORTED_PREFIXES = sorted(PREFIX_TERRITORY.keys(), key=len, reverse=True)

# ─────────────────────────────────────────────────────────────────────────────
# CORE LOGIC FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def build_geo_lookup(ref_bytes: bytes) -> dict:
    df = pd.read_excel(io.BytesIO(ref_bytes))
    lookup = {}
    for _, row in df.iterrows():
        nap = str(row.get('NAP ID', '')).strip()
        if not nap:
            continue
        city = str(row.get('CITY_NAME', '')).strip()        if pd.notna(row.get('CITY_NAME'))        else ''
        brgy = str(row.get('BRGY_NAME', '')).strip()        if pd.notna(row.get('BRGY_NAME'))        else ''
        loc  = str(row.get('LOCATION TAGGING', '')).strip() if pd.notna(row.get('LOCATION TAGGING')) else ''
        lookup[nap] = (city, brgy, loc)
    return lookup


def get_nap_prefix(nap_id: str) -> str:
    if not nap_id:
        return ''
    nap_upper = nap_id.strip().upper()
    for prefix in SORTED_PREFIXES:
        if nap_upper.startswith(prefix):
            return prefix
    return ''


def get_sales_area(nap_id: str) -> str:
    return NAP_AREA_LOOKUP.get(get_nap_prefix(nap_id), '')


def get_province(nap_id: str) -> str:
    return NAP_PROVINCE_LOOKUP.get(get_nap_prefix(nap_id), '')


def get_tech(cabinet: str) -> str:
    if not cabinet:
        return ''
    if 'LSA' in cabinet.upper():
        return 'GPON'
    if cabinet.endswith('-M') or '-M' in cabinet:
        return CABINET_TECH_LOOKUP.get(cabinet, 'ADSL/VDSL')
    return 'GPON'


def get_pla_id(cabinet: str) -> str:
    if not cabinet:
        return ''
    parts = cabinet.strip().split('_')
    prefix = '_'.join(parts[:2])
    if prefix in PLA_ID_LOOKUP:
        return PLA_ID_LOOKUP[prefix]
    prefix = parts[0]
    if prefix in PLA_ID_LOOKUP:
        return PLA_ID_LOOKUP[prefix]
    return ''


def parse_raw(raw: str) -> dict | None:
    fields = raw.split(';')
    n = len(fields)
    if n < TRAILING_COLS + 6:
        return None
    tail = fields[n - TRAILING_COLS:]
    return {
        '_cabinet':        tail[4].strip(),
        '_nap_id':         fields[1].strip(),
        '_status':         fields[2].strip(),
        '_lat':            tail[0].strip(),
        '_lon':            tail[1].strip(),
        '_discovered':     tail[2].strip(),
        '_ports_total':    tail[6].strip(),
        '_ports_assigned': tail[7].strip(),
        '_ports_reserved': tail[8].strip(),
    }


JUNK_PATTERNS = [
    re.compile(r'^\s*nap facility summary report', re.IGNORECASE),
    re.compile(r'^\s*object\s*:', re.IGNORECASE),
    re.compile(r'^\s*specified report', re.IGNORECASE),
    re.compile(r'^\s*nap name pattern', re.IGNORECASE),
    re.compile(r'^\s*report results', re.IGNORECASE),
    re.compile(r'^\s*\d+\s+rows?\s+are\s+displayed', re.IGNORECASE),
    re.compile(r'^\s*location\s*$', re.IGNORECASE),
]


def is_junk_row(raw: str) -> bool:
    first_field = raw.split(';')[0].strip()
    return any(p.match(first_field) for p in JUNK_PATTERNS)


def get_territory(nap_id: str) -> str:
    nap_upper = nap_id.upper().strip()
    for prefix in SORTED_PREFIXES:
        p = prefix.upper()
        if nap_upper == p:
            return PREFIX_TERRITORY[prefix]
        if len(nap_upper) > len(p):
            next_char = nap_upper[len(p)]
            if next_char in ('_', '-') or next_char.isdigit() or next_char == 'L':
                if nap_upper.startswith(p):
                    return PREFIX_TERRITORY[prefix]
    return ''


def apply_filters(rec: dict) -> bool:
    return get_territory(rec['_nap_id']) != ""


def to_int(val: str) -> int | str:
    try:
        return int(val)
    except (ValueError, TypeError):
        return val


def to_coord(val: str) -> str:
    return val.strip() if val.strip() else ''


def strip_suffix(nap_id: str) -> str:
    return re.sub(r'(?<=\d)[A-Za-z]$', '', nap_id)


def calc_utilization(pa: int | str, pt: int | str) -> float | str:
    try:
        t = int(pt)
        a = int(pa)
        return 0.0 if t == 0 else round(a / t, 4)
    except (ValueError, ZeroDivisionError):
        return ''


def merge_duplicates(all_recs: list, geo_lookup: dict) -> list:
    from collections import OrderedDict
    merged = OrderedDict()

    for rec in all_recs:
        base = strip_suffix(rec['_nap_id'])
        if base not in merged:
            merged[base] = {
                '_cabinet':           rec['_cabinet'],
                '_nap_id':            base,
                '_discovered':        rec['_discovered'],
                '_lat':               rec['_lat'],
                '_lon':               rec['_lon'],
                '_ports_assigned':    to_int(rec['_ports_assigned']),
                '_ports_reserved':    to_int(rec['_ports_reserved']),
                '_ports_total':       to_int(rec['_ports_total']),
                '_territory':         get_territory(rec['_nap_id']),
                '_first_ports_total': to_int(rec['_ports_total']),
            }
        else:
            existing    = merged[base]
            new_pt      = to_int(rec['_ports_total'])
            new_pa      = to_int(rec['_ports_assigned'])
            new_pr      = to_int(rec['_ports_reserved'])
            existing_pt = existing['_ports_total']    if isinstance(existing['_ports_total'],    int) else 0
            existing_pa = existing['_ports_assigned'] if isinstance(existing['_ports_assigned'], int) else 0
            existing_pr = existing['_ports_reserved'] if isinstance(existing['_ports_reserved'], int) else 0
            first_pt    = existing['_first_ports_total'] if isinstance(existing['_first_ports_total'], int) else 0

            existing['_ports_assigned'] = existing_pa + (new_pa if isinstance(new_pa, int) else 0)
            existing['_ports_reserved'] = existing_pr + (new_pr if isinstance(new_pr, int) else 0)

            if isinstance(new_pt, int) and new_pt == 16 and first_pt == 16:
                existing['_ports_total'] = 16
            else:
                existing['_ports_total'] = existing_pt + (new_pt if isinstance(new_pt, int) else 0)

    result = []
    for base, m in merged.items():
        pa   = m['_ports_assigned']
        pt   = m['_ports_total']
        util = 0.0 if pt == 0 else round(pa / pt, 4) if isinstance(pa, int) and isinstance(pt, int) else ''
        result.append([
            m['_cabinet'],
            m['_nap_id'],
            m['_discovered'],
            get_pla_id(m['_cabinet']),
            get_tech(m['_cabinet']),
            pa,
            m['_ports_reserved'],
            pt,
            util,
            to_coord(m['_lat']),
            to_coord(m['_lon']),
            get_sales_area(m['_nap_id']),
            m['_territory'],
            geo_lookup.get(m['_nap_id'], ('', '', ''))[1],
            geo_lookup.get(m['_nap_id'], ('', '', ''))[0],
            get_province(m['_nap_id']),
            geo_lookup.get(m['_nap_id'], ('', '', ''))[2],
        ])
    return result


def process_and_build(file_bytes: bytes, geo_lookup: dict, progress_bar) -> tuple:
    use_chunking = len(file_bytes) > SMALL_FILE_THRESHOLD

    buf = io.BytesIO()
    wb  = xlsxwriter.Workbook(buf, {'constant_memory': use_chunking})
    ws  = wb.add_worksheet('NAP Data')

    base       = {'font_name': 'Arial', 'font_size': 10, 'align': 'left', 'valign': 'vcenter'}
    fmt_yellow = wb.add_format({**base, 'bold': True, 'bg_color': '#FFFF00'})
    fmt_green  = wb.add_format({**base, 'bold': True, 'bg_color': '#92D050'})
    fmt_white  = wb.add_format({**base, 'bold': True})
    fmt_data   = wb.add_format({**base})
    fmt_pct    = wb.add_format({**base, 'num_format': '0%'})
    fmt_coord  = wb.add_format({**base})

    for c_idx in range(len(OUTPUT_COLS)):
        ws.set_column(c_idx, c_idx, 21)
    ws.set_default_row(20)
    ws.set_row(0, 20)

    for c_idx, col_name in enumerate(OUTPUT_COLS):
        if col_name in YELLOW_COLS:
            fmt = fmt_yellow
        elif col_name in GREEN_COLS:
            fmt = fmt_green
        else:
            fmt = fmt_white
        ws.write(0, c_idx, col_name, fmt)

    util_idx = OUTPUT_COLS.index('UTILIZATION')
    lat_idx  = OUTPUT_COLS.index('Latitude')
    lon_idx  = OUTPUT_COLS.index('Longitude')

    def write_row(excel_row: int, out_row: list):
        for c_idx, val in enumerate(out_row):
            if c_idx == util_idx:
                ws.write(excel_row, c_idx, val, fmt_pct)
            elif c_idx in (lat_idx, lon_idx):
                ws.write_string(excel_row, c_idx, val, fmt_coord)
            else:
                ws.write(excel_row, c_idx, val, fmt_data)

    text   = file_bytes.decode('utf-8-sig', errors='replace')
    reader = csv.reader(io.StringIO(text))

    total_read = total_written = total_skipped = 0
    all_recs   = []

    for i, row in enumerate(reader):
        if i == 0 or not row:
            continue
        raw = ''.join(c for c in row if c.strip())
        if not raw.strip():
            continue
        if is_junk_row(raw):
            continue

        total_read += 1
        rec = parse_raw(raw)

        if rec is None:
            total_skipped += 1
            continue
        if not apply_filters(rec):
            total_skipped += 1
            continue

        all_recs.append(rec)

        if total_read % CHUNK_SIZE == 0:
            progress_bar.progress(
                min(total_read / max(total_read, 1), 0.6),
                text=f"Reading... {total_read:,} rows so far"
            )

    progress_bar.progress(0.7, text="Merging duplicate NAP IDs...")
    merged_rows   = merge_duplicates(all_recs, geo_lookup)
    total_written = len(merged_rows)

    excel_row    = 1
    preview_rows = []
    for idx, out_row in enumerate(merged_rows):
        write_row(excel_row, out_row)
        excel_row += 1
        if len(preview_rows) < 50:
            preview_rows.append(out_row)
        if idx % CHUNK_SIZE == 0 and idx > 0:
            progress_bar.progress(
                min(0.7 + (idx / total_written) * 0.3, 0.95),
                text=f"Writing... {idx:,} rows written"
            )

    progress_bar.progress(1.0, text="Finalizing Excel file...")
    wb.close()
    buf.seek(0)

    return buf.getvalue(), total_read, total_written, total_skipped, preview_rows, merged_rows


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_supabase() -> Client:
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        return create_client(url, key)
    except KeyError:
        st.error(
            "Supabase credentials missing. Add `SUPABASE_URL` and `SUPABASE_KEY` "
            "to your Streamlit secrets (.streamlit/secrets.toml)."
        )
        st.stop()


# ─────────────────────────────────────────────────────────────────────────────
# AUTH HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def authenticate(username: str, password: str) -> dict | None:
    sb  = get_supabase()
    res = sb.table("users").select("*").eq("username", username.strip()).execute()
    if res.data and verify_password(password, res.data[0]["password_hash"]):
        return res.data[0]
    return None


def user_count() -> int:
    sb  = get_supabase()
    res = sb.table("users").select("id", count="exact").execute()
    return res.count or 0


def get_all_users() -> list:
    sb  = get_supabase()
    res = sb.table("users").select("id, username, is_admin, created_at").order("created_at").execute()
    return res.data or []


def add_user(username: str, password: str, is_admin: bool = False) -> tuple[bool, str]:
    sb = get_supabase()
    try:
        sb.table("users").insert({
            "username":      username.strip(),
            "password_hash": hash_password(password),
            "is_admin":      is_admin,
        }).execute()
        return True, ""
    except Exception as e:
        return False, str(e)


def update_password(username: str, new_password: str) -> bool:
    sb = get_supabase()
    try:
        sb.table("users").update(
            {"password_hash": hash_password(new_password)}
        ).eq("username", username).execute()
        return True
    except Exception:
        return False


def delete_user(user_id: int) -> bool:
    sb = get_supabase()
    try:
        sb.table("users").delete().eq("id", user_id).execute()
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# PAGES
# ─────────────────────────────────────────────────────────────────────────────

def page_first_setup():
    """Shown only when the users table is empty — creates the first admin."""
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.title("📡 NAP Data Converter")
        st.subheader("First-Time Setup")
        st.info("No accounts found. Create the first admin to get started.")

        with st.form("setup_form"):
            username = st.text_input("Admin Username")
            password = st.text_input("Password", type="password")
            confirm  = st.text_input("Confirm Password", type="password")
            if st.form_submit_button("Create Admin Account", type="primary", use_container_width=True):
                if not username or not password:
                    st.error("All fields are required.")
                elif password != confirm:
                    st.error("Passwords do not match.")
                elif len(password) < 8:
                    st.error("Password must be at least 8 characters.")
                else:
                    ok, err = add_user(username, password, is_admin=True)
                    if ok:
                        st.success("Admin account created. Please log in.")
                        st.rerun()
                    else:
                        st.error(f"Failed to create account: {err}")


def page_login():
    """Standard login screen."""
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.title("📡 NAP Data Converter")
        st.subheader("Sign In")

        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            if st.form_submit_button("Login", type="primary", use_container_width=True):
                if not username or not password:
                    st.error("Enter your username and password.")
                else:
                    user = authenticate(username, password)
                    if user:
                        st.session_state.logged_in = True
                        st.session_state.username  = user["username"]
                        st.session_state.is_admin  = user["is_admin"]
                        st.rerun()
                    else:
                        st.error("Invalid username or password.")


def page_user_management():
    """Admin-only: add users, change passwords, delete users."""
    st.title("👥 User Management")

    users = get_all_users()

    # ── User list ─────────────────────────────────────────────────────────────
    st.subheader("Current Users")
    if users:
        h1, h2, h3 = st.columns([3, 2, 1])
        h1.markdown("**Username**")
        h2.markdown("**Role**")
        h3.markdown("**Action**")
        st.divider()
        for u in users:
            c1, c2, c3 = st.columns([3, 2, 1])
            c1.write(u["username"])
            c2.write("Admin" if u["is_admin"] else "User")
            if u["username"] != st.session_state.username:
                if c3.button("Delete", key=f"del_{u['id']}"):
                    if delete_user(u["id"]):
                        st.success(f"Deleted **{u['username']}**.")
                        st.rerun()
                    else:
                        st.error("Delete failed.")
            else:
                c3.caption("(you)")
    else:
        st.info("No users found.")

    st.divider()

    # ── Add user ──────────────────────────────────────────────────────────────
    st.subheader("Add New User")
    with st.form("add_user_form", clear_on_submit=True):
        col1, col2  = st.columns(2)
        new_username = col1.text_input("Username")
        new_password = col2.text_input("Password", type="password")
        new_is_admin = st.checkbox("Grant admin access")
        if st.form_submit_button("Add User", type="primary", use_container_width=True):
            if not new_username or not new_password:
                st.error("Username and password are required.")
            elif len(new_password) < 8:
                st.error("Password must be at least 8 characters.")
            else:
                ok, err = add_user(new_username, new_password, new_is_admin)
                if ok:
                    st.success(f"User **{new_username}** added.")
                    st.rerun()
                else:
                    st.error("Failed — username may already exist.")

    st.divider()

    # ── Change any user's password ────────────────────────────────────────────
    st.subheader("Change Password")
    with st.form("change_pw_form", clear_on_submit=True):
        usernames  = [u["username"] for u in users]
        target     = st.selectbox("Select user", usernames)
        col1, col2 = st.columns(2)
        new_pw     = col1.text_input("New Password", type="password")
        confirm_pw = col2.text_input("Confirm Password", type="password")
        if st.form_submit_button("Change Password", type="primary", use_container_width=True):
            if not new_pw or not confirm_pw:
                st.error("Both fields are required.")
            elif new_pw != confirm_pw:
                st.error("Passwords do not match.")
            elif len(new_pw) < 8:
                st.error("Password must be at least 8 characters.")
            else:
                if update_password(target, new_pw):
                    st.success(f"Password updated for **{target}**.")
                else:
                    st.error("Failed to update password.")


def page_change_my_password():
    """Any logged-in user can change their own password."""
    st.title("🔒 Change My Password")

    with st.form("my_pw_form", clear_on_submit=True):
        current  = st.text_input("Current Password", type="password")
        new_pw   = st.text_input("New Password", type="password")
        confirm  = st.text_input("Confirm New Password", type="password")
        if st.form_submit_button("Update Password", type="primary", use_container_width=True):
            if not current or not new_pw or not confirm:
                st.error("All fields are required.")
            elif not authenticate(st.session_state.username, current):
                st.error("Current password is incorrect.")
            elif new_pw != confirm:
                st.error("New passwords do not match.")
            elif len(new_pw) < 8:
                st.error("Password must be at least 8 characters.")
            else:
                if update_password(st.session_state.username, new_pw):
                    st.success("Password updated successfully.")
                else:
                    st.error("Failed to update password.")


# ─────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────────────────────────────────────

DB_BATCH = 500  # rows per Supabase upsert call


def load_geo_from_db() -> dict:
    """Load entire nap_geo table into a {nap_id: (city, brgy, loc)} dict."""
    sb       = get_supabase()
    result   = {}
    page     = 0
    while True:
        res = (
            sb.table("nap_geo")
            .select("nap_id, city_name, brgy_name, location_tagging")
            .range(page * 1000, (page + 1) * 1000 - 1)
            .execute()
        )
        if not res.data:
            break
        for row in res.data:
            result[row["nap_id"]] = (
                row.get("city_name", ""),
                row.get("brgy_name", ""),
                row.get("location_tagging", ""),
            )
        if len(res.data) < 1000:
            break
        page += 1
    return result


def upload_geo_to_db(ref_bytes: bytes) -> tuple[int, str]:
    """Upsert rows from a reference Excel into nap_geo. Returns (count, error)."""
    sb = get_supabase()
    df = pd.read_excel(io.BytesIO(ref_bytes))
    records = []
    for _, row in df.iterrows():
        nap = str(row.get("NAP ID", "")).strip()
        if not nap:
            continue
        records.append({
            "nap_id":           nap,
            "city_name":        str(row.get("CITY_NAME", "")).strip()        if pd.notna(row.get("CITY_NAME"))        else "",
            "brgy_name":        str(row.get("BRGY_NAME", "")).strip()        if pd.notna(row.get("BRGY_NAME"))        else "",
            "location_tagging": str(row.get("LOCATION TAGGING", "")).strip() if pd.notna(row.get("LOCATION TAGGING")) else "",
            "updated_at":       datetime.utcnow().isoformat(),
        })
    total = 0
    try:
        for i in range(0, len(records), DB_BATCH):
            sb.table("nap_geo").upsert(records[i:i + DB_BATCH]).execute()
            total += len(records[i:i + DB_BATCH])
        return total, ""
    except Exception as e:
        return total, str(e)


def save_to_db(merged_rows: list, snapshot_date: str, uploaded_by: str) -> tuple[int, str]:
    """Upsert all converted rows into nap_data. Returns (count, error)."""
    sb = get_supabase()
    records = []
    for row in merged_rows:
        records.append({
            "snapshot_date":    snapshot_date,
            "uploaded_by":      uploaded_by,
            "cabinet":          row[0]  or None,
            "nap_id":           row[1],
            "discovered_when":  row[2]  or None,
            "pla_id":           row[3]  or None,
            "tech":             row[4]  or None,
            "ports_assigned":   row[5]  if isinstance(row[5], int)   else None,
            "ports_reserved":   row[6]  if isinstance(row[6], int)   else None,
            "ports_total":      row[7]  if isinstance(row[7], int)   else None,
            "utilization":      float(row[8]) if isinstance(row[8], float) else None,
            "latitude":         row[9]  or None,
            "longitude":        row[10] or None,
            "sales_area":       row[11] or None,
            "territory":        row[12] or None,
            "brgy_name":        row[13] or None,
            "city_name":        row[14] or None,
            "province_name":    row[15] or None,
            "location_tagging": row[16] or None,
        })
    total = 0
    try:
        for i in range(0, len(records), DB_BATCH):
            sb.table("nap_data").upsert(
                records[i:i + DB_BATCH],
                on_conflict="nap_id,snapshot_date"
            ).execute()
            total += len(records[i:i + DB_BATCH])
        return total, ""
    except Exception as e:
        return total, str(e)


def get_snapshot_summary() -> list:
    """Return list of {snapshot_date, uploaded_by, row_count} ordered by date desc."""
    sb  = get_supabase()
    res = (
        sb.table("nap_data")
        .select("snapshot_date, uploaded_by")
        .order("snapshot_date", desc=True)
        .execute()
    )
    if not res.data:
        return []
    summary = {}
    for row in res.data:
        d = row["snapshot_date"]
        if d not in summary:
            summary[d] = {"snapshot_date": d, "uploaded_by": row["uploaded_by"], "row_count": 0}
        summary[d]["row_count"] += 1
    return list(summary.values())


def get_snapshot_rows(snapshot_date: str) -> list:
    """Fetch all nap_data rows for a given date (paginated). Returns list of dicts."""
    sb       = get_supabase()
    all_rows = []
    page     = 0
    while True:
        res = (
            sb.table("nap_data")
            .select("*")
            .eq("snapshot_date", snapshot_date)
            .range(page * 1000, (page + 1) * 1000 - 1)
            .execute()
        )
        if not res.data:
            break
        all_rows.extend(res.data)
        if len(res.data) < 1000:
            break
        page += 1
    return all_rows


def build_excel_from_db_rows(db_rows: list) -> bytes:
    """Reconstruct an Excel file from nap_data DB rows."""
    output_rows = []
    for r in db_rows:
        util = r.get("utilization")
        output_rows.append([
            r.get("cabinet", "")          or "",
            r.get("nap_id", "")           or "",
            r.get("discovered_when", "")  or "",
            r.get("pla_id", "")           or "",
            r.get("tech", "")             or "",
            r.get("ports_assigned")       if r.get("ports_assigned") is not None else "",
            r.get("ports_reserved")       if r.get("ports_reserved") is not None else "",
            r.get("ports_total")          if r.get("ports_total")    is not None else "",
            float(util)                   if util is not None else "",
            r.get("latitude", "")         or "",
            r.get("longitude", "")        or "",
            r.get("sales_area", "")       or "",
            r.get("territory", "")        or "",
            r.get("brgy_name", "")        or "",
            r.get("city_name", "")        or "",
            r.get("province_name", "")    or "",
            r.get("location_tagging", "") or "",
        ])

    buf = io.BytesIO()
    wb  = xlsxwriter.Workbook(buf)
    ws  = wb.add_worksheet("NAP Data")
    base      = {"font_name": "Arial", "font_size": 10, "align": "left", "valign": "vcenter"}
    fmt_yellow = wb.add_format({**base, "bold": True, "bg_color": "#FFFF00"})
    fmt_green  = wb.add_format({**base, "bold": True, "bg_color": "#92D050"})
    fmt_white  = wb.add_format({**base, "bold": True})
    fmt_data   = wb.add_format({**base})
    fmt_pct    = wb.add_format({**base, "num_format": "0%"})
    fmt_coord  = wb.add_format({**base})

    for c_idx in range(len(OUTPUT_COLS)):
        ws.set_column(c_idx, c_idx, 21)
    ws.set_default_row(20)
    ws.set_row(0, 20)
    for c_idx, col_name in enumerate(OUTPUT_COLS):
        fmt = fmt_yellow if col_name in YELLOW_COLS else fmt_green if col_name in GREEN_COLS else fmt_white
        ws.write(0, c_idx, col_name, fmt)

    util_idx = OUTPUT_COLS.index("UTILIZATION")
    lat_idx  = OUTPUT_COLS.index("Latitude")
    lon_idx  = OUTPUT_COLS.index("Longitude")

    for r_idx, out_row in enumerate(output_rows, start=1):
        for c_idx, val in enumerate(out_row):
            if c_idx == util_idx:
                ws.write(r_idx, c_idx, val, fmt_pct)
            elif c_idx in (lat_idx, lon_idx):
                ws.write_string(r_idx, c_idx, str(val), fmt_coord)
            else:
                ws.write(r_idx, c_idx, val, fmt_data)

    wb.close()
    buf.seek(0)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# GEO REFERENCE PAGE
# ─────────────────────────────────────────────────────────────────────────────

def upsert_single_geo(nap_id: str, city: str, brgy: str, loc: str) -> tuple[bool, str]:
    """Insert or update a single NAP ID in nap_geo."""
    sb = get_supabase()
    try:
        sb.table("nap_geo").upsert({
            "nap_id":           nap_id.strip(),
            "city_name":        city.strip(),
            "brgy_name":        brgy.strip(),
            "location_tagging": loc.strip(),
            "updated_at":       datetime.utcnow().isoformat(),
        }).execute()
        return True, ""
    except Exception as e:
        return False, str(e)


def delete_single_geo(nap_id: str) -> tuple[bool, str]:
    """Delete a single NAP ID from nap_geo."""
    sb = get_supabase()
    try:
        sb.table("nap_geo").delete().eq("nap_id", nap_id).execute()
        return True, ""
    except Exception as e:
        return False, str(e)


def page_geo_reference():
    """Admin page to upload and manage the NAP GEO reference table."""
    st.title("📁 GEO Reference Manager")
    st.divider()

    tab1, tab2, tab3 = st.tabs(["➕ Add / Edit Entry", "📤 Bulk Upload", "📋 View All"])

    # ── Tab 1: Add or edit a single NAP ID ───────────────────────────────────
    with tab1:
        st.subheader("Add or Update a Single NAP ID")
        st.caption("Use this when there's a new NAP ID or you need to correct an existing entry.")

        with st.spinner("Loading existing entries..."):
            geo = load_geo_from_db()

        # Pre-fill form if NAP ID already exists
        search_nap = st.text_input("NAP ID to add or edit", placeholder="e.g. DVO05L02N01")
        existing   = geo.get(search_nap.strip().upper(), geo.get(search_nap.strip(), None))

        with st.form("single_geo_form", clear_on_submit=True):
            nap_id   = st.text_input("NAP ID",           value=search_nap)
            city     = st.text_input("City Name",         value=existing[0] if existing else "")
            brgy     = st.text_input("Barangay Name",     value=existing[1] if existing else "")
            loc      = st.text_input("Location Tagging",  value=existing[2] if existing else "")

            col1, col2 = st.columns(2)
            save_btn   = col1.form_submit_button("💾 Save", type="primary", use_container_width=True)
            delete_btn = col2.form_submit_button("🗑️ Delete", use_container_width=True)

            if save_btn:
                if not nap_id.strip():
                    st.error("NAP ID is required.")
                else:
                    ok, err = upsert_single_geo(nap_id, city, brgy, loc)
                    if ok:
                        action = "Updated" if existing else "Added"
                        st.success(f"{action} **{nap_id.strip()}** successfully.")
                        st.rerun()
                    else:
                        st.error(f"Failed: {err}")

            if delete_btn:
                if not nap_id.strip():
                    st.error("NAP ID is required.")
                else:
                    ok, err = delete_single_geo(nap_id.strip())
                    if ok:
                        st.success(f"Deleted **{nap_id.strip()}**.")
                        st.rerun()
                    else:
                        st.error(f"Failed: {err}")

    # ── Tab 2: Bulk upload Excel ──────────────────────────────────────────────
    with tab2:
        st.subheader("Bulk Upload from Excel")
        st.caption("Upload your NAP GEO Reference Excel to add or update many entries at once.")

        ref_file = st.file_uploader("NAP GEO Reference Excel", type=["xlsx"])
        if ref_file:
            st.info(f"**{ref_file.name}** ready to upload.")
            if st.button("Upload to Database", type="primary", use_container_width=True):
                with st.spinner("Uploading..."):
                    count, err = upload_geo_to_db(ref_file.read())
                if err:
                    st.error(f"Upload failed: {err}")
                else:
                    st.success(f"Uploaded **{count:,}** NAP IDs successfully.")

    # ── Tab 3: View all entries ───────────────────────────────────────────────
    with tab3:
        st.subheader("Current Entries")
        with st.spinner("Loading..."):
            geo = load_geo_from_db()

        if not geo:
            st.info("No GEO reference data found.")
        else:
            st.metric("Total NAP IDs stored", f"{len(geo):,}")
            search = st.text_input("Search NAP ID", key="geo_search")
            rows = [
                {"NAP ID": nap, "City": v[0], "Barangay": v[1], "Location Tagging": v[2]}
                for nap, v in geo.items()
                if not search or search.upper() in nap.upper()
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, height=400)


# ─────────────────────────────────────────────────────────────────────────────
# DATA HISTORY PAGE
# ─────────────────────────────────────────────────────────────────────────────

def page_data_history():
    """View and re-download past conversion snapshots."""
    st.title("🕓 Data History")
    st.divider()

    with st.spinner("Loading snapshots..."):
        snapshots = get_snapshot_summary()

    if not snapshots:
        st.info("No snapshots saved yet. Run a conversion first.")
        return

    st.subheader(f"Saved Snapshots ({len(snapshots)})")

    h1, h2, h3, h4 = st.columns([2, 2, 2, 1])
    h1.markdown("**Date**")
    h2.markdown("**Uploaded By**")
    h3.markdown("**Rows**")
    h4.markdown("**Download**")
    st.divider()

    for snap in snapshots:
        c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
        c1.write(snap["snapshot_date"])
        c2.write(snap["uploaded_by"])
        c3.write(f"{snap['row_count']:,}")
        if c4.button("Excel", key=f"dl_{snap['snapshot_date']}"):
            with st.spinner(f"Building Excel for {snap['snapshot_date']}..."):
                db_rows    = get_snapshot_rows(snap["snapshot_date"])
                xlsx_bytes = build_excel_from_db_rows(db_rows)
            st.download_button(
                label=f"⬇️ Download {snap['snapshot_date']}",
                data=xlsx_bytes,
                file_name=f"NAP_data_{snap['snapshot_date']}_cleaned.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_{snap['snapshot_date']}",
            )


def page_main_app():
    """The NAP CSV → Excel converter with automatic DB save."""
    st.title("📡 NAP Data Converter")
    st.divider()

    # ── Snapshot date ─────────────────────────────────────────────────────────
    st.subheader("📅 Snapshot Date")
    snapshot_date = st.date_input(
        "Date of this data (default: today)",
        value=date.today(),
        help="Change this if the CSV is from a different date.",
    )

    st.subheader("📂 Upload CSV")
    uploaded = st.file_uploader(
        "NAP CSV File",
        type=["csv"],
        help="The raw semicolon-delimited CSV exported from your system.",
    )

    if uploaded:
        st.success(f"Ready: **{uploaded.name}** ({uploaded.size / 1_000_000:.2f} MB)")

        if st.button("🚀 Convert & Save", use_container_width=True, type="primary"):
            progress_bar = st.progress(0, text="Starting...")

            # Load GEO reference from DB
            progress_bar.progress(0.05, text="Loading GEO reference from database...")
            geo_lookup = load_geo_from_db()

            file_bytes = uploaded.read()
            xlsx_bytes, total_read, total_written, total_skipped, preview_rows, merged_rows = process_and_build(
                file_bytes, geo_lookup, progress_bar
            )

            if total_written == 0:
                st.error("No valid rows found. Please check your CSV and try again.")
            else:
                # ── Save to DB ────────────────────────────────────────────────
                save_status = st.empty()
                save_status.info("Saving to database...")
                saved, err = save_to_db(
                    merged_rows,
                    snapshot_date.isoformat(),
                    st.session_state.username,
                )
                if err:
                    save_status.warning(f"Saved {saved:,} rows but encountered an error: {err}")
                else:
                    save_status.success(f"Saved **{saved:,}** rows to database for **{snapshot_date}**.")

                # ── Summary ───────────────────────────────────────────────────
                st.divider()
                st.subheader("📊 Summary")
                m1, m2, m3 = st.columns(3)
                m1.metric("Total Rows Read",   f"{total_read:,}")
                m2.metric("Rows Written",      f"{total_written:,}")
                m3.metric("Rows Filtered Out", f"{total_skipped:,}")

                # ── Preview ───────────────────────────────────────────────────
                st.subheader("👀 Preview (first 50 rows)")
                df_preview = pd.DataFrame(preview_rows, columns=OUTPUT_COLS)
                df_preview['UTILIZATION'] = df_preview['UTILIZATION'].apply(
                    lambda x: f"{round(x * 100)}%" if isinstance(x, float) else x
                )
                st.dataframe(df_preview, use_container_width=True)

                # ── Download ──────────────────────────────────────────────────
                stem        = uploaded.name.rsplit('.', 1)[0] if '.' in uploaded.name else uploaded.name
                output_name = f"{stem}_{snapshot_date}_cleaned.xlsx"
                st.divider()
                st.download_button(
                    label="⬇️ Download Cleaned Excel File",
                    data=xlsx_bytes,
                    file_name=output_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary",
                )
    else:
        st.info("Upload a CSV file above to get started.")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="NAP Data Converter",
    page_icon="📡",
    layout="wide",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background-color: #F0F4FF;
    border-right: 1px solid #D6E0FF;
    padding-top: 0;
}
[data-testid="stSidebar"] > div:first-child {
    padding-top: 0;
}

/* Logo area */
.sidebar-logo {
    background: linear-gradient(135deg, #4361EE, #3A0CA3);
    color: white;
    padding: 20px 16px 16px 16px;
    margin-bottom: 8px;
}
.sidebar-logo h2 {
    color: white !important;
    margin: 0;
    font-size: 18px;
    font-weight: 700;
}
.sidebar-logo p {
    color: rgba(255,255,255,0.75);
    margin: 4px 0 0 0;
    font-size: 12px;
}

/* Section labels */
.nav-section {
    font-size: 10px;
    font-weight: 700;
    color: #8898C0;
    letter-spacing: 1px;
    text-transform: uppercase;
    padding: 12px 16px 4px 16px;
}

/* Nav radio buttons */
[data-testid="stSidebar"] .stRadio > div {
    gap: 2px;
    padding: 0 8px;
}
[data-testid="stSidebar"] .stRadio label {
    padding: 10px 14px !important;
    border-radius: 8px !important;
    font-size: 14px !important;
    color: #3A4A6B !important;
    font-weight: 500;
    transition: all 0.15s ease;
    cursor: pointer;
}
[data-testid="stSidebar"] .stRadio label:hover {
    background-color: #DDE8FF !important;
    color: #4361EE !important;
}
[data-testid="stSidebar"] .stRadio label[data-checked="true"] {
    background-color: #4361EE !important;
    color: white !important;
}

/* User info card */
.user-card {
    background: white;
    border: 1px solid #D6E0FF;
    border-radius: 10px;
    padding: 12px 14px;
    margin: 8px;
    font-size: 13px;
    color: #3A4A6B;
}
.user-card strong { color: #4361EE; }

/* ── Main content ── */
.block-container {
    padding-top: 1.5rem !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
}

/* Page titles */
h1 { color: #1A202C !important; font-weight: 700 !important; }
h2, h3 { color: #2D3748 !important; }

/* Metric cards */
[data-testid="metric-container"] {
    background: white;
    border: 1px solid #E2E8F0;
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}

/* Buttons */
.stButton > button {
    border-radius: 8px !important;
    font-weight: 500 !important;
}

/* Tabs */
.stTabs [data-baseweb="tab"] {
    border-radius: 6px 6px 0 0;
    font-weight: 500;
}

/* Dataframe */
[data-testid="stDataFrame"] {
    border-radius: 10px;
    overflow: hidden;
    border: 1px solid #E2E8F0;
}

/* Divider */
hr { border-color: #E2E8F0 !important; }

/* Success / info / warning messages */
[data-testid="stAlert"] {
    border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username  = ""
    st.session_state.is_admin  = False

# ── Not logged in ─────────────────────────────────────────────────────────────
if not st.session_state.logged_in:
    if user_count() == 0:
        page_first_setup()
    else:
        page_login()

# ── Logged in ─────────────────────────────────────────────────────────────────
else:
    with st.sidebar:
        # Logo
        st.markdown("""
        <div class="sidebar-logo">
            <h2>📡 NAP Converter</h2>
            <p>Network Access Point System</p>
        </div>
        """, unsafe_allow_html=True)

        # User info
        role = "Admin" if st.session_state.is_admin else "User"
        st.markdown(f"""
        <div class="user-card">
            👤 <strong>{st.session_state.username}</strong><br>
            <span style="font-size:11px; color:#8898C0;">{role}</span>
        </div>
        """, unsafe_allow_html=True)

        # Navigation
        st.markdown('<div class="nav-section">Main</div>', unsafe_allow_html=True)
        nav_options = ["📡  Converter", "🕓  Data History"]

        if st.session_state.is_admin:
            st.markdown('<div class="nav-section">Admin</div>', unsafe_allow_html=True)
            nav_options += ["📁  GEO Reference", "👥  User Management"]

        st.markdown('<div class="nav-section">Account</div>', unsafe_allow_html=True)
        nav_options += ["🔒  Change Password"]

        nav = st.radio("", nav_options, label_visibility="collapsed")

        # Logout at bottom
        st.markdown("<br>" * 2, unsafe_allow_html=True)
        if st.button("↩  Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.username  = ""
            st.session_state.is_admin  = False
            st.rerun()

    if nav == "📡  Converter":
        page_main_app()
    elif nav == "📁  GEO Reference":
        page_geo_reference()
    elif nav == "🕓  Data History":
        page_data_history()
    elif nav == "👥  User Management":
        page_user_management()
    elif nav == "🔒  Change Password":
        page_change_my_password()
