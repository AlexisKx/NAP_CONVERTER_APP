import csv
import re
import io
import pandas as pd
import streamlit as st
import xlsxwriter

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
# PLA ID LOOKUP
# Cabinet prefix (e.g. BGN_001) → PLA ID (e.g. MIN210)
# Source: Book1.xlsx reference file (268 entries)
# ─────────────────────────────────────────────────────────────────────────────

PLA_ID_LOOKUP = {
    'BGN_001': 'MIN210',
    'BNG_001': 'MIN754',
    'BNY_001': 'MIN212',
    'CMN_001': 'MIN380',
    'CMP_001': 'MIN816',
    'CRA_001': 'MIN5',
    'CRN_001': 'MIN131',
    'CRN_002': 'MIN136',
    'CRN_003': 'MIN227',
    'CRN_004': 'MIN3167',
    'CTL_001': 'MIN215',
    'CTT_001': 'MIN1761',
    'CTT_002': 'MIN269',
    'CTT_003': 'MIN288',
    'CTT_004': 'MIN859',
    'CTT_005': 'MIN283',
    'CTT_006': 'MIN1529',
    'CTT_007': 'MIN1530',
    'DIG_001': 'MIN820',
    'DIG_002': 'MIN1611',
    'DIG_003': 'MIN1492',
    'DIG_004': 'MIN156',
    'DIG_005': 'MIN188',
    'DIG_007': 'MIN1582',
    'DIG_008': 'MIN1152',
    'DIG_009': 'MIN1081',
    'DIG_701': 'MIN1160',
    'DIG_702': 'MIN1161',
    'DIG_704': 'MIN1163',
    'DIG_705': 'MIN1164',
    'DIG701': 'MIN1160',
    'DIG702': 'MIN1161',
    'DIG703': 'MIN1162',
    'DIG704': 'MIN1163',
    'DIG705': 'MIN1164',
    'DONMAR-LSA01-001': 'MIN1804',
    'DONMAR-LSA01-001IN1804': 'MIN1804',
    'DOS_001': 'MIN279',
    'DOS_002': 'MIN282',
    'DVO_001': 'MIN2009',
    'DVO_002': 'MIN1053',
    'DVO_003': 'MIN832',
    'DVO_005': 'MIN691',
    'DVO_006': 'MIN697',
    'DVO_007': 'MIN1110',
    'DVO_008': 'MIN1228',
    'DVO_009': 'MIN2532',
    'DVO_010': 'MIN1061',
    'DVO_011': 'MIN154',
    'DVO_013': 'MIN4680',
    'DVO_014': 'MIN1057',
    'DVO_016': 'MIN174',
    'DVO_018': 'MIN1034',
    'DVO_019': 'MIN187',
    'DVO_020': 'MIN1100',
    'DVO_021': 'MIN2',
    'DVO_022': 'MIN1522',
    'DVO_023': 'MIN1055',
    'DVO_024': 'MIN696',
    'DVO_025': 'MIN190',
    'DVO_026': 'MIN165',
    'DVO_027': 'MIN1084',
    'DVO_028': 'MIN1054',
    'DVO_029': 'MIN616',
    'DVO_030': 'MIN1428',
    'DVO_031': 'MIN2457',
    'DVO_032': 'MIN2424',
    'DVO_033': 'MIN1140',
    'DVO_034': 'MIN207',
    'DVO_035': 'MIN1074',
    'DVO_036': 'MIN2433',
    'DVO_038': 'MIN1114',
    'DVO_040': 'MIN1151',
    'DVO_041': 'MIN1298',
    'DVO_047': 'MIN1388',
    'DVO_048': 'MIN162',
    'DVO_049': 'MIN169',
    'DVO_051': 'MIN1985',
    'DVO_052': 'MIN617',
    'DVO_055': 'MIN686',
    'DVO_060': 'MIN694',
    'DVO_061': 'MIN1804',
    'DVO_062': 'MIN3826',
    'DVO_063': 'MIN3292',
    'DVO_064': 'MIN3291',
    'DVO_065': 'MIN168',
    'DVO_066': 'MIN3158',
    'DVO_067': 'MIN1227',
    'DVO_068': 'MIN1208',
    'DVO_069': 'MIN690',
    'DVO_070': 'MIN192',
    'DVO_072': 'MIN205',
    'DVO_074': 'MIN604',
    'DVO_075': 'MIN191',
    'DVO_076': 'MIN1601',
    'DVO_077': 'MIN1698',
    'DVO_078': 'MIN1699',
    'DVO_079': 'MIN1609',
    'DVO_080': 'MIN1603',
    'DVO_081': 'MIN202',
    'DVO_082': 'MIN1078',
    'DVO_083': 'MIN619',
    'DVO_085': 'MIN2430',
    'DVO_086': 'MIN1108',
    'DVO_088': 'MIN909',
    'DVO_089': 'MIN698',
    'DVO_090': 'MIN650',
    'DVO_091': 'MIN1309',
    'DVO_092': 'MIN1684',
    'DVO_093': 'MIN195',
    'DVO_094': 'MIN2449',
    'DVO_095': 'MIN1659',
    'DVO_096': 'MIN2468',
    'DVO_105': 'MIN4725',
    'DVO_608': 'MIN1023',
    'DVO_708': 'MIN1739',
    'DVO_733': 'MIN1176',
    'DVO_741': 'MIN1182',
    'DVO_742': 'MIN1346',
    'DVO_745': 'MIN1403',
    'DVO_910': 'MIN1924',
    'DVO_918': 'MIN1873',
    'DVO009': 'MIN2532',
    'DVO01M01': 'MIN2009',
    'DVO102': 'MIN1377',
    'DVO600': 'MIN832',
    'DVO601': 'MIN1015',
    'DVO603': 'MIN1016',
    'DVO607': 'MIN1022',
    'DVO608': 'MIN1023',
    'DVO615': 'MIN1024',
    'DVO702': 'MIN1165',
    'DVO704': 'MIN1166',
    'DVO706': 'MIN1167',
    'DVO708': 'MIN1739',
    'DVO709': 'MIN1275',
    'DVO710': 'MIN1168',
    'DVO711': 'MIN1169',
    'DVO712': 'MIN1276',
    'DVO717': 'MIN1170',
    'DVO728': 'MIN1174',
    'DVO729': 'MIN1173',
    'DVO733': 'MIN1176',
    'DVO736': 'MIN1178',
    'DVO738': 'MIN1179',
    'DVO739': 'MIN1180',
    'DVO740': 'MIN1181',
    'DVO741': 'MIN1182',
    'DVO742': 'MIN1346',
    'DVO745': 'MIN1403',
    'DVO746': 'MIN1444',
    'DVO748': 'MIN2857',
    'DVO749': 'MIN2860',
    'ESR_001': 'MIN2234',
    'ESR_002': 'MIN655',
    'GSN_001': 'MIN758',
    'GSN_002': 'MIN756',
    'GSN_003': 'MIN759',
    'GSN_004': 'MIN1548',
    'GSN_005': 'MIN438',
    'GSN_006': 'MIN783',
    'GSN_008': 'MIN766',
    'GSN_009': 'MIN764',
    'GSN_010': 'MIN426',
    'GSN_012': 'MIN407',
    'GSN_014': 'MIN430',
    'GSN_015': 'MIN1547',
    'GSN_016': 'MIN1446',
    'GSN_017': 'MIN423',
    'GSN_019': 'MIN428',
    'GSN_602': 'MIN1035',
    'GSN_910': 'MIN1853',
    'GSN_911': 'MIN1562',
    'GSN602': 'MIN1035',
    'GSN605': 'MIN1038',
    'GSN705': 'MIN758',
    'GSN706': 'MIN1277',
    'GSN707': 'MIN1278',
    'GUL_001': 'MIN2205',
    'ISU_001': 'MIN446',
    'KBC_001': 'MIN744',
    'KPW_001': 'MIN834',
    'KRN_001': 'MIN763',
    'KRN_002': 'MIN1070',
    'KRN_003': 'MIN1612',
    'KRN702': 'MIN1359',
    'LBU_001': 'MIN383',
    'LPN_001': 'MIN699',
    'MAI_001': 'MIN1263',
    'MAT_001': 'MIN167',
    'MCO_001': 'MIN667',
    'MDS_001': 'MIN845',
    'MDS_003': 'MIN1299',
    'MLN_001': 'MIN1272',
    'MLU_001': 'MIN397',
    'MLU_002': 'MIN873',
    'MNK_001': 'MIN669',
    'MON_001': 'MIN851',
    'MRU_001': 'MIN126',
    'MTI_001': 'MIN216',
    'MTI_002': 'MIN2269',
    'MTI_003': 'MIN2780',
    'MTI_005': 'MIN238',
    'MTI_006': 'MIN1783',
    'MTI_701': 'MIN1464',
    'MTI_702': 'MIN1465',
    'MTI701': 'MIN1464',
    'MTI702': 'MIN1465',
    'MTL_001': 'MIN745',
    'NBN_001': 'MIN670',
    'PANABO-LSA01-001': 'MIN856',
    'PDA_001': 'MIN689',
    'PGK_001': 'MIN390',
    'PGK_002': 'MIN377',
    'PIK_001': 'MIN857',
    'PLM_001': 'MIN767',
    'PLM_002': 'MIN410',
    'PLM_003': 'MIN414',
    'PLM_004': 'MIN1843',
    'PLM_005': 'MIN1066',
    'PLM_702': 'MIN1374',
    'PLM701': 'MIN1375',
    'PLM702': 'MIN1374',
    'PNB_001': 'MIN229',
    'PNB_002': 'MIN658',
    'PNB_003': 'MIN148',
    'PNB_004': 'MIN135',
    'PNB_005': 'MIN856',
    'PNB_006': 'MIN1211',
    'PNB_007': 'MIN143',
    'PNB_701': 'MIN1185',
    'PNB_702': 'MIN1340',
    'PNB_703': 'MIN1195',
    'PNB702': 'MIN1340',
    'PNB703': 'MIN1195',
    'PNT_001': 'MIN672',
    'PRN_001': 'MIN711',
    'PRN_002': 'MIN710',
    'SCD_001': 'MIN861',
    'SCD_002': 'MIN176',
    'SCD_003': 'MIN679',
    'SCD_004': 'MIN817',
    'SFA_001': 'MIN286',
    'STM_001': 'MIN676',
    'TAN_001': 'MIN437',
    'TCR_002': 'MIN862',
    'TGM_001': 'MIN1721',
    'TGM_002': 'MIN1538',
    'TGM_003': 'MIN1226',
    'TGM_004': 'MIN1763',
    'TGM_005': 'MIN677',
    'TGM_006': 'MIN228',
    'TGM_007': 'MIN1780',
    'TGM_008': 'MIN2484',
    'TGM_009': 'MIN149',
    'TGM_010': 'MIN138',
    'TGM_011': 'MIN142',
    'TGM_012': 'MIN222',
    'TGM_014': 'MIN1652',
    'TGM_017': 'MIN152',
    'TGM_702': 'MIN1344',
    'TGM_704': 'MIN1341',
    'TGM_705': 'MIN1284',
    'TGM702': 'MIN1344',
    'TGM704': 'MIN1341',
    'TGM705': 'MIN1284',
    'TUL_001': 'MIN396',
    'TUP_001': 'MIN773'
}


# ─────────────────────────────────────────────────────────────────────────────
# SALES AREA + PROVINCE LOOKUP
# Source: NAP_UNIQUE_CHAR.xlsx — keyed by NAP ID prefix (e.g. BGN, DVO)
# ─────────────────────────────────────────────────────────────────────────────

NAP_AREA_LOOKUP = {
    'BGN': 'AREA 7-1',
    'BNY': 'AREA 7-1',
    'CMP': 'AREA 7-1',
    'CRA': 'AREA 7-1',
    'CRN': 'AREA 7-1',
    'CTL': 'AREA 7-1',
    'DIG': 'AREA 7-1',
    'DONMAR': 'AREA 7-1',
    'DVO': 'AREA 7-1',
    'MAI': 'AREA 7-1',
    'MAT': 'AREA 7-1',
    'MCO': 'AREA 7-1',
    'MNK': 'AREA 7-1',
    'MON': 'AREA 7-1',
    'MTI': 'AREA 7-1',
    'NBN': 'AREA 7-1',
    'PANABO': 'AREA 7-1',
    'PDA': 'AREA 7-1',
    'PNB': 'AREA 7-1',
    'PNT': 'AREA 7-1',
    'SCD': 'AREA 7-1',
    'STM': 'AREA 7-1',
    'TGM': 'AREA 7-1',
    'LPN': 'AREA 7-1',
    'CMN': 'AREA 7-3',
    'CTT': 'AREA 7-3',
    'DOS': 'AREA 7-3',
    'KBC': 'AREA 7-3',
    'KPW': 'AREA 7-3',
    'LBU': 'AREA 7-3',
    'MDS': 'AREA 7-3',
    'MLN': 'AREA 7-3',
    'MTL': 'AREA 7-3',
    'PRN': 'AREA 7-3',
    'SFA': 'AREA 7-3',
    'TUL': 'AREA 7-3',
    'PGK': 'AREA 7-3',
    'PIK': 'AREA 7-3',
    'BNG': 'AREA 7-4',
    'ESR': 'AREA 7-4',
    'GSN': 'AREA 7-4',
    'ISU': 'AREA 7-4',
    'KRN': 'AREA 7-4',
    'MLU': 'AREA 7-4',
    'PLM': 'AREA 7-4',
    'TAN': 'AREA 7-4',
    'TCR': 'AREA 7-4',
    'TUP': 'AREA 7-4'
}

NAP_PROVINCE_LOOKUP = {
    'BGN': 'DAVAO ORIENTAL',
    'BNY': 'DAVAO ORIENTAL',
    'CMP': 'COMPOSTELA VALLEY',
    'CRA': 'DAVAO ORIENTAL',
    'CRN': 'DAVAO DEL NORTE',
    'CTL': 'DAVAO ORIENTAL',
    'DIG': 'DAVAO DEL SUR',
    'DONMAR': 'DAVAO DEL SUR',
    'DVO': 'DAVAO DEL SUR',
    'MAI': 'COMPOSTELA VALLEY',
    'MAT': 'DAVAO DEL SUR',
    'MCO': 'COMPOSTELA VALLEY',
    'MNK': 'COMPOSTELA VALLEY',
    'MON': 'COMPOSTELA VALLEY',
    'MTI': 'DAVAO ORIENTAL',
    'NBN': 'COMPOSTELA VALLEY',
    'PANABO': 'DAVAO DEL NORTE',
    'PDA': 'DAVAO DEL SUR',
    'PNB': 'DAVAO DEL NORTE',
    'PNT': 'COMPOSTELA VALLEY',
    'SCD': 'DAVAO DEL SUR',
    'STM': 'DAVAO DEL NORTE',
    'TGM': 'DAVAO DEL NORTE',
    'LPN': 'DAVAO ORIENTAL',
    'CMN': 'COTABATO(NORTH COTABATO)',
    'CTT': 'MAGUINDANAO',
    'DOS': 'MAGUINDANAO',
    'KBC': 'COTABATO(NORTH COTABATO)',
    'KPW': 'COTABATO(NORTH COTABATO)',
    'LBU': 'COTABATO(NORTH COTABATO)',
    'MDS': 'COTABATO(NORTH COTABATO)',
    'MLN': 'COTABATO(NORTH COTABATO)',
    'MTL': 'COTABATO(NORTH COTABATO)',
    'PRN': 'MAGUINDANAO',
    'SFA': 'MAGUINDANAO',
    'TUL': 'COTABATO(NORTH COTABATO)',
    'PGK': 'COTABATO(NORTH COTABATO)',
    'PIK': 'COTABATO(NORTH COTABATO)',
    'BNG': 'SOUTH COTABATO',
    'ESR': 'SULTAN KUDARAT',
    'GSN': 'SOUTH COTABATO',
    'ISU': 'SULTAN KUDARAT',
    'KRN': 'SOUTH COTABATO',
    'MLU': 'SARANGANI',
    'PLM': 'SOUTH COTABATO',
    'TAN': 'SOUTH COTABATO',
    'TCR': 'SULTAN KUDARAT',
    'TUP': 'SOUTH COTABATO'
}


def build_geo_lookup(ref_bytes: bytes) -> dict:
    """Build NAP ID → (city, brgy, location_tagging) from reference Excel."""
    df = pd.read_excel(io.BytesIO(ref_bytes))
    lookup = {}
    for _, row in df.iterrows():
        nap = str(row.get('NAP ID', '')).strip()
        if not nap:
            continue
        city = str(row.get('CITY_NAME', '')).strip()   if pd.notna(row.get('CITY_NAME'))        else ''
        brgy = str(row.get('BRGY_NAME', '')).strip()   if pd.notna(row.get('BRGY_NAME'))        else ''
        loc  = str(row.get('LOCATION TAGGING', '')).strip() if pd.notna(row.get('LOCATION TAGGING')) else ''
        lookup[nap] = (city, brgy, loc)
    return lookup


def get_nap_prefix(nap_id: str) -> str:
    """Extract the prefix from NAP ID (e.g. BGN from BGN_001_L901_N01)."""
    return nap_id.strip().split('_')[0].upper() if nap_id else ''


def get_sales_area(nap_id: str) -> str:
    """Look up Sales Area from NAP ID prefix."""
    prefix = get_nap_prefix(nap_id)
    return NAP_AREA_LOOKUP.get(prefix, '')


def get_province(nap_id: str) -> str:
    """Look up Province from NAP ID prefix."""
    prefix = get_nap_prefix(nap_id)
    return NAP_PROVINCE_LOOKUP.get(prefix, '')



def get_tech(cabinet: str) -> str:
    """Determine Tech type from Cabinet name.
    - Blank cabinet           → blank
    - Contains 'LSA'          → GPON (LSA overrides -M)
    - Contains '-M' (no LSA)  → ADSL/VDSL
    - Everything else         → GPON
    """
    if not cabinet:
        return ''
    if 'LSA' in cabinet.upper():
        return 'GPON'
    if '-M' in cabinet:
        return 'ADSL/VDSL'
    return 'GPON'


def get_pla_id(cabinet: str) -> str:
    """Look up PLA ID from Cabinet name.
    Tries full prefix match first (e.g. BGN_001 from BGN_001_GPONA_01),
    then falls back to shorter matches."""
    if not cabinet:
        return ''
    parts = cabinet.strip().split('_')
    # Try first two parts: e.g. BGN_001
    prefix = '_'.join(parts[:2])
    if prefix in PLA_ID_LOOKUP:
        return PLA_ID_LOOKUP[prefix]
    # Try first part only: e.g. BGN
    prefix = parts[0]
    if prefix in PLA_ID_LOOKUP:
        return PLA_ID_LOOKUP[prefix]
    return ''


# ─────────────────────────────────────────────────────────────────────────────
# CORE LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def parse_raw(raw: str) -> dict | None:
    fields = raw.split(';')
    n = len(fields)
    if n < TRAILING_COLS + 6:
        return None
    tail = fields[n - TRAILING_COLS:]
    return {
        '_cabinet':        tail[4].strip(),  # OLT ID (Cabinet); empty if not available
        '_nap_id':         fields[1].strip(),
        '_status':         fields[2].strip(),
        '_lat':            tail[0].strip(),
        '_lon':            tail[1].strip(),
        '_discovered':     tail[2].strip(),
        '_ports_total':    tail[6].strip(),
        '_ports_assigned': tail[7].strip(),
        '_ports_reserved': tail[8].strip(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# JUNK ROW DETECTION
# Skips metadata headers and summary footers that appear in the raw export
# ─────────────────────────────────────────────────────────────────────────────

JUNK_PATTERNS = [
    re.compile(r'^\s*nap facility summary report', re.IGNORECASE),  # Row 1
    re.compile(r'^\s*object\s*:', re.IGNORECASE),                   # Row 2: "Object: ;All reports"
    re.compile(r'^\s*specified report', re.IGNORECASE),             # Row 3
    re.compile(r'^\s*nap name pattern', re.IGNORECASE),             # Row 4
    re.compile(r'^\s*report results', re.IGNORECASE),               # Row 5
    re.compile(r'^\s*\d+\s+rows?\s+are\s+displayed', re.IGNORECASE),  # Footer: "299566 rows are displayed"
    re.compile(r'^\s*location\s*$', re.IGNORECASE),                 # Source header row "Location;NAP ID;..."
]


def is_junk_row(raw: str) -> bool:
    """Returns True if the row is a metadata/summary line that should be skipped."""
    first_field = raw.split(';')[0].strip()
    return any(p.match(first_field) for p in JUNK_PATTERNS)


# ─────────────────────────────────────────────────────────────────────────────
# NAP ID PREFIX FILTER
# Only keep rows where NAP ID starts with one of these prefixes
# ─────────────────────────────────────────────────────────────────────────────

# PREFIX → TERRITORY mapping
# To add Territory 8 in the future, add a new dict entry below
PREFIX_TERRITORY = {
    # ── Territory 7 ──────────────────────────────────────────────────────────
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
    'NBN': 'TERRITORY 7', 'PANABO': 'TERRITORY 7', 'DONMAR': 'TERRITORY 7', 'PDA': 'TERRITORY 7',
    'PGK': 'TERRITORY 7', 'PIK': 'TERRITORY 7', 'PLM': 'TERRITORY 7',
    'PNB': 'TERRITORY 7', 'PNT': 'TERRITORY 7', 'PRN': 'TERRITORY 7',
    'SCD': 'TERRITORY 7', 'SFA': 'TERRITORY 7', 'STM': 'TERRITORY 7',
    'TAN': 'TERRITORY 7', 'TCR': 'TERRITORY 7', 'TGM': 'TERRITORY 7',
    'TUL': 'TERRITORY 7', 'TUP': 'TERRITORY 7',

    # ── Territory 8 (add future prefixes here) ───────────────────────────────
    # 'XYZ': 'TERRITORY 8',
}


def get_territory(nap_id: str) -> str:
    """Returns the territory for a given NAP ID based on its prefix.
    Strict match: prefix must be followed by _ or - or end of string.
    e.g. MON matches MON_001 but NOT MONTAGE.
    """
    nap_upper = nap_id.upper().strip()
    for prefix, territory in PREFIX_TERRITORY.items():
        p = prefix.upper()
        if nap_upper == p or nap_upper.startswith(p + '_') or nap_upper.startswith(p + '-'):
            return territory
    return ''
''


def apply_filters(rec: dict) -> bool:
    """Keep only rows whose NAP ID prefix is in PREFIX_TERRITORY."""
    return get_territory(rec['_nap_id']) != ""


def to_int(val: str) -> int | str:
    """Convert to int if possible, else return original string."""
    try:
        return int(val)
    except (ValueError, TypeError):
        return val

def to_coord(val: str) -> str:
    """Keep coordinates as original string to preserve full precision.
    Float64 only has ~15-17 significant digits which can silently drop
    the last digit for long coordinate strings like 7.1034221285231149."""
    return val.strip() if val.strip() else ''



def strip_suffix(nap_id: str) -> str:
    """Remove trailing single letter suffix from NAP ID (e.g. N01A → N01, N01B → N01)."""
    return re.sub(r'[A-Za-z]$', '', nap_id)


def calc_utilization(pa: int | str, pt: int | str) -> float | str:
    """Returns utilization as a decimal (e.g. 0.12) for Excel percentage formatting."""
    try:
        t = int(pt)
        a = int(pa)
        return 0.0 if t == 0 else round(a / t, 4)
    except (ValueError, ZeroDivisionError):
        return ''



def merge_duplicates(all_recs: list, geo_lookup: dict) -> list:
    """
    Merge rows with the same base NAP ID (after stripping A/B suffix).
    - Ports Assigned, Reserved, Total → summed
    - UTILIZATION → recalculated after summing
    - All other fields → kept from first occurrence
    Returns a list of merged output rows.
    """
    from collections import OrderedDict

    merged = OrderedDict()  # base_nap_id → merged rec

    for rec in all_recs:
        base = strip_suffix(rec['_nap_id'])

        if base not in merged:
            # First occurrence — store as-is with base NAP ID
            merged[base] = {
                '_cabinet':          rec['_cabinet'],
                '_nap_id':           base,
                '_discovered':       rec['_discovered'],
                '_lat':              rec['_lat'],
                '_lon':              rec['_lon'],
                '_ports_assigned':   to_int(rec['_ports_assigned']),
                '_ports_reserved':   to_int(rec['_ports_reserved']),
                '_ports_total':      to_int(rec['_ports_total']),
                '_territory':        get_territory(rec['_nap_id']),
                '_first_ports_total': to_int(rec['_ports_total']),  # remember first ports total
            }
        else:
            # Duplicate — apply merge rules:
            # Same Ports Total (e.g. 16+16) → sum Assigned only, keep Total as is
            # Different Ports Total (e.g. 16+8) → sum both Assigned and Total
            existing     = merged[base]
            new_pt       = to_int(rec['_ports_total'])
            new_pa       = to_int(rec['_ports_assigned'])
            new_pr       = to_int(rec['_ports_reserved'])
            existing_pt  = existing['_ports_total']    if isinstance(existing['_ports_total'],    int) else 0
            existing_pa  = existing['_ports_assigned'] if isinstance(existing['_ports_assigned'], int) else 0
            existing_pr  = existing['_ports_reserved'] if isinstance(existing['_ports_reserved'], int) else 0
            first_pt     = existing['_first_ports_total'] if isinstance(existing['_first_ports_total'], int) else 0

            existing['_ports_assigned'] = existing_pa + (new_pa if isinstance(new_pa, int) else 0)
            existing['_ports_reserved'] = existing_pr + (new_pr if isinstance(new_pr, int) else 0)

            # Rule: only keep Ports Total unchanged when BOTH are 16
            # All other combinations (16+8, 8+8, etc.) → sum Ports Total
            if isinstance(new_pt, int) and new_pt == 16 and first_pt == 16:
                existing['_ports_total'] = 16  # 16+16 → keep as 16
            else:
                existing['_ports_total'] = existing_pt + (new_pt if isinstance(new_pt, int) else 0)

    # Convert merged dict to output rows
    result = []
    for base, m in merged.items():
        pa = m['_ports_assigned']
        pt = m['_ports_total']
        util = 0.0 if pt == 0 else round(pa / pt, 4) if isinstance(pa, int) and isinstance(pt, int) else ''
        result.append([
            m['_cabinet'],
            m['_nap_id'],
            m['_discovered'],
            get_pla_id(m['_cabinet']), get_tech(m['_cabinet']),  # PLA ID and Tech auto-filled
            pa,                      # Ports Assigned (summed)
            m['_ports_reserved'],    # Ports Reserved (summed)
            pt,                      # Ports Total (summed)
            util,                    # UTILIZATION recalculated
            to_coord(m['_lat']),
            to_coord(m['_lon']),
            get_sales_area(m['_nap_id']),
            m['_territory'],
            geo_lookup.get(m['_nap_id'], ('', '', ''))[1],   # BRGY_NAME
            geo_lookup.get(m['_nap_id'], ('', '', ''))[0],   # CITY_NAME
            get_province(m['_nap_id']),
            geo_lookup.get(m['_nap_id'], ('', '', ''))[2],   # LOCATION TAGGING
        ])
    return result


def process_and_build(file_bytes: bytes, ref_bytes: bytes | None, progress_bar) -> tuple:
    """
    Parse uploaded CSV, merge duplicate NAP IDs, and write into an xlsxwriter workbook.
    Returns (xlsx_bytes, total_read, total_written, total_skipped, preview_rows)
    """
    # Build geo lookup from reference file if provided
    geo_lookup = build_geo_lookup(ref_bytes) if ref_bytes else {}

    use_chunking = len(file_bytes) > SMALL_FILE_THRESHOLD

    # ── Set up xlsxwriter workbook ────────────────────────────────────────────
    buf = io.BytesIO()
    wb  = xlsxwriter.Workbook(buf, {'constant_memory': use_chunking})
    ws  = wb.add_worksheet('NAP Data')

    # ── Define formats ────────────────────────────────────────────────────────
    base = {'font_name': 'Arial', 'font_size': 10, 'align': 'left', 'valign': 'vcenter'}

    fmt_yellow  = wb.add_format({**base, 'bold': True, 'bg_color': '#FFFF00'})
    fmt_green   = wb.add_format({**base, 'bold': True, 'bg_color': '#92D050'})
    fmt_white   = wb.add_format({**base, 'bold': True})
    fmt_data    = wb.add_format({**base})
    fmt_pct     = wb.add_format({**base, 'num_format': '0%'})  # e.g. 12%
    fmt_coord   = wb.add_format({**base})                       # lat/lon stored as text, no rounding

    # ── Set column widths and default row height ──────────────────────────────
    for c_idx in range(len(OUTPUT_COLS)):
        ws.set_column(c_idx, c_idx, 21)
    ws.set_default_row(20)

    # ── Write header row ──────────────────────────────────────────────────────
    ws.set_row(0, 20)
    for c_idx, col_name in enumerate(OUTPUT_COLS):
        if col_name in YELLOW_COLS:
            fmt = fmt_yellow
        elif col_name in GREEN_COLS:
            fmt = fmt_green
        else:
            fmt = fmt_white
        ws.write(0, c_idx, col_name, fmt)

    # ── Column index lookups for format selection ─────────────────────────────
    util_idx = OUTPUT_COLS.index('UTILIZATION')
    lat_idx  = OUTPUT_COLS.index('Latitude')
    lon_idx  = OUTPUT_COLS.index('Longitude')

    def write_row(excel_row: int, out_row: list):
        """Write one data row using the correct format per column type."""
        for c_idx, val in enumerate(out_row):
            if c_idx == util_idx:
                ws.write(excel_row, c_idx, val, fmt_pct)
            elif c_idx in (lat_idx, lon_idx):
                ws.write_string(excel_row, c_idx, val, fmt_coord)  # write_string preserves full precision
            else:
                ws.write(excel_row, c_idx, val, fmt_data)

    # ── Stream through file ───────────────────────────────────────────────────
    text   = file_bytes.decode('utf-8-sig', errors='replace')
    reader = csv.reader(io.StringIO(text))

    total_read = total_written = total_skipped = 0
    all_recs   = []   # collect all valid recs for merging

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

    # ── Merge duplicates ──────────────────────────────────────────────────────
    progress_bar.progress(0.7, text="Merging duplicate NAP IDs...")
    merged_rows = merge_duplicates(all_recs, geo_lookup)
    total_written = len(merged_rows)

    # ── Write merged rows to Excel ────────────────────────────────────────────
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

    progress_bar.progress(1.0, text="✅ Finalizing Excel file...")
    wb.close()
    buf.seek(0)

    return buf.getvalue(), total_read, total_written, total_skipped, preview_rows


# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="NAP Data Converter",
    page_icon="📡",
    layout="centered",
)

st.title("📡 NAP Data Converter")
st.markdown("Upload your raw NAP CSV file and download a clean, structured Excel file.")
st.divider()

# ── File Upload ───────────────────────────────────────────────────────────────
st.subheader("📂 Upload Files")

col1, col2 = st.columns(2)
with col1:
    uploaded = st.file_uploader(
        "① NAP CSV File (required)",
        type=["csv"],
        help="The raw semicolon-delimited CSV exported from your system.",
    )
with col2:
    ref_file = st.file_uploader(
        "② NAP GEO Reference (optional)",
        type=["xlsx"],
        help="Upload NAP_GEO_REFERENCE.xlsx to auto-fill BRGY_NAME, CITY_NAME and LOCATION TAGGING.",
    )

if ref_file and not uploaded:
    st.warning("⚠️ Please also upload a NAP CSV file to run the conversion.")

if uploaded:
    st.success(f"✅ CSV uploaded: **{uploaded.name}** ({uploaded.size / 1_000_000:.2f} MB)")
    if ref_file:
        st.success(f"✅ Reference uploaded: **{ref_file.name}** — BRGY, CITY and LOCATION TAGGING will be auto-filled.")
    else:
        st.info("ℹ️ No reference file — BRGY_NAME, CITY_NAME and LOCATION TAGGING will be blank.")

    if st.button("🚀 Convert", use_container_width=True, type="primary"):

        progress_bar = st.progress(0, text="Starting...")

        file_bytes = uploaded.read()
        ref_bytes  = ref_file.read() if ref_file else None
        xlsx_bytes, total_read, total_written, total_skipped, preview_rows = process_and_build(
            file_bytes, ref_bytes, progress_bar
        )

        if total_written == 0:
            st.error("No valid rows found in the file. Please check your CSV and try again.")
        else:
            # ── Summary ───────────────────────────────────────────────────────
            st.divider()
            st.subheader("📊 Summary")
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Rows Read",   f"{total_read:,}")
            m2.metric("Rows Written",      f"{total_written:,}")
            m3.metric("Rows Filtered Out", f"{total_skipped:,}")

            # ── Preview ───────────────────────────────────────────────────────
            st.subheader("👀 Preview (first 50 rows)")
            df_preview = pd.DataFrame(preview_rows, columns=OUTPUT_COLS)
            df_preview['UTILIZATION'] = df_preview['UTILIZATION'].apply(
                lambda x: f"{round(x * 100)}%" if isinstance(x, float) else x
            )
            st.dataframe(df_preview, use_container_width=True)

            # ── Download ──────────────────────────────────────────────────────
            stem = uploaded.name.rsplit('.', 1)[0] if '.' in uploaded.name else uploaded.name
            output_name = stem + '_cleaned.xlsx'
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
    st.info("👆 Upload a CSV file above to get started.")
