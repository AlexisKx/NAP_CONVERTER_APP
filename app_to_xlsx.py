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
# CORE LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def parse_raw(raw: str) -> dict | None:
    fields = raw.split(';')
    n = len(fields)
    if n < TRAILING_COLS + 6:
        return None
    tail = fields[n - TRAILING_COLS:]
    return {
        '_cabinet':        fields[0].strip(),
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


def apply_filters(rec: dict) -> bool:
    """No filters — keep all valid data rows including Planned and (AFI) rows."""
    return True


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



def calc_utilization(assigned: str, total: str) -> float | str:
    """Returns utilization as a decimal (e.g. 0.12) for Excel percentage formatting."""
    try:
        t = int(total)
        a = int(assigned)
        return 0.0 if t == 0 else round(a / t, 4)
    except (ValueError, ZeroDivisionError):
        return ''


def to_output_row(rec: dict) -> list:
    return [
        rec['_cabinet'],                 # text
        rec['_nap_id'],                  # text
        rec['_discovered'],              # text
        '', '',                          # PLA ID, Tech — blank
        to_int(rec['_ports_assigned']),  # number
        to_int(rec['_ports_reserved']),  # number
        to_int(rec['_ports_total']),     # number
        calc_utilization(                # decimal → formatted as % in Excel
            rec['_ports_assigned'],
            rec['_ports_total']
        ),
        to_coord(rec['_lat']),           # text — full precision preserved
        to_coord(rec['_lon']),           # text — full precision preserved
        '', '', '', '', '', '',          # GREEN columns — blank
    ]


def process_and_build(file_bytes: bytes, progress_bar) -> tuple:
    """
    Parse uploaded CSV and write into an xlsxwriter workbook.
    Uses chunked writing for large files (>50MB), direct writing for small files.
    Returns (xlsx_bytes, total_read, total_written, total_skipped, preview_rows)
    """
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
    preview_rows = []
    excel_row    = 1   # 0-indexed; row 0 = header
    chunk        = []

    for i, row in enumerate(reader):
        if i == 0 or not row:
            continue

        raw = ''.join(c for c in row if c.strip())
        if not raw.strip():
            continue

        # Skip junk metadata/summary rows silently before counting
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

        out_row = to_output_row(rec)
        chunk.append(out_row)
        total_written += 1

        if len(preview_rows) < 50:
            preview_rows.append(out_row)

        # ── Flush chunk to Excel ───────────────────────────────────────────
        if len(chunk) >= CHUNK_SIZE:
            for out_row in chunk:
                write_row(excel_row, out_row)
                excel_row += 1
            chunk = []
            progress_bar.progress(
                min(total_written / max(total_read, 1), 0.95),
                text=f"Processing... {total_written:,} rows written so far"
            )

    # ── Flush remaining rows ──────────────────────────────────────────────────
    for out_row in chunk:
        write_row(excel_row, out_row)
        excel_row += 1

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
st.subheader("📂 Upload File")
uploaded = st.file_uploader(
    "Upload your NAP CSV file",
    type=["csv"],
    help="The raw semicolon-delimited CSV exported from your system.",
)

if uploaded:
    st.success(f"✅ File uploaded: **{uploaded.name}** ({uploaded.size / 1_000_000:.2f} MB)")

    if st.button("🚀 Convert", use_container_width=True, type="primary"):

        progress_bar = st.progress(0, text="Starting...")

        file_bytes = uploaded.read()
        xlsx_bytes, total_read, total_written, total_skipped, preview_rows = process_and_build(
            file_bytes, progress_bar
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
            output_name = uploaded.name.replace('.csv', '_cleaned.xlsx')
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