import csv
import re
import io
import json
from datetime import date, datetime
from collections import OrderedDict

import pandas as pd
import streamlit as st
import xlsxwriter
import plotly.express as px
import plotly.graph_objects as go

# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE CONNECTION
# ─────────────────────────────────────────────────────────────────────────────

def get_supabase():
    try:
        from supabase import create_client
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
        return create_client(url, key)
    except Exception as e:
        st.error(f"Could not connect to Supabase: {e}")
        st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

TRAILING_COLS = 12
ADMIN_PASSWORD = "admin123"

JUNK_PATTERNS = [
    re.compile(r'^\s*nap facility summary report', re.IGNORECASE),
    re.compile(r'^\s*object\s*:', re.IGNORECASE),
    re.compile(r'^\s*specified report', re.IGNORECASE),
    re.compile(r'^\s*nap name pattern', re.IGNORECASE),
    re.compile(r'^\s*report results', re.IGNORECASE),
    re.compile(r'^\s*\d+\s+rows?\s+are\s+displayed', re.IGNORECASE),
    re.compile(r'^\s*location\s*$', re.IGNORECASE),
]

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
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def is_junk_row(raw: str) -> bool:
    first = raw.split(';')[0].strip()
    return any(p.match(first) for p in JUNK_PATTERNS)

def get_territory(nap_id: str) -> str:
    nap_upper = nap_id.upper().strip()
    for prefix in SORTED_PREFIXES:
        p = prefix.upper()
        if nap_upper == p:
            return PREFIX_TERRITORY[prefix]
        if len(nap_upper) > len(p):
            nc = nap_upper[len(p)]
            if nc in ('_', '-') or nc.isdigit() or nc == 'L':
                if nap_upper.startswith(p):
                    return PREFIX_TERRITORY[prefix]
    return ''

def strip_suffix(nap_id: str) -> str:
    return re.sub(r'(?<=\d)[A-Za-z]$', '', nap_id)

def to_int(val) -> int | str:
    try:
        return int(val)
    except (ValueError, TypeError):
        return val

def calc_utilization(pa, pt) -> float:
    try:
        t = int(pt)
        a = int(pa)
        return 0.0 if t == 0 else round(a / t, 4)
    except (ValueError, ZeroDivisionError):
        return 0.0

def parse_raw(raw: str) -> dict | None:
    fields = raw.split(';')
    n = len(fields)
    if n < TRAILING_COLS + 6:
        return None
    tail = fields[n - TRAILING_COLS:]
    nap_id = fields[1].strip()
    cabinet = tail[4].strip()
    if ' ' in nap_id:
        return None
    return {
        '_nap_id':         nap_id,
        '_cabinet':        cabinet,
        '_discovered':     tail[2].strip(),
        '_lat':            tail[0].strip(),
        '_lon':            tail[1].strip(),
        '_ports_total':    tail[6].strip(),
        '_ports_assigned': tail[7].strip(),
        '_ports_reserved': tail[8].strip(),
    }

def parse_csv_utilization(file_bytes: bytes) -> list[dict]:
    """Parse CSV and return list of {nap_id, ports_assigned, ports_reserved, ports_total, utilization}."""
    text   = file_bytes.decode('utf-8-sig', errors='replace')
    reader = csv.reader(io.StringIO(text))
    all_recs = []

    for i, row in enumerate(reader):
        if i == 0 or not row:
            continue
        raw = ''.join(c for c in row if c.strip())
        if not raw.strip() or is_junk_row(raw):
            continue
        rec = parse_raw(raw)
        if rec is None:
            continue
        if not get_territory(rec['_nap_id']):
            continue
        all_recs.append(rec)

    # Merge duplicates
    merged = OrderedDict()
    for rec in all_recs:
        base = strip_suffix(rec['_nap_id'])
        if base not in merged:
            merged[base] = {
                'nap_id':          base,
                'ports_assigned':  to_int(rec['_ports_assigned']),
                'ports_reserved':  to_int(rec['_ports_reserved']),
                'ports_total':     to_int(rec['_ports_total']),
                '_first_pt':       to_int(rec['_ports_total']),
            }
        else:
            e      = merged[base]
            new_pt = to_int(rec['_ports_total'])
            new_pa = to_int(rec['_ports_assigned'])
            new_pr = to_int(rec['_ports_reserved'])
            e['_ports_assigned'] = (e['ports_assigned'] if isinstance(e['ports_assigned'], int) else 0) + (new_pa if isinstance(new_pa, int) else 0)
            e['_ports_reserved'] = (e['ports_reserved'] if isinstance(e['ports_reserved'], int) else 0) + (new_pr if isinstance(new_pr, int) else 0)
            first_pt = e['_first_pt'] if isinstance(e['_first_pt'], int) else 0
            if isinstance(new_pt, int) and new_pt == 16 and first_pt == 16:
                e['ports_total'] = 16
            else:
                e['ports_total'] = (e['ports_total'] if isinstance(e['ports_total'], int) else 0) + (new_pt if isinstance(new_pt, int) else 0)
            e['ports_assigned'] = e['_ports_assigned']
            e['ports_reserved'] = e['_ports_reserved']

    results = []
    for base, m in merged.items():
        pa   = m['ports_assigned']  if isinstance(m['ports_assigned'], int)  else 0
        pt   = m['ports_total']     if isinstance(m['ports_total'], int)     else 0
        pr   = m['ports_reserved']  if isinstance(m['ports_reserved'], int)  else 0
        util = calc_utilization(pa, pt)
        results.append({
            'nap_id':          base,
            'ports_assigned':  pa,
            'ports_reserved':  pr,
            'ports_total':     pt,
            'utilization':     util,
            'snapshot_date':   str(date.today()),
        })
    return results


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_dashboard_data(snapshot_date: str) -> pd.DataFrame:
    """Load utilization joined with reference for a given date."""
    sb  = get_supabase()
    res = sb.table('nap_utilization').select(
        'nap_id, ports_assigned, ports_reserved, ports_total, utilization, snapshot_date'
    ).eq('snapshot_date', snapshot_date).execute()

    if not res.data:
        return pd.DataFrame()

    util_df = pd.DataFrame(res.data)

    ref_res = sb.table('nap_reference').select(
        'nap_id, cabinet, pla_id, tech, territory, sales_area, province, city, brgy, location_tag, latitude, longitude'
    ).execute()
    ref_df = pd.DataFrame(ref_res.data) if ref_res.data else pd.DataFrame()

    if ref_df.empty:
        return util_df

    df = util_df.merge(ref_df, on='nap_id', how='left')
    df['utilization_pct'] = (df['utilization'] * 100).round(1)
    return df


@st.cache_data(ttl=300)
def load_available_dates() -> list[str]:
    sb  = get_supabase()
    res = sb.table('nap_utilization').select('snapshot_date').execute()
    if not res.data:
        return []
    dates = sorted(set(r['snapshot_date'] for r in res.data), reverse=True)
    return dates


@st.cache_data(ttl=300)
def load_trend_data(nap_id: str) -> pd.DataFrame:
    sb  = get_supabase()
    res = sb.table('nap_utilization').select(
        'snapshot_date, ports_assigned, ports_total, utilization'
    ).eq('nap_id', nap_id).order('snapshot_date').execute()
    if not res.data:
        return pd.DataFrame()
    df = pd.DataFrame(res.data)
    df['utilization_pct'] = (df['utilization'] * 100).round(1)
    return df


def upsert_utilization(records: list[dict]) -> int:
    sb         = get_supabase()
    batch_size = 500
    total      = 0
    errors     = []
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            sb.table('nap_utilization').upsert(
                batch, on_conflict='nap_id,snapshot_date'
            ).execute()
            total += len(batch)
        except Exception as e:
            errors.append(str(e))
    if errors:
        st.warning(f"Some records could not be saved: {errors[0]}")
    st.cache_data.clear()
    return total


def load_reference_page(search: str = '', page: int = 0, page_size: int = 50) -> tuple[pd.DataFrame, int]:
    sb     = get_supabase()
    query  = sb.table('nap_reference').select('*', count='exact')
    if search:
        query = query.or_(f'nap_id.ilike.%{search}%,cabinet.ilike.%{search}%,city.ilike.%{search}%,brgy.ilike.%{search}%')
    query  = query.range(page * page_size, (page + 1) * page_size - 1)
    res    = query.execute()
    df     = pd.DataFrame(res.data) if res.data else pd.DataFrame()
    total  = res.count if res.count else 0
    return df, total


def update_reference_row(nap_id: str, updates: dict):
    sb = get_supabase()
    updates['updated_at'] = datetime.utcnow().isoformat()
    sb.table('nap_reference').update(updates).eq('nap_id', nap_id).execute()
    st.cache_data.clear()


def bulk_load_reference(ref_bytes: bytes) -> int:
    """Load reference Excel into nap_reference table.
    Supports both 1New_reference_april17.xlsx and NAP_GEO_REFERENCE.xlsx formats.
    """
    df = pd.read_excel(io.BytesIO(ref_bytes))
    df.columns = df.columns.str.strip()

    # Full reference file columns (1New_reference_april17.xlsx)
    col_map_full = {
        'NAP_ID':           'nap_id',
        'Cabinet':          'cabinet',
        'PLA ID':           'pla_id',
        'Tech':             'tech',
        'SALES_TERRITORY':  'territory',
        'SALES_AREA':       'sales_area',
        'PROVINCE_NAME':    'province',
        'CITY_NAME':        'city',
        'BRGY_NAME':        'brgy',
        'LOCATION TAGGING': 'location_tag',
        'DP/NAP LAT':       'latitude',
        'DP/NAP LONG':      'longitude',
    }

    # GEO reference only columns (NAP_GEO_REFERENCE.xlsx)
    col_map_geo = {
        'NAP ID':           'nap_id',
        'CITY_NAME':        'city',
        'BRGY_NAME':        'brgy',
        'LOCATION TAGGING': 'location_tag',
    }

    # Detect which format this is
    if 'NAP_ID' in df.columns or 'Cabinet' in df.columns:
        col_map = col_map_full
    else:
        col_map = col_map_geo

    df   = df.rename(columns=col_map)
    keep = [c for c in col_map.values() if c in df.columns]

    if 'nap_id' not in keep:
        raise ValueError(f"Could not find NAP ID column. Columns found: {df.columns.tolist()}")

    df = df[keep].drop_duplicates(subset='nap_id').fillna('')

    records = df.to_dict('records')
    sb      = get_supabase()
    batch   = 500
    total   = 0
    for i in range(0, len(records), batch):
        sb.table('nap_reference').upsert(
            records[i:i+batch], on_conflict='nap_id'
        ).execute()
        total += len(records[i:i+batch])
    st.cache_data.clear()
    return total


def safe_val(v):
    """Convert any value to a safe type for xlsxwriter — no None, no NaN."""
    if v is None:
        return ''
    try:
        import math
        if isinstance(v, float) and math.isnan(v):
            return ''
    except Exception:
        pass
    return v


def build_excel_report(df: pd.DataFrame) -> bytes:
    """Build full Excel report from dashboard dataframe."""
    # Fill all NaN/None before writing
    df = df.fillna('')

    buf = io.BytesIO()
    wb  = xlsxwriter.Workbook(buf, {'constant_memory': False})
    ws  = wb.add_worksheet('NAP Utilization')

    base       = {'font_name': 'Arial', 'font_size': 10, 'align': 'left', 'valign': 'vcenter'}
    fmt_yellow = wb.add_format({**base, 'bold': True, 'bg_color': '#FFFF00'})
    fmt_green  = wb.add_format({**base, 'bold': True, 'bg_color': '#92D050'})
    fmt_pct    = wb.add_format({**base, 'num_format': '0%'})
    fmt_data   = wb.add_format({**base})

    OUTPUT_COLS = [
        ('NAP ID',           'nap_id',        fmt_yellow),
        ('Cabinet',          'cabinet',        fmt_yellow),
        ('PLA ID',           'pla_id',         fmt_green),
        ('Tech',             'tech',           fmt_green),
        ('Ports Assigned',   'ports_assigned', fmt_yellow),
        ('Ports Reserved',   'ports_reserved', fmt_yellow),
        ('Ports Total',      'ports_total',    fmt_yellow),
        ('UTILIZATION',      'utilization',    fmt_yellow),
        ('SALES_AREA',       'sales_area',     fmt_green),
        ('TERRITORY',        'territory',      fmt_green),
        ('BRGY_NAME',        'brgy',           fmt_green),
        ('CITY_NAME',        'city',           fmt_green),
        ('PROVINCE_NAME',    'province',       fmt_green),
        ('LOCATION TAGGING', 'location_tag',   fmt_green),
        ('Latitude',         'latitude',       fmt_yellow),
        ('Longitude',        'longitude',      fmt_yellow),
        ('Date',             'snapshot_date',  fmt_data),
    ]

    for c, (header, _, fmt) in enumerate(OUTPUT_COLS):
        ws.set_column(c, c, 22)
        ws.write(0, c, header, fmt)

    for r_idx, row in df.iterrows():
        for c, (_, col, fmt) in enumerate(OUTPUT_COLS):
            val = safe_val(row.get(col, ''))
            if col == 'utilization':
                try:
                    ws.write(r_idx + 1, c, float(val) if val != '' else 0.0, fmt_pct)
                except Exception:
                    ws.write(r_idx + 1, c, 0.0, fmt_pct)
            elif col in ('ports_assigned', 'ports_reserved', 'ports_total'):
                try:
                    ws.write(r_idx + 1, c, int(val) if val != '' else 0, fmt_data)
                except Exception:
                    ws.write(r_idx + 1, c, 0, fmt_data)
            else:
                ws.write(r_idx + 1, c, str(val), fmt_data)

    wb.close()
    buf.seek(0)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT APP
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title='NAP Utilization Dashboard',
    page_icon='📡',
    layout='wide',
)

st.title('📡 NAP Utilization Dashboard')
st.caption('Territory 7 · Real-time port utilization tracking')
st.divider()

tabs = st.tabs(['Dashboard', 'Daily Upload', 'Reference Data', 'Admin'])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
with tabs[0]:
    available_dates = load_available_dates()

    if not available_dates:
        st.info('No data yet. Upload your first CSV in the Daily Upload tab.')
    else:
        col_date, col_area, col_tech, col_search = st.columns([2, 2, 2, 3])

        with col_date:
            selected_date = st.selectbox('Snapshot date', available_dates)

        df = load_dashboard_data(selected_date)

        with col_area:
            areas = ['All'] + sorted(df['sales_area'].dropna().unique().tolist()) if not df.empty else ['All']
            sel_area = st.selectbox('Sales area', areas)

        with col_tech:
            techs = ['All'] + sorted(df['tech'].dropna().unique().tolist()) if not df.empty else ['All']
            sel_tech = st.selectbox('Tech', techs)

        with col_search:
            search_q = st.text_input('Search NAP ID / City / BRGY', placeholder='Type to filter...')

        if not df.empty:
            # Apply filters
            fdf = df.copy()
            if sel_area != 'All':
                fdf = fdf[fdf['sales_area'] == sel_area]
            if sel_tech != 'All':
                fdf = fdf[fdf['tech'] == sel_tech]
            if search_q:
                mask = (
                    fdf['nap_id'].str.contains(search_q, case=False, na=False) |
                    fdf['city'].str.contains(search_q, case=False, na=False) |
                    fdf['brgy'].str.contains(search_q, case=False, na=False)
                )
                fdf = fdf[mask]

            st.divider()

            # ── Summary metrics ───────────────────────────────────────────────
            total_naps  = len(fdf)
            total_pa    = fdf['ports_assigned'].sum()
            total_pt    = fdf['ports_total'].sum()
            total_pr    = fdf['ports_reserved'].sum()
            avg_util    = calc_utilization(total_pa, total_pt) * 100
            high_util   = len(fdf[fdf['utilization'] >= 0.8])
            full_util   = len(fdf[fdf['utilization'] >= 1.0])

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric('Total NAPs',        f'{total_naps:,}')
            c2.metric('Ports Assigned',    f'{total_pa:,}')
            c3.metric('Ports Reserved',    f'{total_pr:,}')
            c4.metric('Ports Total',       f'{total_pt:,}')
            c5.metric('Avg Utilization',   f'{avg_util:.1f}%')
            c6.metric('High Util (≥80%)',  f'{high_util:,}', delta=f'{full_util} full')

            st.divider()

            # ── Charts ────────────────────────────────────────────────────────
            chart_col1, chart_col2 = st.columns(2)

            with chart_col1:
                st.subheader('Utilization by Sales Area')
                if 'sales_area' in fdf.columns:
                    area_grp = fdf.groupby('sales_area').agg(
                        pa=('ports_assigned', 'sum'),
                        pt=('ports_total', 'sum'),
                        naps=('nap_id', 'count')
                    ).reset_index()
                    area_grp['util_pct'] = (area_grp['pa'] / area_grp['pt'].replace(0, 1) * 100).round(1)
                    fig = px.bar(area_grp, x='sales_area', y='util_pct',
                                 color='util_pct',
                                 color_continuous_scale='RdYlGn_r',
                                 range_color=[0, 100],
                                 labels={'sales_area': 'Area', 'util_pct': 'Utilization %'},
                                 text='util_pct')
                    fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                    fig.update_layout(showlegend=False, height=300,
                                      margin=dict(t=20, b=20, l=20, r=20),
                                      plot_bgcolor='rgba(0,0,0,0)',
                                      paper_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig, use_container_width=True)

            with chart_col2:
                st.subheader('Utilization distribution')
                bins = [0, 0.25, 0.5, 0.75, 0.9, 1.01]
                labels = ['0-25%', '25-50%', '50-75%', '75-90%', '90-100%']
                fdf['util_band'] = pd.cut(fdf['utilization'], bins=bins, labels=labels, include_lowest=True)
                band_counts = fdf['util_band'].value_counts().reindex(labels).fillna(0).reset_index()
                band_counts.columns = ['Band', 'Count']
                fig2 = px.bar(band_counts, x='Band', y='Count',
                              color='Band',
                              color_discrete_sequence=['#2ecc71', '#f1c40f', '#e67e22', '#e74c3c', '#c0392b'],
                              labels={'Band': 'Utilization range', 'Count': 'NAP count'})
                fig2.update_layout(showlegend=False, height=300,
                                   margin=dict(t=20, b=20, l=20, r=20),
                                   plot_bgcolor='rgba(0,0,0,0)',
                                   paper_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig2, use_container_width=True)

            st.divider()

            # ── Trend chart for individual NAP ────────────────────────────────
            st.subheader('NAP trend over time')
            selected_nap = st.selectbox('Select NAP ID', [''] + sorted(fdf['nap_id'].tolist()))
            if selected_nap:
                trend_df = load_trend_data(selected_nap)
                if not trend_df.empty:
                    fig3 = go.Figure()
                    fig3.add_trace(go.Scatter(
                        x=trend_df['snapshot_date'], y=trend_df['utilization_pct'],
                        mode='lines+markers', name='Utilization %',
                        line=dict(color='#3498db', width=2)
                    ))
                    fig3.add_trace(go.Scatter(
                        x=trend_df['snapshot_date'], y=trend_df['ports_assigned'],
                        mode='lines+markers', name='Ports Assigned',
                        line=dict(color='#2ecc71', width=2), yaxis='y2'
                    ))
                    fig3.update_layout(
                        height=300, margin=dict(t=20, b=20, l=20, r=60),
                        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                        yaxis=dict(title='Utilization %', range=[0, 110]),
                        yaxis2=dict(title='Ports', overlaying='y', side='right'),
                        legend=dict(orientation='h', y=-0.2)
                    )
                    st.plotly_chart(fig3, use_container_width=True)

            st.divider()

            # ── Data table ────────────────────────────────────────────────────
            st.subheader(f'NAP data — {len(fdf):,} records')
            display_cols = ['nap_id', 'cabinet', 'city', 'brgy', 'sales_area',
                            'ports_assigned', 'ports_reserved', 'ports_total', 'utilization_pct', 'tech']
            show_cols    = [c for c in display_cols if c in fdf.columns]
            st.dataframe(
                fdf[show_cols].rename(columns={'utilization_pct': 'UTIL %'}),
                use_container_width=True, height=400
            )

            st.divider()
            xlsx = build_excel_report(fdf)
            st.download_button(
                '⬇️ Download full Excel report',
                data=xlsx,
                file_name=f'nap_utilization_{selected_date}.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                use_container_width=True, type='primary'
            )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — DAILY UPLOAD
# ══════════════════════════════════════════════════════════════════════════════
with tabs[1]:
    st.subheader('Daily CSV Upload')
    st.caption('Upload the raw CSV export from your system. Only Ports Assigned, Reserved, Total and Utilization are extracted and saved.')

    uploaded = st.file_uploader('Upload NAP CSV file', type=['csv'])

    if uploaded:
        st.success(f'File uploaded: **{uploaded.name}** ({uploaded.size / 1_000_000:.2f} MB)')

        snap_date = st.date_input('Snapshot date', value=date.today())

        if st.button('Process and save to database', type='primary', use_container_width=True):
            with st.spinner('Parsing CSV...'):
                file_bytes = uploaded.read()
                records    = parse_csv_utilization(file_bytes)

            if not records:
                st.error('No valid Territory 7 rows found in the file.')
            else:
                for r in records:
                    r['snapshot_date'] = str(snap_date)

                with st.spinner(f'Saving {len(records):,} records to Supabase...'):
                    saved = upsert_utilization(records)

                st.success(f'Saved {saved:,} NAP utilization records for **{snap_date}**.')

                # Show summary
                df_preview = pd.DataFrame(records)
                total_pa   = df_preview['ports_assigned'].sum()
                total_pt   = df_preview['ports_total'].sum()
                avg_util   = calc_utilization(total_pa, total_pt) * 100
                c1, c2, c3 = st.columns(3)
                c1.metric('NAPs processed',  f'{len(records):,}')
                c2.metric('Total assigned',  f'{total_pa:,}')
                c3.metric('Avg utilization', f'{avg_util:.1f}%')

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — REFERENCE DATA
# ══════════════════════════════════════════════════════════════════════════════
with tabs[2]:
    st.subheader('Reference data manager')
    st.caption('View and edit NAP reference data — Cabinet, PLA ID, Tech, Territory, Area, Province, BRGY, City, Location Tagging, Coordinates.')

    # Admin lock
    if 'ref_unlocked' not in st.session_state:
        st.session_state.ref_unlocked = False

    if not st.session_state.ref_unlocked:
        st.warning('Admin access required to edit reference data.')
        with st.form('ref_login'):
            pw = st.text_input('Password', type='password')
            if st.form_submit_button('Unlock'):
                if pw == ADMIN_PASSWORD:
                    st.session_state.ref_unlocked = True
                    st.rerun()
                else:
                    st.error('Incorrect password.')
    else:
        search_ref = st.text_input('Search NAP ID, Cabinet, City or BRGY', placeholder='Type to search...')

        if 'ref_page' not in st.session_state:
            st.session_state.ref_page = 0

        df_ref, total_ref = load_reference_page(search_ref, st.session_state.ref_page)

        st.caption(f'Showing {len(df_ref)} of {total_ref:,} entries')

        if not df_ref.empty:
            col_headers = ['nap_id', 'cabinet', 'pla_id', 'tech', 'territory',
                           'sales_area', 'province', 'city', 'brgy', 'location_tag',
                           'latitude', 'longitude']
            editable_cols = ['cabinet', 'pla_id', 'tech', 'territory',
                             'sales_area', 'province', 'city', 'brgy',
                             'location_tag', 'latitude', 'longitude']
            show_cols = [c for c in col_headers if c in df_ref.columns]

            edited = st.data_editor(
                df_ref[show_cols],
                disabled=['nap_id'],
                use_container_width=True,
                height=450,
                key='ref_editor'
            )

            if st.button('Save changes', type='primary'):
                changes = 0
                for i, row in edited.iterrows():
                    orig = df_ref.iloc[i]
                    updates = {}
                    for col in editable_cols:
                        if col in row and str(row[col]) != str(orig.get(col, '')):
                            updates[col] = row[col]
                    if updates:
                        update_reference_row(row['nap_id'], updates)
                        changes += 1
                if changes:
                    st.success(f'Updated {changes} records.')
                else:
                    st.info('No changes detected.')

        # Pagination
        col_prev, col_info, col_next = st.columns([1, 2, 1])
        with col_prev:
            if st.button('Previous') and st.session_state.ref_page > 0:
                st.session_state.ref_page -= 1
                st.rerun()
        with col_info:
            st.caption(f'Page {st.session_state.ref_page + 1} of {max(1, (total_ref // 50) + 1)}')
        with col_next:
            if st.button('Next') and (st.session_state.ref_page + 1) * 50 < total_ref:
                st.session_state.ref_page += 1
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — ADMIN
# ══════════════════════════════════════════════════════════════════════════════
with tabs[3]:
    st.subheader('Admin')

    if 'admin_unlocked' not in st.session_state:
        st.session_state.admin_unlocked = False

    if not st.session_state.admin_unlocked:
        st.warning('Admin access required.')
        with st.form('admin_login'):
            pw = st.text_input('Password', type='password')
            if st.form_submit_button('Unlock'):
                if pw == ADMIN_PASSWORD:
                    st.session_state.admin_unlocked = True
                    st.rerun()
                else:
                    st.error('Incorrect password.')
    else:
        st.info('Use this section to do the initial load of your reference data into Supabase. Only needs to be done once.')

        with st.expander('Load reference data into Supabase', expanded=True):
            ref_upload = st.file_uploader(
                'Upload 1New_reference_april17.xlsx',
                type=['xlsx'],
                key='admin_ref_upload'
            )
            if ref_upload:
                st.info(f'File ready: **{ref_upload.name}** — {ref_upload.size / 1_000_000:.1f} MB')
                if st.button('Load into Supabase', type='primary'):
                    with st.spinner('Loading reference data... this may take a minute.'):
                        ref_bytes = ref_upload.read()
                        total     = bulk_load_reference(ref_bytes)
                    st.success(f'Loaded {total:,} NAP reference records into Supabase.')

        with st.expander('Clear cache'):
            if st.button('Clear data cache'):
                st.cache_data.clear()
                st.success('Cache cleared.')

        with st.expander('Database stats'):
            if st.button('Refresh stats'):
                sb      = get_supabase()
                ref_cnt = sb.table('nap_reference').select('nap_id', count='exact').execute()
                util_cnt= sb.table('nap_utilization').select('nap_id', count='exact').execute()
                c1, c2  = st.columns(2)
                c1.metric('nap_reference rows',    ref_cnt.count  or 0)
                c2.metric('nap_utilization rows',  util_cnt.count or 0)