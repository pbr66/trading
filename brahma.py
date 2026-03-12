import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np
import os
import traceback
import streamlit.components.v1 as components

# --- 1. SETUP ---
st.set_page_config(page_title="Trading Analysis", layout="wide")
st.title("Situational Analysis ")

# --- 2. SIDEBAR TOP: Asset & Timeframe ---
st.sidebar.header("Settings")

# CLEAR FILTERS LOGIC
if st.sidebar.button("🔄 Clear All Filters", use_container_width=True):
    st.session_state.clear() 
    st.rerun() 

assets = {
    'NASDAQ': 'nasdaq.xlsx', 
    'S&P 500': 'sp500.xlsx',
    'Dow Jones': 'dow.xlsx', 
    'DAX': 'dax.xlsx', 
    'FTSE': 'ftse.xlsx'
}
sel_asset = st.sidebar.pills("Asset", options=list(assets.keys()), default='NASDAQ', key='sel_asset')
file_to_load = assets[sel_asset]

metric_placeholder = st.sidebar.empty() 

# --- RTH SESSION TOGGLE ---
rth_session = st.sidebar.selectbox(
    "RTH Session Hours", 
    ["15:30 - 22:00 (Standard)", "14:30 - 21:00 (US/EU Offset Weeks)"], 
    key='rth_session'
)

# --- 3. DATA ENGINE (NOW WITH EXCEL NEWS PARSER) ---
@st.cache_data
def load_news_data(filepath="news.xlsx"):
    """Reads the multi-tab news.xlsx file and standardizes it."""
    if not os.path.exists(filepath): return None
    try:
        xls = pd.ExcelFile(filepath)
        df_list = []
        for sheet in xls.sheet_names:
            temp = pd.read_excel(xls, sheet_name=sheet)
            if temp.empty: continue
            
            # Find the date column regardless of capitalization
            temp.columns = temp.columns.astype(str).str.strip().str.lower()
            date_col = 'date' if 'date' in temp.columns else None
            if not date_col: continue
            
            temp['Date_str'] = pd.to_datetime(temp[date_col], errors='coerce').dt.date.astype(str)
            temp['Event'] = str(sheet).upper() # Uses the Tab Name (CPI, NFP, etc)
            
            time_col = 'time' if 'time' in temp.columns else None
            if time_col:
                temp['Time_str'] = temp[time_col].astype(str)
            else:
                temp['Time_str'] = None
                
            df_list.append(temp[['Date_str', 'Event', 'Time_str']])
            
        if df_list:
            df_all = pd.concat(df_list, ignore_index=True).dropna(subset=['Date_str'])
            
            def get_first_time(x):
                valid = [str(i) for i in x if str(i).strip() not in ['nan', 'None', 'NaT']]
                return valid[0] if valid else None
                
            # If multiple events land on the same day, join them (e.g., "CPI + FOMC")
            res = df_all.groupby('Date_str').agg(
                News_Event=('Event', lambda x: ' + '.join(x.unique())),
                News_Time=('Time_str', get_first_time)
            ).reset_index()
            return res.rename(columns={'Date_str': 'Date'})
        return None
    except Exception as e:
        return None

@st.cache_data
def load_data(f, tf, asset_name):
    if not os.path.exists(f): return None, None
    try:
        if f.lower().endswith('.csv'):
            df = pd.read_csv(f, sep=None, engine='python', encoding='utf-8-sig')
        elif f.lower().endswith(('.xlsx', '.xls')):
            all_sheets = pd.read_excel(f, sheet_name=None)
            valid_sheets = [sheet for sheet in all_sheets.values() if not sheet.empty]
            if not valid_sheets: return None, None
            df = pd.concat(valid_sheets, ignore_index=True)
        else:
            return None, None

        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        df.columns = df.columns.astype(str).str.strip()
        cols_lower = {c.lower(): c for c in df.columns}
        
        for target in ['open', 'high', 'low', 'close']:
            if target in cols_lower:
                df.rename(columns={cols_lower[target]: target}, inplace=True)

        req_cols = [c for c in ['open', 'high', 'low', 'close'] if c in df.columns]
        if req_cols:
            df = df.dropna(subset=req_cols, how='all')

        date_col = next((c for c in df.columns if c.lower().strip() == 'date'), None)
        time_col = next((c for c in df.columns if c.lower().strip() == 'time' and c != date_col), None)

        raw_dt_series = None
        if date_col and time_col:
            raw_dt_series = df[date_col].astype(str) + ' ' + df[time_col].astype(str)
        else:
            dt_col = next((c for c in df.columns if c.lower().strip() in ['datetime', 'date/time', 'date_time', 'time', 'date']), None)
            if dt_col:
                raw_dt_series = df[dt_col].astype(str)

        if raw_dt_series is not None:
            df['tz_offset'] = raw_dt_series.str.extract(r'(\+\d{2})')[0]
        else:
            df['tz_offset'] = '+01'
        df['tz_offset'] = df['tz_offset'].fillna('+01')

        def clean_time_string(series):
            s = series.astype(str).str.replace('T', ' ', regex=False)
            s = s.str.replace(r'\+\d{2}:?\d{2}.*', '', regex=True) 
            s = s.str.replace(r'\-\d{2}:?\d{2}$', '', regex=True) 
            return s

        df['dt_utc'] = pd.to_datetime(raw_dt_series, utc=True, errors='coerce')
        df = df.dropna(subset=['dt_utc'])

        df['dt_ny'] = df['dt_utc'].dt.tz_convert('America/New_York')
        df['dt_eu'] = df['dt_utc'].dt.tz_convert('Europe/Berlin')
        df['dt_uk'] = df['dt_utc'].dt.tz_convert('Europe/London')

        if date_col and time_col:
            d_str = clean_time_string(df[date_col])
            t_str = clean_time_string(df[time_col])
            df['dt'] = pd.to_datetime(d_str + ' ' + t_str, errors='coerce')
        else:
            if dt_col:
                clean_str = clean_time_string(df[dt_col])
                temp_dt = pd.to_datetime(clean_str, utc=True, errors='coerce')
                if pd.api.types.is_datetime64_any_dtype(temp_dt):
                    df['dt'] = temp_dt.dt.tz_localize(None)
                else:
                    df['dt'] = pd.NaT

        df = df.dropna(subset=['dt'])
        if df.empty: return None, None
            
        if pd.api.types.is_datetime64_any_dtype(df['dt']) and df['dt'].dt.tz is not None:
             df['dt'] = df['dt'].dt.tz_localize(None)
             
        df = df.dropna(subset=['dt'])
        df = df.sort_values('dt_utc')
        
        df['Date_str'] = df['dt'].dt.date.astype(str)
        df['Time_str'] = df['dt'].dt.strftime('%H:%M:%S')
        df['Date'] = df['Date_str']
        df['Time'] = df['Time_str']

        df_raw = df.copy()

        tf_map = {'1m': '1min', '2m': '2min', '3m': '3min', '5m': '5min', '10m': '10min', '15m': '15min', '30m': '30min', '1hr': '1h'}
        pd_tf = tf_map.get(tf, '5min')
        
        if pd_tf != '1min':
            df.set_index('dt', inplace=True)
            df = df.resample(pd_tf).agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'dt_utc': 'first'}).dropna().reset_index()
            if df.empty: return None, None
            df['dt_ny'] = df['dt_utc'].dt.tz_convert('America/New_York')
            df['dt_eu'] = df['dt_utc'].dt.tz_convert('Europe/Berlin')
            df['dt_uk'] = df['dt_utc'].dt.tz_convert('Europe/London')
            
            df['Date_str'] = df['dt'].dt.date.astype(str)
            df['Time_str'] = df['dt'].dt.strftime('%H:%M:%S')
            df['Date'] = df['Date_str']
            df['Time'] = df['Time_str']

        df['prev_h'] = df.groupby('Date_str')['high'].shift(1)
        df['prev_l'] = df.groupby('Date_str')['low'].shift(1)
        df['is_inside'] = (df['high'] < df['prev_h']) & (df['low'] > df['prev_l'])
        df['is_outside'] = (df['high'] > df['prev_h']) & (df['low'] < df['prev_l'])
        df['is_green'] = df['close'] > df['open']
        df['is_red'] = df['close'] < df['open']
        df['is_shaved'] = (df['is_green'] & (df['close'] == df['high'])) | (df['is_red'] & (df['close'] == df['low']))
        df['is_eq_high'] = (df['high'] - df['prev_h']).abs() <= 1.0
        df['is_eq_low'] = (df['low'] - df['prev_l']).abs() <= 1.0

        df['ledge_high'] = df['is_eq_high'] & df['is_eq_high'].shift(1)
        df['ledge_low'] = df['is_eq_low'] & df['is_eq_low'].shift(1)
        df['is_ledge'] = df['ledge_high'] | df['ledge_low']

        df['close_above_prev_high'] = df['close'] > df['prev_h']
        df['close_below_prev_low'] = df['close'] < df['prev_l']

        df['pat_iii'] = df['is_inside'] & df['is_inside'].shift(1) & df['is_inside'].shift(2)
        df['pat_ioi'] = df['is_inside'] & df['is_outside'].shift(1) & df['is_inside'].shift(2)
        df['pat_oi'] =  df['is_inside'] & df['is_outside'].shift(1)
        df['pat_io'] =  df['is_outside'] & df['is_inside'].shift(1)

        day = df.groupby('Date_str').agg(eo=('open', 'first'), ec=('close', 'last'), eh=('high', 'max'), el=('low', 'min')).reset_index().rename(columns={'Date_str': 'Date'})
        day['dt_obj'] = pd.to_datetime(day['Date'], errors='coerce')
        if pd.api.types.is_datetime64_any_dtype(day['dt_obj']) and day['dt_obj'].dt.tz is not None: 
            day['dt_obj'] = day['dt_obj'].dt.tz_localize(None)
            
        day['Year'] = day['dt_obj'].dt.year.astype(str).str.replace(r'\.0$', '', regex=True)
        day['Month'] = day['dt_obj'].dt.strftime('%b') 
        day['DayName'] = day['dt_obj'].dt.day_name()
        
        day_tz = df_raw.groupby('Date_str')['tz_offset'].first().reset_index()
        day = pd.merge(day, day_tz.rename(columns={'Date_str': 'Date'}), on='Date', how='left')

        # === MERGE EXCEL NEWS DATA HERE ===
        news_df = load_news_data("news.xlsx")
        if news_df is not None and not news_df.empty:
            day = pd.merge(day, news_df, on='Date', how='left')
        else:
            day['News_Event'] = np.nan
            day['News_Time'] = np.nan

        us_assets = ['NASDAQ', 'S&P 500', 'Dow Jones']
        if asset_name in us_assets:
            ref_col = 'dt_ny'
            r_s, r_e = pd.to_datetime('09:30:00').time(), pd.to_datetime('16:00:00').time()
            t_pts = ['09:30:00', '09:35:00', '09:40:00', '09:45:00', '10:00:00', '10:30:00']
        elif asset_name == 'DAX':
            ref_col = 'dt_eu'
            r_s, r_e = pd.to_datetime('09:00:00').time(), pd.to_datetime('17:30:00').time()
            t_pts = ['09:00:00', '09:05:00', '09:10:00', '09:15:00', '09:30:00', '10:00:00']
        elif asset_name == 'FTSE':
            ref_col = 'dt_uk'
            r_s, r_e = pd.to_datetime('08:00:00').time(), pd.to_datetime('16:30:00').time()
            t_pts = ['08:00:00', '08:05:00', '08:10:00', '08:15:00', '08:30:00', '09:00:00']

        df_raw['is_rth'] = (df_raw[ref_col].dt.time >= r_s) & (df_raw[ref_col].dt.time < r_e)
        df['is_rth'] = (df[ref_col].dt.time >= r_s) & (df[ref_col].dt.time < r_e)

        rth = df_raw[df_raw['is_rth']]
        rth_d = rth.groupby('Date_str').agg(ro=('open', 'first'), rc=('close', 'last'), rh=('high', 'max'), rl=('low', 'min')).reset_index()
        day = pd.merge(day, rth_d.rename(columns={'Date_str': 'Date'}), on='Date', how='left')

        on_end = pd.to_datetime('08:00:00').time() if asset_name in ['DAX', 'FTSE'] else pd.to_datetime('12:00:00').time()
        on_mask = (df_raw['dt'].dt.time >= pd.to_datetime('00:00:00').time()) & (df_raw['dt'].dt.time <= on_end)
        on_data = df_raw[on_mask].groupby('Date_str').agg(on_h=('high', 'max'), on_l=('low', 'min')).reset_index()
        day = pd.merge(day, on_data.rename(columns={'Date_str': 'Date'}), on='Date', how='left')

        day['pc'], day['ph'], day['pl'] = day['ec'].shift(1), day['eh'].shift(1), day['el'].shift(1)
        day['prc'], day['prh'], day['prl'] = day['rc'].shift(1), day['rh'].shift(1), day['rl'].shift(1)
        day['p_color'] = np.where(day['ec'].shift(1) > day['eo'].shift(1), 'Green', 'Red')
        
        day['gap'] = ((day['ro'] - day['prc']) / day['prc']) * 100
        day['gd'] = np.where(day['gap'] > 0, 'Gap Up', 'Gap Down')

        conditions = [day['ro'] > day['on_h'], day['ro'] < day['on_l'], (day['ro'] >= day['on_l']) & (day['ro'] <= day['on_h'])]
        day['on_rel'] = np.select(conditions, ['Above', 'Below', 'Inside'], default='Unknown')

        rth_resampled = df[df['is_rth']]
        
        eth_pats = df.groupby('Date_str').agg(
            eth_inside=('is_inside', 'any'), eth_outside=('is_outside', 'any'), eth_shaved=('is_shaved', 'any'), 
            eth_eq_high=('is_eq_high', 'any'), eth_eq_low=('is_eq_low', 'any'), eth_ledge=('is_ledge', 'any'),
            eth_close_above_ph=('close_above_prev_high', 'any'), eth_close_below_pl=('close_below_prev_low', 'any'),
            eth_iii=('pat_iii', 'any'), eth_ioi=('pat_ioi', 'any'), eth_oi=('pat_oi', 'any'), eth_io=('pat_io', 'any')
        ).reset_index().rename(columns={'Date_str': 'Date'})
        
        rth_pats = rth_resampled.groupby('Date_str').agg(
            rth_inside=('is_inside', 'any'), rth_outside=('is_outside', 'any'), rth_shaved=('is_shaved', 'any'), 
            rth_eq_high=('is_eq_high', 'any'), rth_eq_low=('is_eq_low', 'any'), rth_ledge=('is_ledge', 'any'),
            rth_close_above_ph=('close_above_prev_high', 'any'), rth_close_below_pl=('close_below_prev_low', 'any'),
            rth_iii=('pat_iii', 'any'), rth_ioi=('pat_ioi', 'any'), rth_oi=('pat_oi', 'any'), rth_io=('pat_io', 'any')
        ).reset_index().rename(columns={'Date_str': 'Date'})

        day = pd.merge(day, eth_pats, on='Date', how='left')
        day = pd.merge(day, rth_pats, on='Date', how='left')
        
        pat_cols = ['eth_inside', 'eth_outside', 'eth_shaved', 'eth_eq_high', 'eth_eq_low', 'eth_ledge', 'eth_close_above_ph', 'eth_close_below_pl', 'eth_iii', 'eth_ioi', 'eth_oi', 'eth_io',
                    'rth_inside', 'rth_outside', 'rth_shaved', 'rth_eq_high', 'rth_eq_low', 'rth_ledge', 'rth_close_above_ph', 'rth_close_below_pl', 'rth_iii', 'rth_ioi', 'rth_oi', 'rth_io']
        day[pat_cols] = day[pat_cols].fillna(False)

        def get_fixed_bar(df_r, t_list, col_name, time_col):
            s, e = pd.to_datetime(t_list[0]).time(), pd.to_datetime(t_list[1]).time()
            tm = df_r[time_col].dt.time
            cond = (tm >= s) & (tm < e)
            temp = df_r[cond].groupby('Date_str').agg(o=('open', 'first'), c=('close', 'last'), h=('high', 'max'), l=('low', 'min')).reset_index()
            temp[col_name] = np.where(temp['c'] > temp['o'], 'Green', 'Red')
            temp[col_name + '_h'] = temp['h']
            temp[col_name + '_l'] = temp['l']
            return temp[['Date_str', col_name, col_name + '_h', col_name + '_l']]

        b_5m_1 = get_fixed_bar(df_raw, [t_pts[0], t_pts[1]], 'f_5m_1', ref_col)
        b_5m_2 = get_fixed_bar(df_raw, [t_pts[1], t_pts[2]], 'f_5m_2', ref_col)
        b_15m_1 = get_fixed_bar(df_raw, [t_pts[0], t_pts[3]], 'f_15m_1', ref_col)
        b_15m_2 = get_fixed_bar(df_raw, [t_pts[3], t_pts[4]], 'f_15m_2', ref_col)
        b_30m_1 = get_fixed_bar(df_raw, [t_pts[0], t_pts[4]], 'f_30m_1', ref_col)
        b_30m_2 = get_fixed_bar(df_raw, [t_pts[4], t_pts[5]], 'f_30m_2', ref_col)
        b_1h_1 = get_fixed_bar(df_raw, [t_pts[0], t_pts[5]], 'f_1h_1', ref_col)

        for b_df in [b_5m_1, b_5m_2, b_15m_1, b_15m_2, b_30m_1, b_30m_2, b_1h_1]:
            day = pd.merge(day, b_df.rename(columns={'Date_str': 'Date'}), on='Date', how='left')

        if 'f_5m_1_h' in day.columns:
            day['f_5m_1_len'] = day['f_5m_1_h'] - day['f_5m_1_l']
            
        day['f_15m_2_pat'] = 'None'
        if 'f_15m_1_h' in day.columns and 'f_15m_2_h' in day.columns:
            inside_mask = (day['f_15m_2_h'] < day['f_15m_1_h']) & (day['f_15m_2_l'] > day['f_15m_1_l'])
            outside_mask = (day['f_15m_2_h'] > day['f_15m_1_h']) & (day['f_15m_2_l'] < day['f_15m_1_l'])
            day.loc[inside_mask, 'f_15m_2_pat'] = 'Inside'
            day.loc[outside_mask, 'f_15m_2_pat'] = 'Outside'

        return df, day
    except Exception as e: 
        st.error(f"Error loading data: {e}\n\n{traceback.format_exc()}")
        return None, None

components.html(
    """
    <script>
    const doc = window.parent.document;
    
    if (!doc.getElementById('backtest_hotkeys')) {
        const scriptTag = doc.createElement('script');
        scriptTag.id = 'backtest_hotkeys';
        doc.body.appendChild(scriptTag);
        
        doc.addEventListener('keydown', function(e) {
            if (doc.activeElement && (doc.activeElement.tagName === 'INPUT' || doc.activeElement.tagName === 'TEXTAREA')) return;
            
            const btns = Array.from(doc.querySelectorAll('button'));
            
            if (e.key === 'ArrowRight' && e.shiftKey) {
                const nextMatchBtn = btns.find(b => b.innerText.includes('Next Match'));
                if (nextMatchBtn) nextMatchBtn.click();
                e.preventDefault();
            }
            else if (e.key === 'ArrowLeft' && e.shiftKey) {
                const prevMatchBtn = btns.find(b => b.innerText.includes('Previous Match'));
                if (prevMatchBtn) prevMatchBtn.click();
                e.preventDefault();
            }
            else if (e.key === 'ArrowRight' && !e.shiftKey) {
                const nextBtn = btns.find(b => b.innerText.includes('Next Bar'));
                if (nextBtn) nextBtn.click();
            }
            else if (e.key === 'ArrowLeft' && !e.shiftKey) {
                const prevBtn = btns.find(b => b.innerText.includes('Prev Bar'));
                if (prevBtn) prevBtn.click();
            }
        });
    }

    if (!doc.getElementById('custom-h-crosshair')) {
        const hLine = doc.createElement('div');
        hLine.id = 'custom-h-crosshair';
        hLine.style.position = 'fixed';
        hLine.style.left = '0';
        hLine.style.width = '100vw';
        hLine.style.height = '1px';
        hLine.style.borderTop = '1px dashed #a0a0a0'; 
        hLine.style.pointerEvents = 'none';
        hLine.style.zIndex = '99999';
        hLine.style.display = 'none';
        doc.body.appendChild(hLine);

        doc.addEventListener('mousemove', function(e) {
            const isMain = e.target.closest('.stApp');
            if (isMain) {
                hLine.style.display = 'block';
                hLine.style.top = e.clientY + 'px';
            } else {
                hLine.style.display = 'none';
            }
        });
        
        doc.addEventListener('mouseleave', function() {
            hLine.style.display = 'none';
        });
    }
    </script>
    """,
    height=0,
    width=0,
)

# --- 4. SIDEBAR CONTINUED ---
tf_opts = ['1m', '2m', '3m', '5m', '10m', '15m', '30m', '1hr']
sel_tf = st.sidebar.pills("Time Frame", tf_opts, default='5m', key='sel_tf')

df_i, df_d = load_data(file_to_load, sel_tf, sel_asset)

if df_d is not None:
    sess = st.sidebar.pills("Trading Hours", ['ETH', 'RTH'], default='RTH', key='sess')
    
    level_opts = ["Globex High", "Globex Low", "Globex Close", "RTH High", "RTH Low", "RTH Close", "ON High", "ON Low"]
    levels = st.sidebar.pills("Levels", level_opts, default=["Globex High", "Globex Low", "RTH Close"], selection_mode="multi", key='levels')
    
    c_tog1, c_tog2 = st.sidebar.columns(2)
    with c_tog1: nums = st.toggle("Numbers", value=True, key='nums')
    with c_tog2: show_50_pct = st.toggle("50% Bar Levels", value=False, key='show_50_pct')
    
    st.sidebar.divider()
    backtest_mode = st.sidebar.toggle("🔬 Backtest Mode", value=False, key='backtest_mode', help="Hides future bars to prevent hindsight bias.")
    chart_height = st.sidebar.slider("Chart Height", min_value=500, max_value=1500, value=650, step=50, key='chart_height')
    st.sidebar.divider()
    
    # --- MACRO ECONOMICS FILTER ---
    st.sidebar.markdown("**Macro Economics (news.xlsx)**")
    news_opt = st.sidebar.pills("News Events", ['Any', 'No News', 'CPI', 'NFP', 'PPI', 'FOMC'], default='Any', key='news_opt')
    st.sidebar.divider()

    # --- MEASURED MOVE TOOL ---
    with st.sidebar.expander("🛠️ Measured Move Tool", expanded=False):
        mm_on = st.toggle("Enable Tool", value=False, key='mm_on')
        if mm_on:
            mm_mode = st.radio("Mode", ["1 Bar", "2 Bars", "Range"], horizontal=True, key='mm_mode')
            if mm_mode == "1 Bar":
                mm_b1 = st.number_input("Bar Number", min_value=1, value=1, key='mm_b1')
                c1, c2 = st.columns(2)
                with c1: mm_a1 = st.selectbox("Point 1 (Base)", ["Open", "High", "Low", "Close"], index=3, key='mm_a1')
                with c2: mm_a2 = st.selectbox("Point 2 (Target)", ["Open", "High", "Low", "Close"], index=1, key='mm_a2')
            elif mm_mode == "2 Bars":
                c1, c2 = st.columns(2)
                with c1:
                    mm_b1 = st.number_input("Bar 1 #", min_value=1, value=1, key='mm_b1')
                    mm_a1 = st.selectbox("Point 1 (Base)", ["Open", "High", "Low", "Close"], index=1, key='mm_a1')
                with c2:
                    mm_b2 = st.number_input("Bar 2 #", min_value=1, value=2, key='mm_b2')
                    mm_a2 = st.selectbox("Point 2 (Target)", ["Open", "High", "Low", "Close"], index=2, key='mm_a2')
            elif mm_mode == "Range":
                c1, c2 = st.columns(2)
                with c1: mm_b1 = st.number_input("Start Bar #", min_value=1, value=1, key='mm_b1')
                with c2: mm_b2 = st.number_input("End Bar #", min_value=1, value=5, key='mm_b2')
                st.caption("Pt 1 (Base) = High. Pt 2 (Target) = Low.")
                
            st.divider()
            mm_calc_type = st.radio("Measurement Type", ["Fractions (%)", "Fixed Points"], horizontal=True, key='mm_calc_type')
            
            if mm_calc_type == "Fractions (%)":
                mm_mults_str = st.text_input("Multiplier Levels", "-1, -0.5, 0, 0.33, 0.5, 0.67, 1, 1.5, 2", key='mm_mults_str')
                try:
                    mm_levels_list = [float(x.strip()) for x in mm_mults_str.split(",")]
                except:
                    mm_levels_list = [0, 0.5, 1] 
            else:
                mm_pts_str = st.text_input("Point Target Levels", "10, 20, 30, 40, 50", key='mm_pts_str')
                try:
                    mm_pts_list = [float(x.strip()) for x in mm_pts_str.split(",")]
                except:
                    mm_pts_list = [10, 20, 50]
    
    st.sidebar.divider()
    g_dir = st.sidebar.pills("Gap", ['Any', 'Gap Up', 'Gap Down'], default='Any', key='g_dir')
    g_sz = st.sidebar.pills("Size", ['Any', 'Less', 'Greater'], default='Any', key='g_sz')
    g_v = st.sidebar.pills("Percentage", [0.5, 1.0, 1.5], default=0.5, key='g_v')
    st.sidebar.divider()

    on_rel_opt = st.sidebar.pills("Open vs ON Range", ['Any', 'Above', 'Below', 'Inside'], default='Any', key='on_rel_opt')
    st.sidebar.divider()
    
    st.sidebar.markdown("**RTH Bar Colour**")
    f_5m_1 = st.sidebar.pills("1st 5m", ['Any', 'Green', 'Red'], default='Any', key='f_5m_1')
    f_5m_2 = st.sidebar.pills("2nd 5m", ['Any', 'Green', 'Red'], default='Any', key='f_5m_2')
    f_15m_1 = st.sidebar.pills("1st 15m", ['Any', 'Green', 'Red'], default='Any', key='f_15m_1')
    f_15m_2 = st.sidebar.pills("2nd 15m", ['Any', 'Green', 'Red'], default='Any', key='f_15m_2')
    f_30m_1 = st.sidebar.pills("1st 30m", ['Any', 'Green', 'Red'], default='Any', key='f_30m_1')
    f_30m_2 = st.sidebar.pills("2nd 30m", ['Any', 'Green', 'Red'], default='Any', key='f_30m_2')
    f_1h_1 = st.sidebar.pills("1st 1hr", ['Any', 'Green', 'Red'], default='Any', key='f_1h_1')
    st.sidebar.divider()

    st.sidebar.markdown("**Advanced Bar Filters**")
    f_15m_2_pat = st.sidebar.pills("2nd 15m Pattern", ['Any', 'Inside', 'Outside'], default='Any', key='f_15m_2_pat')
    
    st.sidebar.markdown("1st 5m Length Thresholds")
    c_th1, c_th2 = st.sidebar.columns(2)
    with c_th1: len_low = st.number_input("Low", value=50.0, step=10.0, key='len_low')
    with c_th2: len_high = st.number_input("High", value=100.0, step=10.0, key='len_high')
    f_5m_1_len_opt = st.sidebar.pills("1st 5m Length Filter", ['Any', 'Less', 'Between', 'Greater'], default='Any', key='f_5m_1_len_opt')
    st.sidebar.divider()
    
    pd_col = st.sidebar.pills("Prev Day Colour", ['Any', 'Green', 'Red'], default='Any', key='pd_col')
    dfin = st.sidebar.pills(f"Current Day Finish", ['Any', 'Green', 'Red'], default='Any', key='dfin')
    st.sidebar.divider()
    
    pat_opts = ['Any', 'Inside Bar', 'Outside Bar', 'Shaved Head', 'Equal Highs', 'Equal Lows', 'Ledge', 'Close > Prev High', 'Close < Prev Low', 'iii', 'ioi', 'oi', 'io']
    pat = st.sidebar.pills(f"{sel_tf} Pattern", pat_opts, default='Any', key='pat')
    
    days_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    sel_day = st.sidebar.pills("Day", ['Any'] + days_week, default='Any', key='sel_day')
    
    months_opts = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    sel_month = st.sidebar.pills("Month", ['Any'] + months_opts, default='Any', key='sel_month')
    
    years_avail = sorted([y for y in df_d['Year'].unique().tolist() if y and str(y) != 'nan'], reverse=True)
    sel_year = st.sidebar.pills("Year", ['Any'] + years_avail, default='Any', key='sel_year')

    # --- FILTER LOGIC ---
    df_f = df_d.copy()
    if g_dir != 'Any': df_f = df_f[df_f['gd'] == g_dir]
    if g_sz == 'Less': df_f = df_f[df_f['gap'].abs() < g_v]
    elif g_sz == 'Greater': df_f = df_f[df_f['gap'].abs() > g_v]
    if on_rel_opt != 'Any': df_f = df_f[df_f['on_rel'] == on_rel_opt]
    
    if f_5m_1 != 'Any': df_f = df_f[df_f['f_5m_1'] == f_5m_1]
    if f_5m_2 != 'Any': df_f = df_f[df_f['f_5m_2'] == f_5m_2]
    if f_15m_1 != 'Any': df_f = df_f[df_f['f_15m_1'] == f_15m_1]
    if f_15m_2 != 'Any': df_f = df_f[df_f['f_15m_2'] == f_15m_2]
    if f_30m_1 != 'Any': df_f = df_f[df_f['f_30m_1'] == f_30m_1]
    if f_30m_2 != 'Any': df_f = df_f[df_f['f_30m_2'] == f_30m_2]
    if f_1h_1 != 'Any': df_f = df_f[df_f['f_1h_1'] == f_1h_1]
    
    if f_15m_2_pat != 'Any': 
        df_f = df_f[df_f['f_15m_2_pat'] == f_15m_2_pat]
        
    if f_5m_1_len_opt == 'Less':
        df_f = df_f[df_f['f_5m_1_len'] < len_low]
    elif f_5m_1_len_opt == 'Between':
        df_f = df_f[(df_f['f_5m_1_len'] >= len_low) & (df_f['f_5m_1_len'] <= len_high)]
    elif f_5m_1_len_opt == 'Greater':
        df_f = df_f[df_f['f_5m_1_len'] > len_high]
    
    if pd_col != 'Any': df_f = df_f[df_f['p_color'] == pd_col]
    df_f['dc'] = np.where(df_f['rc' if sess=='RTH' else 'ec'] > df_f['ro' if sess=='RTH' else 'eo'], 'Green', 'Red')
    if dfin != 'Any': df_f = df_f[df_f['dc'] == dfin]
    
    if pat == 'Inside Bar': df_f = df_f[df_f['rth_inside'] if sess == 'RTH' else df_f['eth_inside']]
    elif pat == 'Outside Bar': df_f = df_f[df_f['rth_outside'] if sess == 'RTH' else df_f['eth_outside']]
    elif pat == 'Shaved Head': df_f = df_f[df_f['rth_shaved'] if sess == 'RTH' else df_f['eth_shaved']]
    elif pat == 'Equal Highs': df_f = df_f[df_f['rth_eq_high'] if sess == 'RTH' else df_f['eth_eq_high']]
    elif pat == 'Equal Lows':  df_f = df_f[df_f['rth_eq_low'] if sess == 'RTH' else df_f['eth_eq_low']]
    elif pat == 'Ledge':       df_f = df_f[df_f['rth_ledge'] if sess == 'RTH' else df_f['eth_ledge']]
    elif pat == 'Close > Prev High': df_f = df_f[df_f['rth_close_above_ph'] if sess == 'RTH' else df_f['eth_close_above_ph']]
    elif pat == 'Close < Prev Low':  df_f = df_f[df_f['rth_close_below_pl'] if sess == 'RTH' else df_f['eth_close_below_pl']]
    elif pat == 'iii': df_f = df_f[df_f['rth_iii'] if sess == 'RTH' else df_f['eth_iii']]
    elif pat == 'ioi': df_f = df_f[df_f['rth_ioi'] if sess == 'RTH' else df_f['eth_ioi']]
    elif pat == 'oi':  df_f = df_f[df_f['rth_oi']  if sess == 'RTH' else df_f['eth_oi']]
    elif pat == 'io':  df_f = df_f[df_f['rth_io']  if sess == 'RTH' else df_f['eth_io']]

    if sel_day != 'Any': df_f = df_f[df_f['DayName'] == sel_day]
    if sel_month != 'Any': df_f = df_f[df_f['Month'] == sel_month]
    if sel_year != 'Any': df_f = df_f[df_f['Year'] == sel_year]

    # APPLY NEWS FILTER
    if news_opt != 'Any':
        if 'News_Event' in df_f.columns:
            if news_opt == 'No News':
                df_f = df_f[df_f['News_Event'].isna() | (df_f['News_Event'] == '') | (df_f['News_Event'] == 'None')]
            else:
                df_f = df_f[df_f['News_Event'].str.contains(news_opt, na=False, case=False)]
    
    all_dates_chrono = sorted(df_d['Date'].unique().tolist())
    filtered_dates_chrono = sorted(df_f['Date'].tolist())
    pill_dates = sorted(df_f['Date'].tolist(), reverse=True)
    
    metric_placeholder.metric("Total Days Found", len(filtered_dates_chrono))

    # --- 5. DISPLAY LAYOUT ---
    if pill_dates:
        if 'selected_date' not in st.session_state: st.session_state.selected_date = pill_dates[0]

        def go_prev_c():
            if st.session_state.selected_date in all_dates_chrono:
                idx = all_dates_chrono.index(st.session_state.selected_date)
                if idx > 0: st.session_state.selected_date = all_dates_chrono[idx - 1]
        def go_next_c():
            if st.session_state.selected_date in all_dates_chrono:
                idx = all_dates_chrono.index(st.session_state.selected_date)
                if idx < len(all_dates_chrono) - 1: st.session_state.selected_date = all_dates_chrono[idx + 1]
        def go_prev_f():
            if st.session_state.selected_date in filtered_dates_chrono:
                idx = filtered_dates_chrono.index(st.session_state.selected_date)
                if idx > 0: st.session_state.selected_date = filtered_dates_chrono[idx - 1]
        def go_next_f():
            if st.session_state.selected_date in filtered_dates_chrono:
                idx = filtered_dates_chrono.index(st.session_state.selected_date)
                if idx < len(filtered_dates_chrono) - 1: st.session_state.selected_date = filtered_dates_chrono[idx + 1]

        # REORDERED CONTAINERS 
        nav_cont = st.container()
        slider_cont = st.container()
        graph_cont = st.container()
        perf_cont = st.container() 
        dates_cont = st.container()

        sd = st.session_state.selected_date
        c_idx = all_dates_chrono.index(sd) if sd in all_dates_chrono else -1
        f_idx = filtered_dates_chrono.index(sd) if sd in filtered_dates_chrono else -1

        pd_df_full = df_i[df_i['Date_str'] == sd].copy()
        day_info = df_d[df_d['Date'] == sd].iloc[0] if not df_d[df_d['Date'] == sd].empty else None
        
        if "14:30" in rth_session:
            rth_start_time = pd.to_datetime('14:30:00').time()
            rth_end_time = pd.to_datetime('21:00:00').time()
        else:
            rth_start_time = pd.to_datetime('15:30:00').time()
            rth_end_time = pd.to_datetime('22:00:00').time()
            
        if sess == 'RTH':
            pd_df_full = pd_df_full[pd_df_full['is_rth']]

        with nav_cont:
            nav_c1, nav_c2 = st.columns(2)
            with nav_c1:
                st.markdown("#### Chronological Navigation")
                nc1, nc2 = st.columns(2)
                with nc1: st.button("⬅️ Previous Day", on_click=go_prev_c, disabled=(c_idx <= 0), use_container_width=True)
                with nc2: st.button("Next Day ➡️", on_click=go_next_c, disabled=(c_idx == -1 or c_idx == len(all_dates_chrono) - 1), use_container_width=True)
                
            with nav_c2:
                st.markdown("#### Filtered Navigation")
                nf1, nf2 = st.columns(2)
                with nf1: st.button("⬅️ Previous Match", on_click=go_prev_f, disabled=(f_idx <= 0), use_container_width=True)
                with nf2: st.button("Next Match ➡️", on_click=go_next_f, disabled=(f_idx == -1 or f_idx == len(filtered_dates_chrono) - 1), use_container_width=True)
            st.divider()

            # --- COMPARE MARKETS ---
            st.markdown("### 🔍 Compare Markets")
            comp_on = st.toggle("Enable Side-by-Side Market Comparison", value=False, key='comp_on')
            comp_assets = []
            if comp_on:
                comp_assets = st.multiselect("Compare with:", [a for a in assets.keys() if a != sel_asset], key='comp_assets')
            st.divider()

        with slider_cont:
            if backtest_mode and not pd_df_full.empty:
                max_bars = len(pd_df_full)
                if 'bar_idx' not in st.session_state: st.session_state.bar_idx = 1
                if 'last_bt_date' not in st.session_state or st.session_state.last_bt_date != sd:
                    st.session_state.bar_idx = 1
                    st.session_state.last_bt_date = sd
                    
                if st.session_state.bar_idx > max_bars: st.session_state.bar_idx = max_bars
                if st.session_state.bar_idx < 1: st.session_state.bar_idx = 1

                def step_b():
                    if st.session_state.bar_idx > 1: st.session_state.bar_idx -= 1
                def step_f(max_b):
                    if st.session_state.bar_idx < max_b: st.session_state.bar_idx += 1

                st.markdown("##### ⏱️ Time Machine (Syncs to all charts below)")
                c1, c2, c3 = st.columns([1, 8, 1])
                with c1: st.button("⬅️ Prev Bar", on_click=step_b, use_container_width=True)
                with c2: current_bar_idx = st.slider("Scrub through the day:", 1, max_bars, key='bar_idx', label_visibility="collapsed")
                with c3: st.button("Next Bar ➡️", on_click=step_f, args=(max_bars,), use_container_width=True)
            else:
                current_bar_idx = len(pd_df_full)

        pd_df_slice = pd_df_full.iloc[:current_bar_idx].copy()
        current_time_limit = pd_df_slice['dt'].max() if not pd_df_slice.empty else None
        
        tf_mins_map = {'1m': 1, '2m': 2, '3m': 3, '5m': 5, '10m': 10, '15m': 15, '30m': 30, '1hr': 60}
        pad_mins = tf_mins_map.get(sel_tf, 5) * 2  
        pad_time = pd.Timedelta(minutes=pad_mins)
        
        x_min_main = pd_df_full['dt'].min() - pad_time if not pd_df_full.empty else pd.Timestamp.now()
        x_max_main = pd_df_full['dt'].max() + pad_time if not pd_df_full.empty else pd.Timestamp.now()

        # --- LEFT & RIGHT CHART RENDERING ---
        with graph_cont:
            num_charts = 1 + len(comp_assets) if (comp_on and comp_assets) else 1
            cols = st.columns(num_charts)

            # --- COLUMN 1: MAIN ASSET ---
            with cols[0]:
                if not pd_df_full.empty and day_info is not None:
                    info = day_info
                    
                    if pd.notna(info['rc']) and pd.notna(info['ro']) and pd.notna(info['ec']) and pd.notna(info['eo']):
                        pts_diff = info['rc'] - info['ro'] if sess == 'RTH' else info['ec'] - info['eo']
                        sign = "+" if pts_diff > 0 else ""
                        badge_bg = "#e6f4ea" if pts_diff > 0 else "#fce8e6"
                        badge_txt = "#0d652d" if pts_diff > 0 else "#a50e0e"
                        st.markdown(f"""
                        <div style="background-color: {badge_bg}; color: {badge_txt}; padding: 4px 10px; border-radius: 4px; display: inline-block; font-weight: bold; font-size: 14px; margin-bottom: 5px;">
                            Result: {sign}{pts_diff:.1f} pts
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='margin-bottom: 30px;'></div>", unsafe_allow_html=True)

                    f5_len_str = f"{info['f_5m_1_len']:.1f}pt" if pd.notna(info['f_5m_1_len']) else "N/A"
                    f15_pat_str = info['f_15m_2_pat'] if pd.notna(info['f_15m_2_pat']) else "N/A"
                    
                    # EXTRACT NEWS FOR INFO BANNER
                    n_ev = info.get('News_Event', np.nan)
                    news_html = f" | <span style='color:#d93025;'><b>🚨 NEWS: {n_ev}</b></span>" if pd.notna(n_ev) and n_ev != '' else ""

                    st.markdown(f"""
                    <div style="font-size: 0.85em; padding: 8px; background-color: #e8f0fe; border-radius: 6px; margin-bottom: 10px; color: #1967d2; border: 1px solid #d2e3fc;">
                        <b>{sel_asset}</b> | {sd} ({info['DayName']})<br>
                        Open: <b>{info['on_rel']} ON</b> | Gap: {info['gap']:.2f}%<br>
                        1st 5m: {info['f_5m_1']} ({f5_len_str}) | 2nd 15m: {info['f_15m_2']} ({f15_pat_str}){news_html}
                    </div>
                    """, unsafe_allow_html=True)

                    pd_df_plot = pd_df_full.copy()
                    if backtest_mode and current_time_limit is not None:
                        pd_df_plot.loc[pd_df_plot['dt'] > current_time_limit, ['open', 'high', 'low', 'close']] = np.nan

                    fig = go.Figure()
                    fig.add_trace(go.Candlestick(x=pd_df_plot['dt'], open=pd_df_plot['open'], high=pd_df_plot['high'], low=pd_df_plot['low'], close=pd_df_plot['close'], name="Price", increasing_line_color='green', decreasing_line_color='black', hoverinfo='none'))

                    custom_data = np.stack((pd_df_plot['open'], pd_df_plot['high'], pd_df_plot['low'], pd_df_plot['close']), axis=-1)
                    ohlc_hover = "<b>Time:</b> %{x|%H:%M} &nbsp;&nbsp;|&nbsp;&nbsp; <b>O:</b> %{customdata[0]:,.2f} &nbsp;&nbsp; <b>H:</b> %{customdata[1]:,.2f} &nbsp;&nbsp; <b>L:</b> %{customdata[2]:,.2f} &nbsp;&nbsp; <b>C:</b> %{customdata[3]:,.2f}<extra></extra>"
                    fig.add_trace(go.Scatter(x=pd_df_plot['dt'], y=[0.03] * len(pd_df_plot), yaxis='y2', mode='markers', marker=dict(color='rgba(0,0,0,0)', size=1), customdata=custom_data, hovertemplate=ohlc_hover, showlegend=False))

                    rb = pd_df_slice[pd_df_slice['is_rth']].copy()
                    rb['n'] = np.arange(1, len(rb) + 1)

                    if sess == 'ETH' and pd.notna(info['on_h']) and pd.notna(info['on_l']):
                        on_end_str = '08:00:00' if sel_asset in ['DAX', 'FTSE'] else '12:00:00'
                        on_start_dt = pd.to_datetime(f"{sd} 00:00:00")
                        on_end_dt = pd.to_datetime(f"{sd} {on_end_str}")
                        fig.add_shape(type="rect", x0=on_start_dt, y0=info['on_l'], x1=on_end_dt, y1=info['on_h'], line=dict(color="RoyalBlue", width=1, dash="dot"), fillcolor="LightSkyBlue", opacity=0.15, layer="below")

                    if show_50_pct and not pd_df_slice.empty:
                        mid_prices = (pd_df_slice['high'] + pd_df_slice['low']) / 2
                        fig.add_trace(go.Scatter(x=pd_df_slice['dt'], y=mid_prices, mode='markers', marker=dict(symbol='line-ew', size=14, color='rgba(255, 0, 0, 0.35)', line=dict(width=3, color='rgba(255, 0, 0, 0.35)')), showlegend=False, hoverinfo='skip'))

                    if pat != 'Any':
                        t_bars = pd.DataFrame()
                        y_v, m_s = None, 'star'
                        if pat == 'Inside Bar': t_bars, y_v = pd_df_slice[pd_df_slice['is_inside']], pd_df_slice[pd_df_slice['is_inside']]['high'] * 1.0005
                        elif pat == 'Outside Bar': t_bars, y_v = pd_df_slice[pd_df_slice['is_outside']], pd_df_slice[pd_df_slice['is_outside']]['high'] * 1.0005
                        elif pat == 'Shaved Head': t_bars, y_v = pd_df_slice[pd_df_slice['is_shaved']], pd_df_slice[pd_df_slice['is_shaved']]['high'] * 1.0005
                        elif pat == 'Equal Highs': t_bars, y_v, m_s = pd_df_slice[pd_df_slice['is_eq_high']], pd_df_slice[pd_df_slice['is_eq_high']]['high'] * 1.0005, 'triangle-down'
                        elif pat == 'Equal Lows': t_bars, y_v, m_s = pd_df_slice[pd_df_slice['is_eq_low']], pd_df_slice[pd_df_slice['is_eq_low']]['low'] * 0.9995, 'triangle-up'
                        elif pat == 'Ledge': t_bars, m_s = pd_df_slice[pd_df_slice['is_ledge']], 'square'; y_v = np.where(t_bars['ledge_high'], t_bars['high'] * 1.0005, t_bars['low'] * 0.9995)
                        elif pat == 'Close > Prev High': t_bars, y_v, m_s = pd_df_slice[pd_df_slice['close_above_prev_high']], pd_df_slice[pd_df_slice['close_above_prev_high']]['high'] * 1.0005, 'triangle-up'
                        elif pat == 'Close < Prev Low': t_bars, y_v, m_s = pd_df_slice[pd_df_slice['close_below_prev_low']], pd_df_slice[pd_df_slice['close_below_prev_low']]['low'] * 0.9995, 'triangle-down'
                        elif pat == 'iii': t_bars, y_v = pd_df_slice[pd_df_slice['pat_iii']], pd_df_slice[pd_df_slice['pat_iii']]['high'] * 1.0005
                        elif pat == 'ioi': t_bars, y_v = pd_df_slice[pd_df_slice['pat_ioi']], pd_df_slice[pd_df_slice['pat_ioi']]['high'] * 1.0005
                        elif pat == 'oi': t_bars, y_v = pd_df_slice[pd_df_slice['pat_oi']], pd_df_slice[pd_df_slice['pat_oi']]['high'] * 1.0005
                        elif pat == 'io': t_bars, y_v = pd_df_slice[pd_df_slice['pat_io']], pd_df_slice[pd_df_slice['pat_io']]['high'] * 1.0005
                        if not t_bars.empty: fig.add_trace(go.Scatter(x=t_bars['dt'], y=y_v, mode='markers', marker=dict(symbol=m_s, size=10, color='black'), name=f"{pat} Marker", hoverinfo='skip'))
                    
                    if "Globex High" in (levels or []): fig.add_hline(y=info['ph'], line_dash="dash", line_color="darkgreen", annotation_text=" Prev Globex High")
                    if "Globex Low" in (levels or []): fig.add_hline(y=info['pl'], line_dash="dash", line_color="black", annotation_text=" Prev Globex Low")
                    if "Globex Close" in (levels or []): fig.add_hline(y=info['pc'], line_dash="dash", line_color="gray", annotation_text=" Prev Globex Close")
                    if "RTH High" in (levels or []): fig.add_hline(y=info['prh'], line_dash="solid", line_color="darkgreen", annotation_text=" Prev RTH High")
                    if "RTH Low" in (levels or []): fig.add_hline(y=info['prl'], line_dash="solid", line_color="black", annotation_text=" Prev RTH Low")
                    if "RTH Close" in (levels or []): fig.add_hline(y=info['prc'], line_dash="solid", line_color="black", annotation_text=" Prev RTH Close")
                    if "ON High" in (levels or []): fig.add_hline(y=info['on_h'], line_dash="dot", line_color="orange", annotation_text=" ON High")
                    if "ON Low" in (levels or []): fig.add_hline(y=info['on_l'], line_dash="dot", line_color="purple", annotation_text=" ON Low")

                    # --- DRAW VERTICAL NEWS SPIKE LINE ---
                    n_time = info.get('News_Time', np.nan)
                    if pd.notna(n_ev) and pd.notna(n_time) and str(n_time).strip() not in ['None', 'nan']:
                        try:
                            t_clean = str(n_time).replace('T', ' ').strip()
                            n_dt = pd.to_datetime(f"{sd} {t_clean}")
                            fig.add_vline(x=n_dt, line_width=2, line_dash="dash", line_color="rgba(217, 48, 37, 0.7)", annotation_text=f" 🚨 {n_ev}", annotation_position="top left", annotation_font_color="red")
                        except: pass

                    if nums and not rb.empty: fig.add_trace(go.Scatter(x=rb['dt'], y=rb['high'], text=rb['n'], mode="text", textposition="top center", showlegend=False, hoverinfo='skip'))

                    fig.update_xaxes(range=[x_min_main, x_max_main], showgrid=True, gridwidth=1, gridcolor='rgba(200, 200, 200, 0.3)', showspikes=True, spikecolor="#a0a0a0", spikesnap="cursor", spikemode="across", spikethickness=1, spikedash="dash")
                    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(200, 200, 200, 0.3)', showspikes=False)
                    fig.update_layout(
                        title=dict(text=f"<span style='font-size:12px; color:gray;'>(Fullscreen Hotkeys: Shift + ➡️/⬅️ to change dates)</span>", x=0.01, y=0.98),
                        yaxis2=dict(range=[0, 1], fixedrange=True, visible=False, overlaying='y'),
                        height=chart_height, xaxis_rangeslider_visible=False, template="plotly_white", margin=dict(t=40, b=0, l=10, r=10),
                        dragmode='pan', hovermode='x', hoverdistance=-1, spikedistance=-1, uirevision=sd, hoverlabel=dict(bgcolor="rgba(255, 255, 255, 0.9)", font_size=13, font_family="Arial", bordercolor="rgba(0, 0, 0, 0.2)")
                    )
                    st.plotly_chart(fig, use_container_width=True, config={'scrollZoom': True, 'displayModeBar': True})
                else: st.warning(f"No valid intraday data found for {sd} within the selected hours.")

            # --- COLUMNS 2+: COMPARE ASSETS ---
            if comp_on and comp_assets:
                for i, c_asset in enumerate(comp_assets):
                    with cols[i+1]:
                        c_df_i, c_df_d = load_data(assets[c_asset], sel_tf, c_asset)
                        if c_df_i is not None and sd in c_df_d['Date'].values:
                            c_pd_df_full = c_df_i[c_df_i['Date_str'] == sd].copy()
                            c_info = c_df_d[c_df_d['Date'] == sd].iloc[0]

                            if pd.notna(c_info['rc']) and pd.notna(c_info['ro']) and pd.notna(c_info['ec']) and pd.notna(c_info['eo']):
                                c_pts_diff = c_info['rc'] - c_info['ro'] if sess == 'RTH' else c_info['ec'] - c_info['eo']
                                c_sign = "+" if c_pts_diff > 0 else ""
                                c_badge_bg = "#e6f4ea" if c_pts_diff > 0 else "#fce8e6"
                                c_badge_txt = "#0d652d" if c_pts_diff > 0 else "#a50e0e"
                                st.markdown(f"""
                                <div style="background-color: {c_badge_bg}; color: {c_badge_txt}; padding: 4px 10px; border-radius: 4px; display: inline-block; font-weight: bold; font-size: 14px; margin-bottom: 5px;">
                                    Result: {c_sign}{c_pts_diff:.1f} pts
                                </div>
                                """, unsafe_allow_html=True)
                            else:
                                st.markdown("<div style='margin-bottom: 30px;'></div>", unsafe_allow_html=True)

                            c_f5_len_str = f"{c_info['f_5m_1_len']:.1f}pt" if pd.notna(c_info['f_5m_1_len']) else "N/A"
                            c_f15_pat_str = c_info['f_15m_2_pat'] if pd.notna(c_info['f_15m_2_pat']) else "N/A"
                            
                            c_n_ev = c_info.get('News_Event', np.nan)
                            c_news_html = f" | <span style='color:#d93025;'><b>🚨 NEWS: {c_n_ev}</b></span>" if pd.notna(c_n_ev) and c_n_ev != '' else ""

                            st.markdown(f"""
                            <div style="font-size: 0.85em; padding: 8px; background-color: #f3f4f6; border-radius: 6px; margin-bottom: 10px; color: #3c4043; border: 1px solid #e8eaed;">
                                <b>{c_asset}</b> | {sd} ({c_info['DayName']})<br>
                                Open: <b>{c_info['on_rel']} ON</b> | Gap: {c_info['gap']:.2f}%<br>
                                1st 5m: {c_info['f_5m_1']} ({c_f5_len_str}) | 2nd 15m: {c_info['f_15m_2']}{c_news_html}
                            </div>
                            """, unsafe_allow_html=True)

                            if sess == 'RTH':
                                c_pd_df_full = c_pd_df_full[c_pd_df_full['is_rth']]
                            
                            if not c_pd_df_full.empty:
                                c_pd_df_plot = c_pd_df_full.copy()
                                if backtest_mode and current_time_limit is not None:
                                    c_pd_df_plot.loc[c_pd_df_plot['dt'] > current_time_limit, ['open', 'high', 'low', 'close']] = np.nan
                                
                                c_fig = go.Figure()
                                c_fig.add_trace(go.Candlestick(x=c_pd_df_plot['dt'], open=c_pd_df_plot['open'], high=c_pd_df_plot['high'], low=c_pd_df_plot['low'], close=c_pd_df_plot['close'], name=c_asset, increasing_line_color='green', decreasing_line_color='black', hoverinfo='none'))
                                
                                c_custom_data = np.stack((c_pd_df_plot['open'], c_pd_df_plot['high'], c_pd_df_plot['low'], c_pd_df_plot['close']), axis=-1)
                                c_hover = "<b>Time:</b> %{x|%H:%M} &nbsp;&nbsp;|&nbsp;&nbsp; <b>O:</b> %{customdata[0]:,.2f} &nbsp;&nbsp; <b>H:</b> %{customdata[1]:,.2f} &nbsp;&nbsp; <b>L:</b> %{customdata[2]:,.2f} &nbsp;&nbsp; <b>C:</b> %{customdata[3]:,.2f}<extra></extra>"
                                c_fig.add_trace(go.Scatter(x=c_pd_df_plot['dt'], y=[0.05]*len(c_pd_df_plot), yaxis='y2', mode='markers', marker=dict(color='rgba(0,0,0,0)', size=1), customdata=c_custom_data, hovertemplate=c_hover, showlegend=False))

                                c_pd_df_slice = c_pd_df_full[c_pd_df_full['dt'] <= current_time_limit] if current_time_limit else c_pd_df_full.copy()
                                c_rb = c_pd_df_slice[c_pd_df_slice['is_rth']].copy()
                                c_rb['n'] = np.arange(1, len(c_rb) + 1)

                                if sess == 'ETH' and pd.notna(c_info['on_h']) and pd.notna(c_info['on_l']):
                                    c_on_end_str = '08:00:00' if c_asset in ['DAX', 'FTSE'] else '12:00:00'
                                    c_on_start_dt = pd.to_datetime(f"{sd} 00:00:00")
                                    c_on_end_dt = pd.to_datetime(f"{sd} {c_on_end_str}")
                                    c_fig.add_shape(type="rect", x0=c_on_start_dt, y0=c_info['on_l'], x1=c_on_end_dt, y1=c_info['on_h'], line=dict(color="RoyalBlue", width=1, dash="dot"), fillcolor="LightSkyBlue", opacity=0.15, layer="below")

                                if show_50_pct and not c_pd_df_slice.empty:
                                    c_mid_prices = (c_pd_df_slice['high'] + c_pd_df_slice['low']) / 2
                                    c_fig.add_trace(go.Scatter(x=c_pd_df_slice['dt'], y=c_mid_prices, mode='markers', marker=dict(symbol='line-ew', size=14, color='rgba(255, 0, 0, 0.35)', line=dict(width=3, color='rgba(255, 0, 0, 0.35)')), showlegend=False, hoverinfo='skip'))

                                if pat != 'Any':
                                    c_t_bars = pd.DataFrame()
                                    c_y_v, c_m_s = None, 'star'
                                    if pat == 'Inside Bar': c_t_bars, c_y_v = c_pd_df_slice[c_pd_df_slice['is_inside']], c_pd_df_slice[c_pd_df_slice['is_inside']]['high'] * 1.0005
                                    elif pat == 'Outside Bar': c_t_bars, c_y_v = c_pd_df_slice[c_pd_df_slice['is_outside']], c_pd_df_slice[c_pd_df_slice['is_outside']]['high'] * 1.0005
                                    elif pat == 'Shaved Head': c_t_bars, c_y_v = c_pd_df_slice[c_pd_df_slice['is_shaved']], c_pd_df_slice[c_pd_df_slice['is_shaved']]['high'] * 1.0005
                                    elif pat == 'Equal Highs': c_t_bars, c_y_v, c_m_s = c_pd_df_slice[c_pd_df_slice['is_eq_high']], c_pd_df_slice[c_pd_df_slice['is_eq_high']]['high'] * 1.0005, 'triangle-down'
                                    elif pat == 'Equal Lows': c_t_bars, c_y_v, c_m_s = c_pd_df_slice[c_pd_df_slice['is_eq_low']], c_pd_df_slice[c_pd_df_slice['is_eq_low']]['low'] * 0.9995, 'triangle-up'
                                    elif pat == 'Ledge': c_t_bars, c_m_s = c_pd_df_slice[c_pd_df_slice['is_ledge']], 'square'; c_y_v = np.where(c_t_bars['ledge_high'], c_t_bars['high'] * 1.0005, c_t_bars['low'] * 0.9995)
                                    elif pat == 'Close > Prev High': c_t_bars, c_y_v, c_m_s = c_pd_df_slice[c_pd_df_slice['close_above_prev_high']], c_pd_df_slice[c_pd_df_slice['close_above_prev_high']]['high'] * 1.0005, 'triangle-up'
                                    elif pat == 'Close < Prev Low': c_t_bars, c_y_v, c_m_s = c_pd_df_slice[c_pd_df_slice['close_below_prev_low']], c_pd_df_slice[c_pd_df_slice['close_below_prev_low']]['low'] * 0.9995, 'triangle-down'
                                    if not c_t_bars.empty: c_fig.add_trace(go.Scatter(x=c_t_bars['dt'], y=c_y_v, mode='markers', marker=dict(symbol=c_m_s, size=10, color='black'), hoverinfo='skip', showlegend=False))

                                if "Globex High" in (levels or []): c_fig.add_hline(y=c_info['ph'], line_dash="dash", line_color="darkgreen", annotation_text=" Prev Globex High")
                                if "Globex Low" in (levels or []): c_fig.add_hline(y=c_info['pl'], line_dash="dash", line_color="black", annotation_text=" Prev Globex Low")
                                if "Globex Close" in (levels or []): c_fig.add_hline(y=c_info['pc'], line_dash="dash", line_color="gray", annotation_text=" Prev Globex Close")
                                if "RTH High" in (levels or []): c_fig.add_hline(y=c_info['prh'], line_dash="solid", line_color="darkgreen", annotation_text=" Prev RTH High")
                                if "RTH Low" in (levels or []): c_fig.add_hline(y=c_info['prl'], line_dash="solid", line_color="black", annotation_text=" Prev RTH Low")
                                if "RTH Close" in (levels or []): c_fig.add_hline(y=c_info['prc'], line_dash="solid", line_color="black", annotation_text=" Prev RTH Close")

                                # COMPARE NEWS LINE
                                c_n_time = c_info.get('News_Time', np.nan)
                                if pd.notna(c_n_ev) and pd.notna(c_n_time) and str(c_n_time).strip() not in ['None', 'nan']:
                                    try:
                                        t_clean_c = str(c_n_time).replace('T', ' ').strip()
                                        c_n_dt = pd.to_datetime(f"{sd} {t_clean_c}")
                                        c_fig.add_vline(x=c_n_dt, line_width=2, line_dash="dash", line_color="rgba(217, 48, 37, 0.7)", annotation_text=f" 🚨 {c_n_ev}", annotation_position="top left", annotation_font_color="red")
                                    except: pass

                                if nums and not c_rb.empty:
                                    c_fig.add_trace(go.Scatter(x=c_rb['dt'], y=c_rb['high'], text=c_rb['n'], mode="text", textposition="top center", showlegend=False, hoverinfo='skip'))

                                c_fig.update_xaxes(range=[x_min_main, x_max_main], showgrid=True, gridwidth=1, gridcolor='rgba(200, 200, 200, 0.3)', showspikes=True, spikecolor="#a0a0a0", spikesnap="cursor", spikemode="across", spikethickness=1, spikedash="dash")
                                c_fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(200, 200, 200, 0.3)', showspikes=False)
                                c_fig.update_layout(
                                    title=dict(text=f"<span style='font-size:12px; color:gray;'>(Fullscreen Hotkeys: Shift + ➡️/⬅️)</span>", x=0.01, y=0.98),
                                    yaxis2=dict(range=[0,1], fixedrange=True, visible=False, overlaying='y'),
                                    height=chart_height, xaxis_rangeslider_visible=False, template="plotly_white", margin=dict(t=40, b=0, l=10, r=10),
                                    dragmode='pan', hovermode='x', hoverdistance=-1, spikedistance=-1, uirevision=sd,
                                    hoverlabel=dict(bgcolor="rgba(255, 255, 255, 0.9)", font_size=11, font_family="Arial", bordercolor="rgba(0, 0, 0, 0.2)")
                                )
                                st.plotly_chart(c_fig, use_container_width=True, config={'scrollZoom': True, 'displayModeBar': True})
                            else:
                                st.warning(f"No RTH data for {c_asset} on {sd}")
                        else:
                            st.warning(f"No data found for {c_asset} on {sd}")

        # --- MOVED PERFORMANCE SUMMARY HERE ---
        with perf_cont:
            st.divider()
            with st.expander("📊 Strategy Performance Summary", expanded=False):
                if not df_f.empty:
                    df_metrics = df_f.copy()
                    if sess == 'RTH':
                        df_metrics['pts_diff'] = df_metrics['rc'] - df_metrics['ro']
                    else:
                        df_metrics['pts_diff'] = df_metrics['ec'] - df_metrics['eo']
                    
                    df_metrics = df_metrics.dropna(subset=['pts_diff'])
                    samp_size = len(df_metrics)
                    
                    if samp_size > 0:
                        wins = df_metrics[df_metrics['pts_diff'] > 0]
                        losses = df_metrics[df_metrics['pts_diff'] <= 0]
                        
                        win_ct = len(wins)
                        loss_ct = len(losses)
                        win_rt = (win_ct / samp_size) * 100
                        
                        tot_pl = df_metrics['pts_diff'].sum()
                        exp = tot_pl / samp_size
                        
                        gr_prof = wins['pts_diff'].sum() if win_ct > 0 else 0
                        gr_loss = abs(losses['pts_diff'].sum()) if loss_ct > 0 else 0
                        pf = (gr_prof / gr_loss) if gr_loss != 0 else float('inf')
                        
                        avg_w = gr_prof / win_ct if win_ct > 0 else 0
                        avg_l = -(gr_loss / loss_ct) if loss_ct > 0 else 0
                        
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Sample Size", samp_size)
                        m2.metric("Win Rate", f"{win_rt:.1f}%")
                        m3.metric("Expectancy", f"{'+' if exp>0 else ''}{exp:.1f} pts")
                        m4.metric("Profit Factor", f"{pf:.2f}" if pf != float('inf') else "MAX")
                        
                        st.divider()
                        st.markdown(f"**Wins / Losses:** {win_ct} / {loss_ct}")
                        st.markdown(f"**Avg Win / Avg Loss:** +{avg_w:.1f} / {avg_l:.1f} pts")
                        
                        if not df_metrics.empty:
                            best_val = df_metrics['pts_diff'].max()
                            best_day = df_metrics.loc[df_metrics['pts_diff'].idxmax(), 'Date']
                            st.markdown(f"**Best Day:** {best_day} (+{best_val:.1f} pts)")
                            
                            worst_val = df_metrics['pts_diff'].min()
                            worst_day = df_metrics.loc[df_metrics['pts_diff'].idxmin(), 'Date']
                            st.markdown(f"**Worst Day:** {worst_day} ({worst_val:.1f} pts)")
                else:
                    st.info("No matching days to analyze.")

        with dates_cont:
            st.divider()
            st.markdown("### Jump to a Filtered Date")
            if pill_dates:
                css_rules = []
                for i, d in enumerate(pill_dates):
                    border_color = "#00c853" if df_f[df_f['Date'] == d].iloc[0]['dc'] == 'Green' else "#ff5252"
                    css_rules.append(f'''
                    section[data-testid="stMain"] div[data-testid="stPills"] button:nth-child({i+1}) {{
                        border-left: 6px solid {border_color} !important;
                        border-top-left-radius: 4px !important;
                        border-bottom-left-radius: 4px !important;
                    }}
                    ''')
                st.markdown(f"<style>{''.join(css_rules)}</style>", unsafe_allow_html=True)

            clicked_date = st.pills("Matches:", pill_dates, default=(sd if sd in pill_dates else None), selection_mode="single")
            if clicked_date and clicked_date != st.session_state.selected_date:
                st.session_state.selected_date = clicked_date
                st.rerun()
    else: st.warning("No days match your current filter selection.")
else: st.error("No data available. Please ensure your files are properly formatted.")
