"""
FTD Intelligence Dashboard — Production Version
================================================
Reads from:
  - Google Drive folder: Prediction.csv + Monthly_Domain_*.csv files
  - Google Sheets: Beeswax daily data + Domain lists
  - Streamlit session state: Manual domain flags

Deploy on Streamlit Cloud:
  - Add secrets in Streamlit Cloud dashboard (see secrets.toml.example)
  - Requirements: see requirements.txt
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import warnings
warnings.filterwarnings('ignore')

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ── Page config ───────────────────────────────────────────────────────────
st.set_page_config(
    page_title="FTD Intelligence",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background: #0e1117; color: #e0e0e0; }
.metric-card { background:#1a1d27; border:1px solid #2a2d3a; border-radius:8px; padding:16px 20px; margin:4px 0; }
.metric-label { font-size:11px; color:#888; text-transform:uppercase; letter-spacing:1px; }
.metric-value { font-family:'IBM Plex Mono',monospace; font-size:28px; font-weight:600; margin:4px 0; }
.metric-sub   { font-size:12px; color:#666; }
.domain-a { background:#0d2b1a; border-radius:4px; padding:6px 10px; margin:2px 0; font-family:'IBM Plex Mono',monospace; font-size:12px; }
.domain-b { background:#1a1d27; border-radius:4px; padding:6px 10px; margin:2px 0; font-family:'IBM Plex Mono',monospace; font-size:12px; }
.domain-c { background:#2b1a00; border-radius:4px; padding:6px 10px; margin:2px 0; font-family:'IBM Plex Mono',monospace; font-size:12px; }
.domain-flag { background:#1a1527; border-radius:4px; padding:6px 10px; margin:2px 0; font-family:'IBM Plex Mono',monospace; font-size:12px; border-left:3px solid #9c27b0; }
.rag-card { background:#1a1d27; border:1px solid #2a2d3a; border-radius:6px; padding:12px 16px; margin:6px 0; }
.rag-positive { border-left:3px solid #00c853; }
.rag-negative { border-left:3px solid #f44336; }
.rag-neutral  { border-left:3px solid #666; }
.section-header { font-family:'IBM Plex Mono'; font-size:11px; color:#888;
                  text-transform:uppercase; letter-spacing:2px;
                  border-bottom:1px solid #2a2d3a; padding-bottom:6px; margin:16px 0 8px 0; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────
BF_WEEK   = 48
DRIVE_FOLDER_ID  = "1OaqRSn2t6DuoaV9SFEV9PwCA7xaoVQzr"
BEESWAX_SHEET_ID = "13vA6osPYLm-G8cZfjU4mcr4CVaBG94Uc2YEV1gnAGto"
DOMAIN_LIST_SHEET_ID = "1jWNMGLh0vM-ZXzv-Zz1wDR15peZyzO4dtLimXdjcHmU"

STATUS_COLORS = {
    'URGENT — signal lost':           '#f44336',
    'URGENT — overspend + declining': '#f44336',
    'FIX CPA — overspending':         '#ff6d00',
    'WATCH — declining':              '#ffd600',
    'SCALE — efficient + growing':    '#00c853',
    'MAINTAIN — stable':              '#4fc3f7',
    'WATCH — new signal':             '#ffd600',
    'REVIEW':                         '#666',
}

TREND_EMOJI = {
    'GROWING':'↑','DECLINING':'↓','STABLE':'→',
    'DROPPED':'✕','NEW':'★','DEAD':'—'
}

GEO_NAMES = {
    'DE':'germany','IT':'italy','SE':'sweden','RO':'romania','HU':'hungary',
    'PL':'poland','FI':'finland','AT':'austria','CA':'canada','MY':'malaysia',
    'ID':'indonesia','AU':'australia','TR':'turkey','CH':'switzerland',
    'CZ':'czech','BE':'belgium','CL':'chile','NO':'norway','SG':'singapore',
    'HR':'croatia','SK':'slovakia','GR':'greece','NZ':'new zealand',
    'ME':'montenegro','SI':'slovenia',
}

POSITIVE_KW = ['launch','expand','legali','licens','growth','approv','record','opportunit','partner']
NEGATIVE_KW = ['ban','restrict','regulat','crackdown','illegal','prohibit','fine','suspend','penalt','sanction']

GAMING_PATTERNS = [
    'bet365','betway','888casino','pokerstars','williamhill','unibet',
    'bwin','ladbrokes','coral','paddy','betfair','draftkings','fanduel',
    'casino','poker','sportbet','gambling','betsson','casumo','leovegas',
]

# ── Google API setup ──────────────────────────────────────────────────────

@st.cache_resource
def get_google_services():
    try:
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=[
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/spreadsheets",
            ]
        )
        drive_svc  = build("drive",  "v3", credentials=creds)
        sheets_svc = build("sheets", "v4", credentials=creds)
        return drive_svc, sheets_svc
    except Exception as e:
        st.error(f"Google API connection failed: {e}")
        return None, None


# ── Data fetchers ─────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def fetch_prediction_from_drive():
    drive_svc, _ = get_google_services()
    if drive_svc is None:
        return None
    try:
        results = drive_svc.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and name contains 'Prediction' and mimeType='text/csv'",
            orderBy="modifiedTime desc",
            pageSize=10,
            fields="files(id, name, modifiedTime)"
        ).execute()
        files = results.get('files', [])
        if not files:
            return None
        all_dfs = []
        for f in files:
            request  = drive_svc.files().get_media(fileId=f['id'])
            buf      = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            buf.seek(0)
            df = pd.read_csv(buf)
            all_dfs.append(df)
        return pd.concat(all_dfs, ignore_index=True) if all_dfs else None
    except Exception as e:
        st.warning(f"Could not fetch Prediction files from Drive: {e}")
        return None


@st.cache_data(ttl=3600)
def fetch_monthly_domain_from_drive():
    drive_svc, _ = get_google_services()
    if drive_svc is None:
        return None
    try:
        results = drive_svc.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and name contains 'Monthly_Domain' and mimeType='text/csv'",
            orderBy="modifiedTime desc",
            pageSize=5,
            fields="files(id, name, modifiedTime)"
        ).execute()
        files = results.get('files', [])
        if not files:
            return None
        all_dfs = []
        for f in files:
            request    = drive_svc.files().get_media(fileId=f['id'])
            buf        = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            buf.seek(0)
            df = pd.read_csv(buf, low_memory=False)
            df.columns = [c.strip() for c in df.columns]
            # Normalise domain column
            if 'Domain' in df.columns:
                df = df.rename(columns={'Domain': 'domain'})
            elif 'Banner Domain (2nd level)' in df.columns:
                df = df.rename(columns={'Banner Domain (2nd level)': 'domain'})
            df['source_file'] = f['name']
            all_dfs.append(df)
        return pd.concat(all_dfs, ignore_index=True) if all_dfs else None
    except Exception as e:
        st.warning(f"Could not fetch Monthly Domain files from Drive: {e}")
        return None


@st.cache_data(ttl=1800)
def fetch_beeswax_sheet():
    _, sheets_svc = get_google_services()
    if sheets_svc is None:
        return None
    try:
        result = sheets_svc.spreadsheets().values().get(
            spreadsheetId=BEESWAX_SHEET_ID,
            range="Sheet1!A:F"
        ).execute()
        values = result.get('values', [])
        if not values:
            return None
        df = pd.DataFrame(values[1:], columns=values[0])
        return df
    except Exception as e:
        st.warning(f"Could not fetch Beeswax sheet: {e}")
        return None


@st.cache_data(ttl=3600)
def fetch_domain_lists():
    _, sheets_svc = get_google_services()
    if sheets_svc is None:
        return pd.DataFrame(columns=['domain','list_name'])
    try:
        result = sheets_svc.spreadsheets().values().get(
            spreadsheetId=DOMAIN_LIST_SHEET_ID,
            range="Sheet1!A:B"
        ).execute()
        values = result.get('values', [])
        if not values or len(values) < 2:
            return pd.DataFrame(columns=['domain','list_name'])
        df = pd.DataFrame(values[1:], columns=['domain','list_name'])
        df['domain'] = df['domain'].str.lower().str.strip()
        return df
    except Exception as e:
        st.warning(f"Could not fetch domain lists: {e}")
        return pd.DataFrame(columns=['domain','list_name'])


def save_domain_flags(flags: dict):
    """Save manual domain flags to Streamlit session state."""
    st.session_state['domain_flags'] = flags


def get_domain_flags() -> dict:
    return st.session_state.get('domain_flags', {})


# ── Parsers ───────────────────────────────────────────────────────────────

def parse_campaign(x):
    s     = str(x).strip()
    parts = s.split('_')
    brand = parts[0] if len(parts) > 0 else 'UNK'
    geo   = parts[1].strip() if len(parts) > 1 else 'UNK'
    if 'Acquisition' in s:   camp_type = 'ACQ'
    elif 'Retention' in s:   camp_type = 'RET'
    elif 'Awareness' in s:   camp_type = 'AWA'
    else:                    camp_type = 'UNK'
    return brand, geo, camp_type


def cpa_color(cpa, target):
    if pd.isna(cpa):         return '#666'
    if cpa <= target * 0.5:  return '#00c853'
    if cpa <= target:        return '#ffd600'
    return '#f44336'


def is_gaming_domain(domain: str) -> bool:
    d = str(domain).lower()
    return any(p in d for p in GAMING_PATTERNS)


def domain_in_wl(domain: str, domain_lists: pd.DataFrame) -> list:
    """Returns list of WL names this domain appears in."""
    d = str(domain).lower().strip()
    matches = domain_lists[domain_lists['domain'] == d]['list_name'].tolist()
    return matches


# ── Data processing ───────────────────────────────────────────────────────

@st.cache_data(ttl=1800)
def process_prediction(pred_raw):
    if pred_raw is None:
        return None, None, None

    pred = pred_raw.copy()
    pred.columns = [c.strip() for c in pred.columns]

    parsed = pred['Campaign'].apply(
        lambda x: pd.Series(parse_campaign(x), index=['brand','geo','camp_type']))
    pred = pd.concat([pred, parsed], axis=1)
    pred['date']  = pd.to_datetime(pred['Date'], dayfirst=False)
    pred['month'] = pred['date'].dt.to_period('M').astype(str)
    pred['week']  = pred['date'].dt.isocalendar().week
    pred['year']  = pred['date'].dt.year

    acq       = pred[pred['camp_type'] == 'ACQ'].copy()
    acq_clean = acq[~((acq['year'] == 2025) & (acq['week'] == BF_WEEK))].copy()

    mom   = acq_clean.groupby(['brand','geo','month'])['FTD'].sum().reset_index()
    pivot = mom.pivot_table(index=['brand','geo'], columns='month', values='FTD', fill_value=0)
    pivot.columns  = [str(c) for c in pivot.columns]
    pivot['total'] = pivot.sum(axis=1)

    all_months = sorted([c for c in pivot.columns if c.startswith('20')])
    early_m    = [m for m in all_months if m <= '2025-12']
    recent_m   = [m for m in all_months if '2026-02' <= m <= '2026-03']

    pivot['early']  = pivot[early_m].mean(axis=1)  if early_m  else 0
    pivot['recent'] = pivot[recent_m].mean(axis=1) if recent_m else 0

    def classify(r):
        if r['total'] < 10:                         return 'DEAD'
        if r['early'] == 0 and r['recent'] > 5:    return 'NEW'
        if r['recent'] == 0 and r['early'] > 5:    return 'DROPPED'
        if r['recent'] >= r['early'] * 1.3:        return 'GROWING'
        if r['recent'] <= r['early'] * 0.7:        return 'DECLINING'
        return 'STABLE'

    pivot['trend'] = pivot.apply(classify, axis=1)
    return acq_clean, pivot, all_months


@st.cache_data(ttl=1800)
def process_beeswax(bw_raw):
    if bw_raw is None:
        return None

    df = bw_raw.copy()
    df.columns = [c.strip() for c in df.columns]

    # Clean cost column
    df['Total Cost'] = df['Total Cost'].astype(str).str.replace(
        r'[€$£,]', '', regex=True).apply(pd.to_numeric, errors='coerce').fillna(0)
    df['Impressions']  = pd.to_numeric(df['Impressions'].astype(str).str.replace(',',''), errors='coerce').fillna(0)
    df['Clicks']       = pd.to_numeric(df['Clicks'].astype(str).str.replace(',',''), errors='coerce').fillna(0)
    df['Conversions']  = pd.to_numeric(df['Conversions'].astype(str).str.replace(',',''), errors='coerce').fillna(0)
    df['date']         = pd.to_datetime(df['Date'], dayfirst=False)
    df['month']        = df['date'].dt.to_period('M').astype(str)

    parsed = df['Insertion Order'].apply(
        lambda x: pd.Series(parse_campaign(x), index=['brand','geo','camp_type']))
    df = pd.concat([df, parsed], axis=1)

    return df


@st.cache_data(ttl=1800)
def process_monthly_domain(md_raw):
    if md_raw is None:
        return None

    df = md_raw.copy()
    for col in ['Tracked Ads','Clicks','FTD','DEP','REG']:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(',','').str.replace('%',''),
                errors='coerce').fillna(0)

    parsed = df['Campaign'].apply(
        lambda x: pd.Series(parse_campaign(x), index=['brand','geo','camp_type']))
    df = pd.concat([df.reset_index(drop=True), parsed.reset_index(drop=True)], axis=1)
    return df


def build_combined(acq_clean, pivot, bw_df, target_cpa):
    if bw_df is None or acq_clean is None:
        return None

    bw_acq = bw_df[bw_df['camp_type'] == 'ACQ']
    spend_m = bw_acq.groupby(['brand','geo','month'])['Total Cost'].sum().reset_index()
    spend_m.columns = ['brand','geo','month','spend']

    ftd_m  = acq_clean.groupby(['brand','geo','month'])['FTD'].sum().reset_index()
    cpa_df = ftd_m.merge(spend_m, on=['brand','geo','month'], how='inner')
    cpa_df['cpa'] = cpa_df['spend'] / cpa_df['FTD'].replace(0, np.nan)

    totals = cpa_df.groupby(['brand','geo']).agg(
        total_spend=('spend','sum'),
        total_ftd=('FTD','sum')
    ).reset_index()
    totals['avg_cpa'] = totals['total_spend'] / totals['total_ftd'].replace(0, np.nan)

    trend_r  = pivot[['early','recent','total','trend']].reset_index()
    combined = totals.merge(trend_r, on=['brand','geo'], how='inner')
    combined['brand_geo'] = combined['brand'] + '_' + combined['geo']
    combined['camp_type'] = 'ACQ'

    def get_status(r):
        over = not pd.isna(r['avg_cpa']) and r['avg_cpa'] > target_cpa
        t    = r['trend']
        if t == 'DROPPED':                           return 'URGENT — signal lost'
        if over and t == 'DECLINING':                return 'URGENT — overspend + declining'
        if over and t in ('GROWING','STABLE','NEW'): return 'FIX CPA — overspending'
        if not over and t == 'DECLINING':            return 'WATCH — declining'
        if not over and t == 'GROWING':              return 'SCALE — efficient + growing'
        if not over and t == 'STABLE':               return 'MAINTAIN — stable'
        if t == 'NEW':                               return 'WATCH — new signal'
        return 'REVIEW'

    combined['status'] = combined.apply(get_status, axis=1)
    priority_map = {
        'URGENT — signal lost': 0, 'URGENT — overspend + declining': 1,
        'FIX CPA — overspending': 2, 'WATCH — declining': 3,
        'SCALE — efficient + growing': 4, 'MAINTAIN — stable': 5,
        'WATCH — new signal': 6, 'REVIEW': 7,
    }
    combined['priority'] = combined['status'].map(priority_map)
    combined = combined[combined['total'] >= 10].sort_values('priority')
    return combined, cpa_df


def score_domains(md_df, domain_lists, domain_flags):
    if md_df is None:
        return None

    md_acq = md_df[md_df['camp_type'] == 'ACQ'].copy()

    domain_grp = md_acq.groupby(['brand','geo','domain','camp_type']).agg(
        imp=('Tracked Ads','sum'),
        clicks=('Clicks','sum'),
        ftd=('FTD','sum'),
    ).reset_index()
    domain_grp['ctr']        = domain_grp['clicks'] / domain_grp['imp'].replace(0, np.nan) * 100
    domain_grp['ftd_per_1k'] = domain_grp['ftd']    / domain_grp['imp'].replace(0, np.nan) * 1000

    # Geo CTR thresholds
    geo_thresholds = {}
    for geo, gdf in domain_grp.groupby('geo'):
        valid = gdf[(gdf['ctr'].notna()) & (gdf['imp'] >= 500)]
        if len(valid) >= 5:
            geo_thresholds[geo] = {
                'p25': valid['ctr'].quantile(0.25),
                'p75': valid['ctr'].quantile(0.75),
            }

    def assign_tier(r):
        d         = str(r['domain']).lower()
        flag      = domain_flags.get(d, {})
        manual    = flag.get('manual_tier')

        # Manual override takes priority
        if manual:
            return manual, 'manual_override'

        t = geo_thresholds.get(r['geo'])
        if r['imp'] < 500 or pd.isna(r['ctr']): return 'D', 'low_data'
        if t is None:                            return 'B', 'no_geo_threshold'
        if r['ctr'] >= t['p75'] and r['ftd'] > 0: return 'A', 'high_ctr+ftd'
        if r['ctr'] >= t['p75']:                 return 'B', 'high_ctr_no_ftd'
        if r['ctr'] < t['p25']:                  return 'C', 'low_ctr'
        if r['ftd'] > 0:                         return 'A', 'mid_ctr+ftd'
        return 'B', 'mid_ctr'

    tiers = domain_grp.apply(assign_tier, axis=1)
    domain_grp['tier']        = tiers.apply(lambda x: x[0])
    domain_grp['tier_reason'] = tiers.apply(lambda x: x[1])

    # Cross-reference with WL
    domain_grp['in_wl']      = domain_grp['domain'].apply(
        lambda d: domain_in_wl(d, domain_lists))
    domain_grp['wl_names']   = domain_grp['in_wl'].apply(
        lambda x: ', '.join(x) if x else '')
    domain_grp['is_in_wl']   = domain_grp['in_wl'].apply(bool)
    domain_grp['is_gaming']  = domain_grp['domain'].apply(is_gaming_domain)
    domain_grp['is_flagged'] = domain_grp['domain'].apply(
        lambda d: d.lower() in domain_flags)

    # WL recommendation
    def wl_rec(r):
        flag = domain_flags.get(str(r['domain']).lower(), {})
        if flag.get('exclude_from_wl'):
            return 'EXCLUDED BY USER'
        if r['tier'] == 'A' and not r['is_in_wl']:
            return 'ADD TO WL'
        if r['tier'] == 'A' and r['is_in_wl']:
            return 'KEEP IN WL'
        if r['tier'] in ('C','D') and r['is_in_wl']:
            return 'CONSIDER REMOVING'
        if r['tier'] == 'B' and not r['is_in_wl']:
            return 'MONITOR'
        return ''

    domain_grp['wl_recommendation'] = domain_grp.apply(wl_rec, axis=1)

    return domain_grp


# ── Domain trend analysis ─────────────────────────────────────────────────

def domain_trend_analysis(md_df, domain, brand, geo):
    """Monthly FTD + CTR trend for a specific domain."""
    if md_df is None:
        return None
    sub = md_df[
        (md_df['domain'] == domain) &
        (md_df['brand']  == brand)  &
        (md_df['geo']    == geo)
    ]
    if sub.empty:
        return None

    # Group by source_file as proxy for month
    trend = sub.groupby('source_file').agg(
        imp=('Tracked Ads','sum'),
        clicks=('Clicks','sum'),
        ftd=('FTD','sum'),
    ).reset_index()
    trend['ctr'] = trend['clicks'] / trend['imp'].replace(0, np.nan) * 100
    return trend


# ── Action text ───────────────────────────────────────────────────────────

def action_text(row, answers=None, target=100.0):
    if answers is None:
        answers = {}
    actions = []
    t   = row['trend']
    s   = row['status']
    cpa = row['avg_cpa']
    bg  = row['brand_geo']

    if 'URGENT — signal lost' in s:
        ans = answers.get(f'{bg}_paused')
        if ans is None:
            actions.append(("question", "paused",
                "🔴 Signal lost completely. Is this campaign paused on your end?",
                ["Yes — intentionally paused", "No — should be running"]))
        elif ans == "Yes — intentionally paused":
            actions.append(("info",
                "🟡 Paused intentionally. When reactivating, monitor first 48h and start with conservative bids on Tier A domains only."))
        else:
            spend_ans = answers.get(f'{bg}_spend_check')
            if spend_ans is None:
                actions.append(("question", "spend_check",
                    "🔴 Should be running. Did you check if budget was exhausted in Beeswax?",
                    ["Yes — budget exhausted", "No — budget available", "Not checked yet"]))
            elif spend_ans == "Yes — budget exhausted":
                actions.append(("info",
                    "🔴 Request budget top-up from client. Do not reallocate from other campaigns until confirmed."))
            elif spend_ans == "No — budget available":
                actions.append(("info",
                    "🔴 Budget available but no delivery. Check Beeswax for bid floor issues, creative disapprovals, or domain blocklist conflicts. Escalate immediately."))
            else:
                actions.append(("info",
                    "🔴 Check Beeswax delivery report first — look at win rate and bid rejections, then come back here."))

    if 'URGENT — overspend + declining' in s:
        pct = ((cpa / target) - 1) * 100 if target > 0 else 0
        actions.append(("info",
            f"🔴 CPA EUR{cpa:.0f} — {pct:.0f}% above target and FTDs declining. "
            f"Reduce bids 20-30% on Tier C/D domains immediately. Flag to client."))

    if 'FIX CPA — overspending' in s:
        ans = answers.get(f'{bg}_domain_check')
        if ans is None:
            actions.append(("question", "domain_check",
                f"🟠 CPA EUR{cpa:.0f} — above target but FTDs growing. Already excluded Tier C/D domains?",
                ["Yes — already excluded", "No — not yet"]))
        elif ans == "No — not yet":
            actions.append(("info",
                "🟠 Exclude all Tier C/D domains listed below. Re-check CPA after 7 days. "
                "If still above target, reduce bids 15% on Tier B domains."))
        else:
            actions.append(("info",
                "🟠 Already excluded. Next: reduce bids 15% on Tier B, concentrate budget on Tier A only."))

    if t == 'DECLINING' and not pd.isna(cpa) and cpa <= target:
        ans = answers.get(f'{bg}_spend_trend')
        if ans is None:
            actions.append(("question", "spend_trend",
                "🟡 FTDs declining but CPA healthy. Did spend also drop in the same period?",
                ["Yes — spend dropped too", "No — spend held steady"]))
        elif ans == "Yes — spend dropped too":
            actions.append(("info",
                "🟡 Likely a budget reduction. Confirm with client. CPA is healthy so scaling back up is low risk."))
        else:
            actions.append(("info",
                "🟡 Spend held but FTDs dropped — targeting quality issue. "
                "Audit domain mix and exclude any Tier C/D domains added recently. "
                "Check domain drill-down tab for which specific domains declined."))

    if 'SCALE' in s:
        actions.append(("info",
            f"🟢 CPA EUR{cpa:.0f} — efficient and growing. Increase bids 10-15% on Tier A domains. "
            f"Push client for more budget here."))

    if t == 'STABLE' and not pd.isna(cpa) and cpa <= target:
        actions.append(("info",
            "🔵 Healthy and consistent. Maintain current domain mix. "
            "Test 1-2 new Tier B domains without touching what is working."))

    if t == 'NEW':
        actions.append(("info",
            "⭐ New geo with early signal. Run on Tier A/B domains only. "
            "Do not scale until 3 more weeks of data."))

    if not actions:
        actions.append(("info", "Review manually — insufficient signal for automated action."))

    return actions


# ── RAG ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def fetch_rag_news(geos):
    feeds = [
        'https://www.gamblinginsider.com/feed',
        'https://sbcnews.co.uk/feed/',
        'https://calvinayre.com/feed/',
    ]
    articles = []
    for url in feeds:
        try:
            r    = requests.get(url, timeout=8, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(r.content, 'xml')
            for item in soup.find_all('item')[:20]:
                title = item.find('title')
                desc  = item.find('description')
                pub   = item.find('pubDate')
                articles.append({
                    'title':  title.get_text(strip=True) if title else '',
                    'body':   desc.get_text(strip=True)  if desc  else '',
                    'date':   pub.get_text(strip=True)   if pub   else '',
                    'source': url.split('/')[2],
                })
        except Exception:
            pass

    results = {}
    for geo in geos:
        name     = GEO_NAMES.get(geo, geo.lower())
        relevant = [a for a in articles if name in (a['title'] + a['body']).lower()]
        if not relevant:
            results[geo] = {'articles': [], 'sentiment': 'neutral'}
            continue
        scores = []
        for a in relevant:
            t   = (a['title'] + a['body']).lower()
            neg = sum(1 for k in NEGATIVE_KW if k in t)
            pos = sum(1 for k in POSITIVE_KW if k in t)
            scores.append((pos - neg) / max(pos + neg, 1))
        avg       = sum(scores) / len(scores)
        sentiment = 'positive' if avg > 0.1 else 'negative' if avg < -0.1 else 'neutral'
        results[geo] = {'articles': relevant[:3], 'sentiment': sentiment}
    return results


# ── Sidebar ───────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### FTD Intelligence")
    st.markdown("---")
    target     = st.number_input("Target CPA (EUR)", value=100, min_value=1)
    TARGET_CPA = float(target)
    st.markdown("---")
    camp_type_filter = st.multiselect(
        "Campaign type",
        options=["ACQ", "RET", "AWA"],
        default=["ACQ"]
    )
    st.markdown("---")
    run_rag = st.toggle("Fetch market news", value=False)
    st.markdown("---")
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Data refreshes automatically every 30 min.")


# ── Load data ─────────────────────────────────────────────────────────────

with st.spinner("Loading data from Google Drive and Sheets..."):
    pred_raw  = fetch_prediction_from_drive()
    md_raw    = fetch_monthly_domain_from_drive()
    bw_raw    = fetch_beeswax_sheet()
    dom_lists = fetch_domain_lists()
    dom_flags = get_domain_flags()

if pred_raw is None:
    st.error("Could not load Prediction data from Drive. Check your service account permissions and Drive folder.")
    st.stop()

acq_clean, pivot, all_months = process_prediction(pred_raw)
bw_df   = process_beeswax(bw_raw)
md_df   = process_monthly_domain(md_raw)

result  = build_combined(acq_clean, pivot, bw_df, TARGET_CPA)
if result is None:
    st.error("Could not build campaign data. Check Beeswax Sheet connection.")
    st.stop()

combined, cpa_df = result
domain_grp = score_domains(md_df, dom_lists, dom_flags)

combined_view   = combined[combined['camp_type'].isin(camp_type_filter)].copy()
domain_grp_view = domain_grp[domain_grp['camp_type'].isin(camp_type_filter)].copy() if domain_grp is not None else None

# ── KPI bar ───────────────────────────────────────────────────────────────
urgent  = combined_view[combined_view['priority'] <= 1]
scaling = combined_view[combined_view['status'] == 'SCALE — efficient + growing']
fix_cpa = combined_view[combined_view['status'] == 'FIX CPA — overspending']
med_cpa = combined_view['avg_cpa'].median()
min_date = acq_clean['date'].min().strftime('%b %d %Y')
max_date = acq_clean['date'].max().strftime('%b %d %Y')

c1, c2, c3, c4 = st.columns(4)
for col, label, val, sub, color in [
    (c1, "URGENT",    len(urgent),  "need immediate action",  "#f44336"),
    (c2, "SCALE NOW", len(scaling), "efficient + growing",    "#00c853"),
    (c3, "FIX CPA",   len(fix_cpa), "overspending campaigns", "#ff6d00"),
    (c4, "MEDIAN CPA",
         f"EUR{med_cpa:.0f}" if not pd.isna(med_cpa) else "N/A",
         f"target EUR{TARGET_CPA:.0f}",
         cpa_color(med_cpa, TARGET_CPA)),
]:
    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value" style="color:{color}">{val}</div>
        <div class="metric-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)

st.caption(f"Data: {min_date} to {max_date}  |  {len(combined_view)} campaigns  |  BF week excluded")
st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Campaign Health",
    "Domain Intelligence",
    "Domain Drill-down",
    "What To Do",
    "Market Context",
])


# ══════════════════════════════════════════════════════════════════════════
# TAB 1 — Campaign Health
# ══════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### Campaign Health Overview")

    col_f1, col_f2 = st.columns([2, 1])
    with col_f1:
        status_filter = st.multiselect("Filter by status",
            options=list(STATUS_COLORS.keys()), default=list(STATUS_COLORS.keys()))
    with col_f2:
        geo_filter = st.multiselect("Filter by geo",
            options=sorted(combined_view['geo'].unique()), default=[])

    view = combined_view[combined_view['status'].isin(status_filter)]
    if geo_filter:
        view = view[view['geo'].isin(geo_filter)]

    fig = go.Figure()
    for status_val, color in STATUS_COLORS.items():
        sub = view[view['status'] == status_val]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub['total_ftd'], y=sub['avg_cpa'],
            mode='markers', name=status_val,
            text=sub['brand'] + '_' + sub['geo'],
            customdata=sub[['camp_type','trend']],
            hovertemplate='<b>%{text}</b><br>Type: %{customdata[0]}<br>Trend: %{customdata[1]}<br>FTD: %{x:.0f}<br>CPA: EUR%{y:.1f}<extra></extra>',
            marker=dict(color=color, size=10, opacity=0.85,
                        line=dict(color='#0e1117', width=1)),
        ))

    fig.add_hline(y=TARGET_CPA, line_dash='dash', line_color='#ff6d00',
                  annotation_text=f"Target EUR{TARGET_CPA:.0f}",
                  annotation_font_color='#ff6d00')
    fig.update_layout(
        paper_bgcolor='#0e1117', plot_bgcolor='#1a1d27',
        font=dict(color='#ccc', family='IBM Plex Mono'), height=460,
        xaxis=dict(title='Total FTDs', gridcolor='#2a2d3a', zeroline=False),
        yaxis=dict(title='Avg CPA (EUR)', gridcolor='#2a2d3a', zeroline=False),
        legend=dict(bgcolor='#1a1d27', bordercolor='#2a2d3a', borderwidth=1, font=dict(size=10)),
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Monthly trend
    st.markdown("### Monthly FTD Trend — Top 10")
    month_cols = [m for m in all_months if m not in ['2026-04']]
    top10      = combined_view.nlargest(10, 'total_ftd')['brand_geo'].tolist()

    fig2 = go.Figure()
    for bg in top10:
        row = combined_view[combined_view['brand_geo'] == bg]
        if row.empty:
            continue
        row = row.iloc[0]
        monthly = acq_clean[(acq_clean['brand'] == row['brand']) &
                             (acq_clean['geo']   == row['geo'])]
        ms = monthly.groupby('month')['FTD'].sum().reset_index()
        ms['month'] = ms['month'].astype(str)
        ms = ms[ms['month'].isin(month_cols)]
        fig2.add_trace(go.Scatter(
            x=ms['month'], y=ms['FTD'], name=bg,
            mode='lines+markers',
            line=dict(color=STATUS_COLORS.get(row['status'],'#888'), width=2),
            marker=dict(size=6),
            hovertemplate=f'<b>{bg}</b><br>%{{x}}<br>FTD: %{{y:.0f}}<extra></extra>'
        ))

    fig2.update_layout(
        paper_bgcolor='#0e1117', plot_bgcolor='#1a1d27',
        font=dict(color='#ccc', family='IBM Plex Mono'), height=340,
        xaxis=dict(gridcolor='#2a2d3a'),
        yaxis=dict(title='Monthly FTD', gridcolor='#2a2d3a'),
        legend=dict(bgcolor='#1a1d27', bordercolor='#2a2d3a', font=dict(size=10)),
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Beeswax spend table
    if bw_df is not None:
        st.markdown("### Spend Summary (Beeswax)")
        bw_summary = bw_df[bw_df['camp_type'].isin(camp_type_filter)].groupby(
            ['brand','geo','camp_type','month']
        ).agg(
            spend=('Total Cost','sum'),
            imps=('Impressions','sum'),
            clicks=('Clicks','sum'),
            conversions=('Conversions','sum'),
        ).reset_index()
        bw_summary['CTR'] = (bw_summary['clicks'] / bw_summary['imps'].replace(0,np.nan) * 100).round(4)
        bw_summary['CPM'] = (bw_summary['spend'] / bw_summary['imps'].replace(0,np.nan) * 1000).round(2)
        st.dataframe(bw_summary.sort_values(['brand','geo','month']),
                     use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 2 — Domain Intelligence
# ══════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### Domain Intelligence")

    if domain_grp_view is None:
        st.warning("No Monthly Domain data available. Check Drive folder for Monthly_Domain files.")
    else:
        col_d1, col_d2, col_d3 = st.columns(3)
        with col_d1:
            sel_brand = st.selectbox("Brand", sorted(domain_grp_view['brand'].unique()))
        with col_d2:
            brand_geos = sorted(domain_grp_view[domain_grp_view['brand'] == sel_brand]['geo'].unique())
            sel_geo    = st.selectbox("Geo", brand_geos)
        with col_d3:
            wl_filter = st.selectbox("WL status",
                ['All','In WL','Not in WL','Recommended to Add','Consider Removing'])

        dview = domain_grp_view[
            (domain_grp_view['brand'] == sel_brand) &
            (domain_grp_view['geo']   == sel_geo)
        ].sort_values(['tier','ftd'], ascending=[True, False])

        if wl_filter == 'In WL':
            dview = dview[dview['is_in_wl']]
        elif wl_filter == 'Not in WL':
            dview = dview[~dview['is_in_wl']]
        elif wl_filter == 'Recommended to Add':
            dview = dview[dview['wl_recommendation'] == 'ADD TO WL']
        elif wl_filter == 'Consider Removing':
            dview = dview[dview['wl_recommendation'] == 'CONSIDER REMOVING']

        if dview.empty:
            st.warning("No domain data for this selection.")
        else:
            # Summary row
            tier_counts = dview['tier'].value_counts()
            tier_colors = {'A':'#00c853','B':'#4fc3f7','C':'#ff6d00','D':'#f44336'}

            col_s1, col_s2, col_s3, col_s4 = st.columns(4)
            col_s1.metric("Add to WL", len(dview[dview['wl_recommendation']=='ADD TO WL']))
            col_s2.metric("Keep in WL", len(dview[dview['wl_recommendation']=='KEEP IN WL']))
            col_s3.metric("Consider Removing", len(dview[dview['wl_recommendation']=='CONSIDER REMOVING']))
            col_s4.metric("Gaming Domains", len(dview[dview['is_gaming']]))

            st.markdown("---")

            # Domain table with flags
            display_cols = ['domain','tier','imp','clicks','ftd','ctr',
                           'is_in_wl','wl_names','is_gaming','wl_recommendation']
            disp = dview[display_cols].copy()
            disp.columns = ['Domain','Tier','Impressions','Clicks','FTDs','CTR%',
                           'In WL','WL Names','Gaming?','WL Recommendation']
            disp['CTR%']       = disp['CTR%'].apply(lambda x: f"{x:.4f}%" if pd.notna(x) else '-')
            disp['Impressions'] = disp['Impressions'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else '-')

            st.dataframe(disp, use_container_width=True, hide_index=True,
                        column_config={
                            'Tier': st.column_config.TextColumn(width='small'),
                            'Gaming?': st.column_config.CheckboxColumn(width='small'),
                            'In WL': st.column_config.CheckboxColumn(width='small'),
                            'WL Recommendation': st.column_config.TextColumn(width='medium'),
                        })

            # Manual domain flagging UI
            st.markdown("---")
            st.markdown("### Flag a Domain")
            st.caption("Override the automatic tier or mark a domain to exclude from WL recommendations.")

            col_fl1, col_fl2, col_fl3, col_fl4 = st.columns([2,1,1,1])
            with col_fl1:
                flag_domain = st.selectbox("Select domain to flag",
                    options=sorted(dview['domain'].unique()))
            with col_fl2:
                manual_tier = st.selectbox("Manual tier override",
                    options=['No override','A','B','C','D'])
            with col_fl3:
                exclude_wl = st.checkbox("Exclude from WL recommendations")
            with col_fl4:
                flag_note = st.text_input("Note (optional)")

            if st.button("Save flag"):
                flags = get_domain_flags()
                flags[flag_domain.lower()] = {
                    'manual_tier':    manual_tier if manual_tier != 'No override' else None,
                    'exclude_from_wl': exclude_wl,
                    'note':           flag_note,
                    'flagged_at':     datetime.utcnow().isoformat(),
                }
                save_domain_flags(flags)
                st.success(f"Flagged {flag_domain}")
                st.cache_data.clear()
                st.rerun()

            # Show existing flags
            if dom_flags:
                st.markdown("**Current manual flags:**")
                flags_df = pd.DataFrame([
                    {'domain': k, **v} for k, v in dom_flags.items()
                ])
                st.dataframe(flags_df, use_container_width=True, hide_index=True)

                if st.button("Clear all flags"):
                    save_domain_flags({})
                    st.cache_data.clear()
                    st.rerun()


# ══════════════════════════════════════════════════════════════════════════
# TAB 3 — Domain Drill-down
# ══════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### Domain Drill-down")
    st.caption("Select a domain to see its performance trend, why it changed, and what to do next.")

    if domain_grp_view is None or md_df is None:
        st.warning("No Monthly Domain data available.")
    else:
        col_dd1, col_dd2, col_dd3 = st.columns(3)
        with col_dd1:
            dd_brand = st.selectbox("Brand", sorted(domain_grp_view['brand'].unique()), key='dd_brand')
        with col_dd2:
            dd_geos  = sorted(domain_grp_view[domain_grp_view['brand']==dd_brand]['geo'].unique())
            dd_geo   = st.selectbox("Geo", dd_geos, key='dd_geo')
        with col_dd3:
            dd_domains = sorted(domain_grp_view[
                (domain_grp_view['brand']==dd_brand) &
                (domain_grp_view['geo']==dd_geo)
            ]['domain'].unique())
            dd_domain = st.selectbox("Domain", dd_domains, key='dd_domain')

        # Domain summary card
        dd_row = domain_grp_view[
            (domain_grp_view['brand']  == dd_brand) &
            (domain_grp_view['geo']    == dd_geo)   &
            (domain_grp_view['domain'] == dd_domain)
        ]

        if not dd_row.empty:
            dd_row = dd_row.iloc[0]
            col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
            tier_color = {'A':'#00c853','B':'#4fc3f7','C':'#ff6d00','D':'#f44336'}.get(dd_row['tier'],'#666')
            col_m1.markdown(f"""<div class="metric-card">
                <div class="metric-label">Tier</div>
                <div class="metric-value" style="color:{tier_color}">{dd_row['tier']}</div>
                <div class="metric-sub">{dd_row['tier_reason']}</div>
            </div>""", unsafe_allow_html=True)
            col_m2.markdown(f"""<div class="metric-card">
                <div class="metric-label">Total FTDs</div>
                <div class="metric-value">{dd_row['ftd']:.0f}</div>
                <div class="metric-sub">across all months</div>
            </div>""", unsafe_allow_html=True)
            col_m3.markdown(f"""<div class="metric-card">
                <div class="metric-label">CTR</div>
                <div class="metric-value">{dd_row['ctr']:.4f}%</div>
                <div class="metric-sub">avg impressions: {dd_row['imp']:,.0f}</div>
            </div>""", unsafe_allow_html=True)
            col_m4.markdown(f"""<div class="metric-card">
                <div class="metric-label">In WL?</div>
                <div class="metric-value" style="color:{'#00c853' if dd_row['is_in_wl'] else '#666'}">
                    {'YES' if dd_row['is_in_wl'] else 'NO'}</div>
                <div class="metric-sub">{dd_row['wl_names'] if dd_row['wl_names'] else 'not in any list'}</div>
            </div>""", unsafe_allow_html=True)
            col_m5.markdown(f"""<div class="metric-card">
                <div class="metric-label">Recommendation</div>
                <div class="metric-value" style="font-size:14px;color:{tier_color}">
                    {dd_row['wl_recommendation']}</div>
                <div class="metric-sub">{'Gaming domain' if dd_row['is_gaming'] else ''}</div>
            </div>""", unsafe_allow_html=True)

            st.markdown("---")

            # Monthly trend chart
            trend_data = domain_trend_analysis(md_df, dd_domain, dd_brand, dd_geo)
            if trend_data is not None and not trend_data.empty:
                st.markdown("**Performance by month**")
                fig_dt = go.Figure()
                fig_dt.add_trace(go.Bar(
                    x=trend_data['source_file'], y=trend_data['ftd'],
                    name='FTDs', marker_color='#00c853', opacity=0.8,
                    hovertemplate='%{x}<br>FTDs: %{y:.0f}<extra></extra>'
                ))
                fig_dt.add_trace(go.Scatter(
                    x=trend_data['source_file'], y=trend_data['ctr'],
                    name='CTR%', yaxis='y2', line=dict(color='#4fc3f7', width=2),
                    hovertemplate='%{x}<br>CTR: %{y:.4f}%<extra></extra>'
                ))
                fig_dt.update_layout(
                    paper_bgcolor='#0e1117', plot_bgcolor='#1a1d27',
                    font=dict(color='#ccc', family='IBM Plex Mono'), height=300,
                    xaxis=dict(gridcolor='#2a2d3a'),
                    yaxis=dict(title='FTDs', gridcolor='#2a2d3a'),
                    yaxis2=dict(title='CTR%', overlaying='y', side='right'),
                    legend=dict(bgcolor='#1a1d27'),
                    margin=dict(l=40,r=40,t=20,b=40),
                )
                st.plotly_chart(fig_dt, use_container_width=True)

                # Why did it drop?
                if len(trend_data) >= 2:
                    last_ftd  = trend_data.iloc[-1]['ftd']
                    prev_ftd  = trend_data.iloc[-2]['ftd']
                    last_ctr  = trend_data.iloc[-1]['ctr']
                    prev_ctr  = trend_data.iloc[-2]['ctr']

                    st.markdown("**Automated diagnosis**")
                    if last_ftd < prev_ftd * 0.5:
                        if last_ctr < prev_ctr * 0.7:
                            st.warning(
                                f"FTDs dropped {prev_ftd:.0f} → {last_ftd:.0f} AND CTR dropped {prev_ctr:.4f}% → {last_ctr:.4f}%. "
                                f"Both signals declining — likely this domain's audience quality dropped or you lost impression share. "
                                f"Consider reducing bid or removing from WL if trend continues next month.")
                        else:
                            st.warning(
                                f"FTDs dropped {prev_ftd:.0f} → {last_ftd:.0f} but CTR held steady. "
                                f"Users are still clicking but not converting — possible landing page issue, "
                                f"offer mismatch, or increased competition for same users on this domain.")
                    elif last_ftd > prev_ftd * 1.3:
                        st.success(
                            f"FTDs grew {prev_ftd:.0f} → {last_ftd:.0f}. "
                            f"Domain is improving. If not already in WL, consider adding it.")
                    else:
                        st.info(f"FTDs stable: {prev_ftd:.0f} → {last_ftd:.0f}. No significant change detected.")

            # Manual flag for this domain
            st.markdown("---")
            st.markdown("**Flag this domain**")
            col_f1, col_f2, col_f3 = st.columns([1,1,2])
            with col_f1:
                mt = st.selectbox("Tier override", ['No override','A','B','C','D'], key='dd_tier')
            with col_f2:
                ex = st.checkbox("Exclude from WL recs", key='dd_exclude')
            with col_f3:
                nt = st.text_input("Note", key='dd_note')
            if st.button("Save flag", key='dd_save'):
                flags = get_domain_flags()
                flags[dd_domain.lower()] = {
                    'manual_tier':     mt if mt != 'No override' else None,
                    'exclude_from_wl': ex,
                    'note':            nt,
                    'flagged_at':      datetime.utcnow().isoformat(),
                }
                save_domain_flags(flags)
                st.success(f"Flagged {dd_domain}")
                st.cache_data.clear()
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════
# TAB 4 — What To Do
# ══════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("### Action Plan")
    st.caption("Ordered by priority. Answer questions to get specific next steps.")

    priority_groups = [
        ('URGENT',   ['URGENT — signal lost','URGENT — overspend + declining']),
        ('FIX CPA',  ['FIX CPA — overspending']),
        ('WATCH',    ['WATCH — declining','WATCH — new signal']),
        ('SCALE',    ['SCALE — efficient + growing']),
        ('MAINTAIN', ['MAINTAIN — stable']),
    ]
    group_colors = {
        'URGENT':'#f44336','FIX CPA':'#ff6d00',
        'WATCH':'#ffd600','SCALE':'#00c853','MAINTAIN':'#4fc3f7'
    }

    for group_label, statuses in priority_groups:
        group_df = combined_view[combined_view['status'].isin(statuses)]
        if group_df.empty:
            continue
        color = group_colors[group_label]
        st.markdown(f"""
        <div class="section-header" style="color:{color}">
            {group_label} — {len(group_df)} campaigns
        </div>""", unsafe_allow_html=True)

        for _, row in group_df.iterrows():
            bg        = row['brand_geo']
            trend_sym = TREND_EMOJI.get(row['trend'],'?')
            cpa_val   = row['avg_cpa']
            cpa_str   = f"EUR{cpa_val:.0f}" if pd.notna(cpa_val) else "N/A"
            col       = cpa_color(cpa_val, TARGET_CPA)

            with st.expander(
                f"{bg}  |  {trend_sym} {row['trend']}  |  CPA {cpa_str}  |  {row['total_ftd']:.0f} FTDs"
            ):
                ans_key = f"answers_{bg}"
                if ans_key not in st.session_state:
                    st.session_state[ans_key] = {}

                actions = action_text(row, st.session_state[ans_key], TARGET_CPA)
                for act in actions:
                    if isinstance(act, tuple) and act[0] == "question":
                        _, q_id, q_text, q_options = act
                        st.markdown(q_text)
                        btn_cols = st.columns(len(q_options))
                        for i, opt in enumerate(q_options):
                            if btn_cols[i].button(opt, key=f"{bg}_{q_id}_{i}"):
                                st.session_state[ans_key][f"{bg}_{q_id}"] = opt
                                st.rerun()
                    else:
                        msg = act[1] if isinstance(act, tuple) else act
                        st.markdown(msg)

                st.markdown("---")

                if domain_grp_view is not None:
                    d_brand = domain_grp_view[
                        (domain_grp_view['brand'] == row['brand']) &
                        (domain_grp_view['geo']   == row['geo'])
                    ]
                    if not d_brand.empty:
                        ca, cb, cc = st.columns(3)
                        with ca:
                            st.markdown("**Include (Tier A)**")
                            top_a = d_brand[d_brand['tier']=='A'].nlargest(5,'ftd')
                            if top_a.empty:
                                st.caption("No Tier A domains yet")
                            for _, dr in top_a.iterrows():
                                wl_tag = " ✓" if dr['is_in_wl'] else " ⚡ NEW"
                                st.markdown(f"""<div class="domain-a">
                                    {dr['domain']}{wl_tag}<br>
                                    <span style="color:#666;font-size:10px">
                                    {dr['ftd']:.0f} FTDs | CTR {dr['ctr']:.4f}%</span>
                                </div>""", unsafe_allow_html=True)
                        with cb:
                            st.markdown("**Test (Tier B)**")
                            top_b = d_brand[d_brand['tier']=='B'].nlargest(5,'clicks')
                            if top_b.empty:
                                st.caption("No Tier B domains")
                            for _, dr in top_b.iterrows():
                                wl_tag = " ✓" if dr['is_in_wl'] else ""
                                st.markdown(f"""<div class="domain-b">
                                    {dr['domain']}{wl_tag}<br>
                                    <span style="color:#666;font-size:10px">
                                    CTR {dr['ctr']:.4f}%</span>
                                </div>""", unsafe_allow_html=True)
                        with cc:
                            st.markdown("**Exclude (Tier C/D)**")
                            top_c = d_brand[d_brand['tier'].isin(['C','D'])].nlargest(5,'imp')
                            if top_c.empty:
                                st.caption("No domains to exclude")
                            for _, dr in top_c.iterrows():
                                wl_tag = " ⚠ IN WL" if dr['is_in_wl'] else ""
                                st.markdown(f"""<div class="domain-c">
                                    {dr['domain']}{wl_tag}<br>
                                    <span style="color:#666;font-size:10px">
                                    {dr['imp']:,.0f} imps | CTR {dr['ctr']:.4f}%</span>
                                </div>""", unsafe_allow_html=True)

                # CPA sparkline
                bg_daily = cpa_df[
                    (cpa_df['brand'] == row['brand']) &
                    (cpa_df['geo']   == row['geo'])
                ].copy()
                bg_daily['cpa'] = bg_daily['spend'] / bg_daily['FTD'].replace(0, np.nan)
                bg_daily = bg_daily.dropna(subset=['cpa'])

                if len(bg_daily) > 2:
                    fig_s = go.Figure()
                    fig_s.add_hline(y=TARGET_CPA, line_dash='dash',
                                    line_color='#ff6d00', opacity=0.6)
                    fig_s.add_trace(go.Scatter(
                        x=bg_daily['month'], y=bg_daily['cpa'],
                        mode='lines+markers',
                        line=dict(color=col, width=2), marker=dict(size=5),
                        fill='tozeroy', fillcolor='rgba(80,80,80,0.1)',
                        hovertemplate='%{x}<br>CPA: EUR%{y:.1f}<extra></extra>'
                    ))
                    fig_s.update_layout(
                        paper_bgcolor='#0e1117', plot_bgcolor='#1a1d27',
                        height=150, margin=dict(l=30,r=10,t=10,b=30),
                        font=dict(color='#aaa', size=10, family='IBM Plex Mono'),
                        xaxis=dict(gridcolor='#2a2d3a'),
                        yaxis=dict(gridcolor='#2a2d3a', title='CPA'),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_s, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 5 — Market Context
# ══════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown("### Market Context — Gambling News by Geo")
    st.caption("Scraped from gamblinginsider.com, sbcnews.co.uk, calvinayre.com.")

    active_geos = sorted(combined_view['geo'].unique())

    if not run_rag:
        st.info("Enable 'Fetch market news' in the sidebar to load geo-level context.")
    else:
        with st.spinner("Fetching news..."):
            rag_data = fetch_rag_news(tuple(active_geos))

        pos = sum(1 for g in rag_data.values() if g['sentiment']=='positive')
        neg = sum(1 for g in rag_data.values() if g['sentiment']=='negative')
        neu = sum(1 for g in rag_data.values() if g['sentiment']=='neutral')

        c1, c2, c3 = st.columns(3)
        c1.metric("Positive market signal", pos, f"of {len(active_geos)} geos")
        c2.metric("Negative/restrictive",   neg, f"of {len(active_geos)} geos")
        c3.metric("No recent news",         neu, f"of {len(active_geos)} geos")
        st.markdown("---")

        for geo in active_geos:
            data      = rag_data.get(geo, {})
            sentiment = data.get('sentiment','neutral')
            articles  = data.get('articles',[])
            color     = {'positive':'#00c853','negative':'#f44336','neutral':'#666'}[sentiment]
            emoji     = {'positive':'🟢','negative':'🔴','neutral':'⚪'}[sentiment]

            with st.expander(f"{emoji} {geo} — {GEO_NAMES.get(geo,geo).title()} ({sentiment.upper()})"):
                if not articles:
                    st.caption("No recent gambling-specific news found.")
                else:
                    for art in articles:
                        st.markdown(f"""
                        <div class="rag-card rag-{sentiment}">
                            <div style="font-weight:600;font-size:13px">{art['title'][:120]}</div>
                            <div style="font-size:11px;color:#666;margin-top:4px">
                                {art['source']} · {art['date'][:16]}</div>
                            <div style="font-size:12px;color:#aaa;margin-top:6px">
                                {art['body'][:200]}...</div>
                        </div>""", unsafe_allow_html=True)

                impact = combined_view[combined_view['geo']==geo][
                    ['brand','total_ftd','avg_cpa','status']].head(5)
                if not impact.empty:
                    st.markdown("**Your campaigns in this geo:**")
                    st.dataframe(impact, use_container_width=True, hide_index=True)
