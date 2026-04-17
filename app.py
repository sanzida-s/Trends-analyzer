"""
FTD Intelligence Dashboard — Datawrkz
--------------------------------------
Run in Colab:

    !pip install streamlit plotly pandas numpy requests beautifulsoup4 -q
    !wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O cloudflared
    !chmod +x cloudflared

    import subprocess, time
    proc = subprocess.Popen(
        ["streamlit", "run", "/content/app.py",
         "--server.port", "8501", "--server.headless", "true",
         "--server.enableCORS", "false", "--server.enableXsrfProtection", "false",
         "--server.address", "0.0.0.0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(6)

    cf = subprocess.Popen(["./cloudflared","tunnel","--url","http://localhost:8501"],
        stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    time.sleep(15)
    output = cf.stderr.read1(4096).decode()
    for line in output.split('\\n'):
        if 'trycloudflare.com' in line and 'https://' in line:
            print("URL:", line[line.find('https://'):].strip())
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import requests
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')

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
.metric-sub { font-size:12px; color:#666; }
.domain-a { background:#0d2b1a; border-radius:4px; padding:6px 10px; margin:2px 0; font-family:'IBM Plex Mono',monospace; font-size:12px; }
.domain-b { background:#1a1d27; border-radius:4px; padding:6px 10px; margin:2px 0; font-family:'IBM Plex Mono',monospace; font-size:12px; }
.domain-c { background:#2b1a00; border-radius:4px; padding:6px 10px; margin:2px 0; font-family:'IBM Plex Mono',monospace; font-size:12px; }
.rag-card { background:#1a1d27; border:1px solid #2a2d3a; border-radius:6px; padding:12px 16px; margin:6px 0; }
.rag-positive { border-left:3px solid #00c853; }
.rag-negative { border-left:3px solid #f44336; }
.rag-neutral  { border-left:3px solid #666; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────
BF_WEEK = 48

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
    'GROWING': '↑', 'DECLINING': '↓', 'STABLE': '→',
    'DROPPED': '✕', 'NEW': '★', 'DEAD': '—'
}

ADTECH = [
    'doubleclick','googlesyndication','advertising','taboola','outbrain',
    'criteo','appnexus','rubiconproject','openx','pubmatic','adform',
    'xandr','moatads','doubleverify','ias.net','bidswitch',
]

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


# ── Helpers ───────────────────────────────────────────────────────────────

def parse_campaign(x):
    s = str(x).strip()
    parts = s.split('_')
    brand = parts[0] if len(parts) > 0 else 'UNK'
    geo   = parts[1].strip() if len(parts) > 1 else 'UNK'
    if 'Acquisition' in s:   camp_type = 'ACQ'
    elif 'Retention' in s:   camp_type = 'RET'
    elif 'Awareness' in s:   camp_type = 'AWA'
    else:                    camp_type = 'UNK'
    return brand, geo, camp_type


def cpa_color(cpa, target):
    if pd.isna(cpa):              return '#666'
    if cpa <= target * 0.5:      return '#00c853'
    if cpa <= target:            return '#ffd600'
    return '#f44336'


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
                "🟡 Paused intentionally. No action needed. When reactivating, monitor first 48h closely and start with conservative bids."))
        else:
            spend_ans = answers.get(f'{bg}_spend_check')
            if spend_ans is None:
                actions.append(("question", "spend_check",
                    "🔴 Campaign should be running. Did you check if budget was exhausted in Beeswax?",
                    ["Yes — budget exhausted", "No — budget is available", "Not checked yet"]))
            elif spend_ans == "Yes — budget exhausted":
                actions.append(("info",
                    "🔴 Request budget top-up from client. Do not reallocate from other campaigns until confirmed."))
            elif spend_ans == "No — budget is available":
                actions.append(("info",
                    "🔴 Budget available but no delivery. Check Beeswax for bid floor issues, creative disapprovals, or domain blocklist conflicts. Escalate immediately."))
            else:
                actions.append(("info",
                    "🔴 Check Beeswax delivery report first — look at win rate and bid rejections, then come back here."))

    if 'URGENT — overspend + declining' in s:
        pct = ((cpa / target) - 1) * 100 if target > 0 else 0
        actions.append(("info",
            f"🔴 CPA at EUR{cpa:.0f} — {pct:.0f}% above target and FTDs are declining. "
            f"Reduce bids 20-30% on Tier C/D domains immediately. Flag to client."))

    if 'FIX CPA — overspending' in s:
        ans = answers.get(f'{bg}_domain_check')
        if ans is None:
            actions.append(("question", "domain_check",
                f"🟠 CPA EUR{cpa:.0f} — above target but FTDs are growing. Have you already excluded Tier C/D domains?",
                ["Yes — already excluded", "No — not yet"]))
        elif ans == "No — not yet":
            actions.append(("info",
                "🟠 Exclude all Tier C/D domains listed below. Re-check CPA after 7 days. "
                "If still above target, reduce bids by 15% on Tier B domains."))
        else:
            actions.append(("info",
                "🟠 Tier C/D already excluded. Next: reduce bids 15% on Tier B and concentrate budget on Tier A only."))

    if t == 'DECLINING' and not pd.isna(cpa) and cpa <= target:
        ans = answers.get(f'{bg}_spend_trend')
        if ans is None:
            actions.append(("question", "spend_trend",
                "🟡 FTDs declining but CPA is healthy. Did spend also drop in the same period?",
                ["Yes — spend dropped too", "No — spend held steady"]))
        elif ans == "Yes — spend dropped too":
            actions.append(("info",
                "🟡 Likely a budget reduction. Confirm with client. If unintentional, request restoration. "
                "CPA is healthy so scaling back up is low risk."))
        else:
            actions.append(("info",
                "🟡 Spend held but FTDs dropped — targeting quality issue. "
                "Audit domain mix and exclude any Tier C/D domains added recently."))

    if 'SCALE' in s:
        actions.append(("info",
            f"🟢 CPA EUR{cpa:.0f} — efficient and growing. Increase bids 10-15% on Tier A domains. "
            f"Push client for more budget here — this is your best performing combo."))

    if t == 'STABLE' and not pd.isna(cpa) and cpa <= target:
        actions.append(("info",
            "🔵 Healthy and consistent. Maintain current domain mix. "
            "Test 1-2 new Tier B domains without touching what is already working."))

    if t == 'NEW':
        actions.append(("info",
            "⭐ New geo with early signal. Run conservatively on Tier A/B domains only. "
            "Do not scale until 3 more weeks of data accumulate."))

    if not actions:
        actions.append(("info", "Review manually — insufficient signal for automated action."))

    return actions


# ── Data loading ──────────────────────────────────────────────────────────

@st.cache_data
def load_data(pred_path, spend_path, md_feb_path, md_mar_path, target_cpa):

    # Prediction
    pred = pd.read_csv(pred_path)
    pred.columns = [c.strip() for c in pred.columns]
    parsed = pred['Campaign'].apply(
        lambda x: pd.Series(parse_campaign(x), index=['brand','geo','camp_type']))
    pred = pd.concat([pred, parsed], axis=1)
    pred['date']  = pd.to_datetime(pred['Date'], dayfirst=False, infer_datetime_format=True)
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

    # Spend
    spend = pd.read_csv(spend_path)
    spend.columns = [c.strip() for c in spend.columns]
    spend['date']  = pd.to_datetime(spend['date'], dayfirst=False, infer_datetime_format=True)
    parsed_s = spend['insertion_order'].apply(
        lambda x: pd.Series(parse_campaign(x), index=['brand','geo','camp_type']))
    spend    = pd.concat([spend, parsed_s], axis=1)
    spend['month'] = spend['date'].dt.to_period('M').astype(str)
    spend_acq = spend[spend['camp_type'] == 'ACQ']

    spend_m = spend_acq.groupby(['brand','geo','month'])['total_cost'].sum().reset_index()
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
    combined['brand_geo']  = combined['brand'] + '_' + combined['geo']
    combined['camp_type']  = 'ACQ'

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

    # Monthly Domain
    def load_md(path, month_label):
        df = pd.read_csv(path, low_memory=False)
        df.columns = [c.strip() for c in df.columns]
        if 'Domain' in df.columns:
            df = df.rename(columns={'Domain': 'domain'})
        elif 'Banner Domain (2nd level)' in df.columns:
            df = df.rename(columns={'Banner Domain (2nd level)': 'domain'})
        for col in ['Tracked Ads','Clicks','FTD','DEP','REG']:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(',','').str.replace('%',''),
                    errors='coerce').fillna(0)
        parsed_m = df['Campaign'].apply(
            lambda x: pd.Series(parse_campaign(x), index=['brand','geo','camp_type']))
        df = pd.concat([df.reset_index(drop=True), parsed_m.reset_index(drop=True)], axis=1)
        df['month'] = month_label
        return df

    md     = pd.concat([load_md(md_feb_path,'Feb'), load_md(md_mar_path,'Mar')], ignore_index=True)
    md_acq = md[md['camp_type'] == 'ACQ'].copy()

    domain_grp = md_acq.groupby(['brand','geo','domain','camp_type']).agg(
        imp=('Tracked Ads','sum'),
        clicks=('Clicks','sum'),
        ftd=('FTD','sum'),
    ).reset_index()
    domain_grp['ctr']        = domain_grp['clicks'] / domain_grp['imp'].replace(0, np.nan) * 100
    domain_grp['ftd_per_1k'] = domain_grp['ftd']    / domain_grp['imp'].replace(0, np.nan) * 1000

    geo_thresholds = {}
    for geo, gdf in domain_grp.groupby('geo'):
        valid = gdf[(gdf['ctr'].notna()) & (gdf['imp'] >= 500)]
        if len(valid) >= 5:
            geo_thresholds[geo] = {
                'p25': valid['ctr'].quantile(0.25),
                'p75': valid['ctr'].quantile(0.75),
            }

    def assign_tier(r):
        d = str(r['domain']).lower()
        if any(p in d for p in ADTECH):             return 'D', 'adtech'
        if r['imp'] < 500 or pd.isna(r['ctr']):    return 'D', 'low_data'
        t = geo_thresholds.get(r['geo'])
        if t is None:                               return 'B', 'no_geo_threshold'
        if r['ctr'] >= t['p75'] and r['ftd'] > 0:  return 'A', 'high_ctr+ftd'
        if r['ctr'] >= t['p75']:                    return 'B', 'high_ctr_no_ftd'
        if r['ctr'] < t['p25']:                     return 'C', 'low_ctr'
        if r['ftd'] > 0:                            return 'A', 'mid_ctr+ftd'
        return 'B', 'mid_ctr'

    tiers = domain_grp.apply(assign_tier, axis=1)
    domain_grp['tier']        = tiers.apply(lambda x: x[0])
    domain_grp['tier_reason'] = tiers.apply(lambda x: x[1])

    return combined, domain_grp, acq_clean, cpa_df, all_months


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
            results[geo] = {'articles': [], 'sentiment': 'neutral', 'modifier': 0}
            continue
        scores = []
        for a in relevant:
            t   = (a['title'] + a['body']).lower()
            neg = sum(1 for k in NEGATIVE_KW if k in t)
            pos = sum(1 for k in POSITIVE_KW if k in t)
            scores.append((pos - neg) / max(pos + neg, 1))
        avg       = sum(scores) / len(scores)
        sentiment = 'positive' if avg > 0.1 else 'negative' if avg < -0.1 else 'neutral'
        results[geo] = {'articles': relevant[:3], 'sentiment': sentiment, 'modifier': round(avg, 2)}
    return results


# ── Sidebar ───────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### FTD Intelligence")
    st.markdown("---")
    st.markdown("**Upload your files**")
    pred_file  = st.file_uploader("Prediction.csv",         type='csv', key='pred')
    spend_file = st.file_uploader("Spends.csv",             type='csv', key='spend')
    md_feb     = st.file_uploader("Monthly_Domain_Feb.csv", type='csv', key='feb')
    md_mar     = st.file_uploader("Monthly_Domain_Mar.csv", type='csv', key='mar')
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
    run_rag = st.toggle("Fetch market news (RAG)", value=False)
    st.caption("Scrapes gambling news sites. Takes ~30s.")


# ── Gate ──────────────────────────────────────────────────────────────────

if not all([pred_file, spend_file, md_feb, md_mar]):
    st.markdown("## Upload your 4 files in the sidebar to get started")
    st.markdown("""
| File | What it is |
|---|---|
| `Prediction.csv` | Adform daily FTDs — your ground truth |
| `Spends.csv` | Daily spend per campaign from Beeswax |
| `Monthly_Domain_Feb.csv` | Adform domain breakdown for February |
| `Monthly_Domain_Mar.csv` | Adform domain breakdown for March |
""")
    st.stop()

with st.spinner("Loading and processing data..."):
    combined, domain_grp, acq_clean, cpa_df, all_months = load_data(
        pred_file, spend_file, md_feb, md_mar, TARGET_CPA
    )

combined_view   = combined[combined['camp_type'].isin(camp_type_filter)].copy()
domain_grp_view = domain_grp[domain_grp['camp_type'].isin(camp_type_filter)].copy()

# ── KPI bar ───────────────────────────────────────────────────────────────
urgent  = combined_view[combined_view['priority'] <= 1]
scaling = combined_view[combined_view['status'] == 'SCALE — efficient + growing']
fix_cpa = combined_view[combined_view['status'] == 'FIX CPA — overspending']
med_cpa = combined_view['avg_cpa'].median()

min_date = acq_clean['date'].min().strftime('%b %d %Y')
max_date = acq_clean['date'].max().strftime('%b %d %Y')

c1, c2, c3, c4 = st.columns(4)
for col, label, val, sub, color in [
    (c1, "URGENT",     len(urgent),   "need immediate action",  "#f44336"),
    (c2, "SCALE NOW",  len(scaling),  "efficient + growing",    "#00c853"),
    (c3, "FIX CPA",    len(fix_cpa),  "overspending campaigns", "#ff6d00"),
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

st.caption(f"Data: {min_date} to {max_date}  |  {len(combined_view)} campaigns  |  Black Friday week excluded")
st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs([
    "Campaign Health",
    "Domain Intel",
    "What To Do",
    "Market Context",
])


# ══════════════════════════════════════════════════════════════════════════
# TAB 1
# ══════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### Campaign Health")

    col_f1, col_f2 = st.columns([2, 1])
    with col_f1:
        status_filter = st.multiselect(
            "Filter by status",
            options=list(STATUS_COLORS.keys()),
            default=list(STATUS_COLORS.keys()),
        )
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
            x=sub['total_ftd'],
            y=sub['avg_cpa'],
            mode='markers',
            name=status_val,
            text=sub['brand'] + '_' + sub['geo'],
            customdata=sub[['camp_type', 'trend']],
            hovertemplate='<b>%{text}</b><br>Type: %{customdata[0]}<br>Trend: %{customdata[1]}<br>FTD: %{x:.0f}<br>CPA: EUR%{y:.1f}<extra></extra>',
            marker=dict(color=color, size=10, opacity=0.85,
                        line=dict(color='#0e1117', width=1)),
        ))

    fig.add_hline(y=TARGET_CPA, line_dash='dash', line_color='#ff6d00',
                  annotation_text=f"Target EUR{TARGET_CPA:.0f}",
                  annotation_font_color='#ff6d00')
    fig.update_layout(
        paper_bgcolor='#0e1117', plot_bgcolor='#1a1d27',
        font=dict(color='#ccc', family='IBM Plex Mono'), height=480,
        xaxis=dict(title='Total FTDs', gridcolor='#2a2d3a', zeroline=False),
        yaxis=dict(title='Avg CPA (EUR)', gridcolor='#2a2d3a', zeroline=False),
        legend=dict(bgcolor='#1a1d27', bordercolor='#2a2d3a', borderwidth=1, font=dict(size=10)),
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Monthly FTD Trend — Top 10 Campaigns")
    month_cols = [m for m in all_months if m != '2026-04']
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
            line=dict(color=STATUS_COLORS.get(row['status'], '#888'), width=2),
            marker=dict(size=6),
            hovertemplate=f'<b>{bg}</b><br>%{{x}}<br>FTD: %{{y:.0f}}<extra></extra>'
        ))

    fig2.update_layout(
        paper_bgcolor='#0e1117', plot_bgcolor='#1a1d27',
        font=dict(color='#ccc', family='IBM Plex Mono'), height=360,
        xaxis=dict(gridcolor='#2a2d3a'),
        yaxis=dict(title='Monthly FTD', gridcolor='#2a2d3a'),
        legend=dict(bgcolor='#1a1d27', bordercolor='#2a2d3a', font=dict(size=10)),
        margin=dict(l=40, r=20, t=20, b=40),
    )
    st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 2
# ══════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### Domain Intelligence")
    st.caption("Tier A = scale  |  Tier B = test  |  Tier C = exclude  |  Tier D = always block")

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        sel_brand = st.selectbox("Brand", sorted(domain_grp_view['brand'].unique()))
    with col_d2:
        brand_geos = sorted(domain_grp_view[domain_grp_view['brand'] == sel_brand]['geo'].unique())
        sel_geo    = st.selectbox("Geo", brand_geos)

    dview = domain_grp_view[
        (domain_grp_view['brand'] == sel_brand) &
        (domain_grp_view['geo']   == sel_geo)
    ].sort_values(['tier','ftd'], ascending=[True, False])

    if dview.empty:
        st.warning("No domain data for this brand+geo combination.")
    else:
        tier_counts = dview['tier'].value_counts()
        tier_colors = {'A':'#00c853','B':'#4fc3f7','C':'#ff6d00','D':'#f44336'}

        col_p1, col_p2 = st.columns([1, 2])
        with col_p1:
            fig_d = go.Figure(go.Pie(
                labels=[f"Tier {t}" for t in tier_counts.index],
                values=tier_counts.values, hole=0.6,
                marker_colors=[tier_colors.get(t,'#888') for t in tier_counts.index],
                textinfo='label+percent',
                textfont=dict(size=11, family='IBM Plex Mono'),
            ))
            fig_d.update_layout(
                paper_bgcolor='#0e1117', font=dict(color='#ccc'),
                height=220, margin=dict(l=0,r=0,t=20,b=0), showlegend=False,
            )
            st.plotly_chart(fig_d, use_container_width=True)

        with col_p2:
            for tier_val, label, color, desc in [
                ('A','TIER A — SCALE',   '#00c853','High CTR + confirmed FTDs. Prioritise here.'),
                ('B','TIER B — TEST',    '#4fc3f7','Decent CTR, no confirmed FTD yet.'),
                ('C','TIER C — EXCLUDE', '#ff6d00','Low CTR, view-heavy. Not driving clicks.'),
                ('D','TIER D — BLOCK',   '#f44336','Ad-tech or insufficient data.'),
            ]:
                count = tier_counts.get(tier_val, 0)
                ftd_t = dview[dview['tier'] == tier_val]['ftd'].sum()
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:12px;
                            background:#1a1d27;border-left:3px solid {color};
                            border-radius:4px;padding:8px 12px;margin:4px 0;">
                    <div style="color:{color};font-family:'IBM Plex Mono';font-weight:600;min-width:100px">{label}</div>
                    <div style="font-size:12px;color:#aaa;flex:1">{desc}</div>
                    <div style="font-family:'IBM Plex Mono';font-size:13px;color:{color}">{count} | {ftd_t:.0f} FTDs</div>
                </div>""", unsafe_allow_html=True)

        for tier_val in ['A','B','C']:
            t_data = dview[dview['tier'] == tier_val].head(15)
            if t_data.empty:
                continue
            color = tier_colors.get(tier_val, '#888')
            st.markdown(f"**<span style='color:{color}'>Tier {tier_val} domains</span>**",
                        unsafe_allow_html=True)
            display = t_data[['domain','imp','clicks','ftd','ctr','ftd_per_1k']].copy()
            display.columns = ['Domain','Impressions','Clicks','FTDs','CTR %','FTD/1k imp']
            display['CTR %']       = display['CTR %'].apply(
                lambda x: f"{x:.4f}%" if pd.notna(x) else '-')
            display['FTD/1k imp']  = display['FTD/1k imp'].apply(
                lambda x: f"{x:.5f}" if pd.notna(x) else '-')
            display['Impressions'] = display['Impressions'].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) else '-')
            st.dataframe(display, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 3
# ══════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### Action Plan")
    st.caption("Ordered by priority. Answer the questions to get specific next steps.")

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
        <div style="font-family:'IBM Plex Mono';font-size:12px;color:{color};
                    text-transform:uppercase;letter-spacing:2px;
                    margin:20px 0 8px 0;border-bottom:1px solid {color}33;padding-bottom:4px;">
            {group_label} — {len(group_df)} campaigns
        </div>""", unsafe_allow_html=True)

        for _, row in group_df.iterrows():
            bg        = row['brand_geo']
            trend_sym = TREND_EMOJI.get(row['trend'], '?')
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

                d_brand = domain_grp_view[
                    (domain_grp_view['brand'] == row['brand']) &
                    (domain_grp_view['geo']   == row['geo'])
                ]
                if not d_brand.empty:
                    ca, cb, cc = st.columns(3)
                    with ca:
                        st.markdown("**Include (Tier A)**")
                        top_a = d_brand[d_brand['tier'] == 'A'].nlargest(5,'ftd')
                        if top_a.empty:
                            st.caption("No Tier A domains yet")
                        for _, dr in top_a.iterrows():
                            st.markdown(f"""<div class="domain-a">
                                {dr['domain']}<br>
                                <span style="color:#666;font-size:10px">
                                {dr['ftd']:.0f} FTDs | CTR {dr['ctr']:.4f}%</span>
                            </div>""", unsafe_allow_html=True)
                    with cb:
                        st.markdown("**Test (Tier B)**")
                        top_b = d_brand[d_brand['tier'] == 'B'].nlargest(5,'clicks')
                        if top_b.empty:
                            st.caption("No Tier B domains")
                        for _, dr in top_b.iterrows():
                            st.markdown(f"""<div class="domain-b">
                                {dr['domain']}<br>
                                <span style="color:#666;font-size:10px">
                                CTR {dr['ctr']:.4f}%</span>
                            </div>""", unsafe_allow_html=True)
                    with cc:
                        st.markdown("**Exclude (Tier C/D)**")
                        top_c = d_brand[d_brand['tier'].isin(['C','D'])].nlargest(5,'imp')
                        if top_c.empty:
                            st.caption("No domains to exclude")
                        for _, dr in top_c.iterrows():
                            st.markdown(f"""<div class="domain-c">
                                {dr['domain']}<br>
                                <span style="color:#666;font-size:10px">
                                {dr['imp']:,.0f} imps | CTR {dr['ctr']:.4f}%</span>
                            </div>""", unsafe_allow_html=True)

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
                        line=dict(color=col, width=2),
                        marker=dict(size=5),
                        fill='tozeroy',
                        fillcolor='rgba(80,80,80,0.1)',
                        hovertemplate='%{x}<br>CPA: EUR%{y:.1f}<extra></extra>'
                    ))
                    fig_s.update_layout(
                        paper_bgcolor='#0e1117', plot_bgcolor='#1a1d27',
                        height=160, margin=dict(l=30,r=10,t=10,b=30),
                        font=dict(color='#aaa', size=10, family='IBM Plex Mono'),
                        xaxis=dict(gridcolor='#2a2d3a'),
                        yaxis=dict(gridcolor='#2a2d3a', title='CPA EUR'),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_s, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════
# TAB 4
# ══════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("### Market Context — Gambling News by Geo")
    st.caption("Scraped from gamblinginsider.com, sbcnews.co.uk, calvinayre.com. Refreshes every hour.")

    active_geos = sorted(combined_view['geo'].unique())

    if not run_rag:
        st.info("Enable 'Fetch market news' in the sidebar to load geo-level news context.")
    else:
        with st.spinner("Fetching news..."):
            rag_data = fetch_rag_news(tuple(active_geos))

        pos = sum(1 for g in rag_data.values() if g['sentiment'] == 'positive')
        neg = sum(1 for g in rag_data.values() if g['sentiment'] == 'negative')
        neu = sum(1 for g in rag_data.values() if g['sentiment'] == 'neutral')

        c1, c2, c3 = st.columns(3)
        c1.metric("Positive market signal", pos, f"of {len(active_geos)} geos")
        c2.metric("Negative/restrictive",   neg, f"of {len(active_geos)} geos")
        c3.metric("No recent news",         neu, f"of {len(active_geos)} geos")
        st.markdown("---")

        for geo in active_geos:
            data      = rag_data.get(geo, {})
            sentiment = data.get('sentiment','neutral')
            articles  = data.get('articles', [])
            color     = {'positive':'#00c853','negative':'#f44336','neutral':'#666'}[sentiment]
            emoji     = {'positive':'🟢','negative':'🔴','neutral':'⚪'}[sentiment]

            with st.expander(
                f"{emoji} {geo} — {GEO_NAMES.get(geo,geo).title()} ({sentiment.upper()})"
            ):
                if not articles:
                    st.caption("No recent gambling-specific news found for this geo.")
                else:
                    for art in articles:
                        st.markdown(f"""
                        <div class="rag-card rag-{sentiment}">
                            <div style="font-weight:600;font-size:13px">{art['title'][:120]}</div>
                            <div style="font-size:11px;color:#666;margin-top:4px">
                                {art['source']} · {art['date'][:16]}
                            </div>
                            <div style="font-size:12px;color:#aaa;margin-top:6px">
                                {art['body'][:200]}...
                            </div>
                        </div>""", unsafe_allow_html=True)

                impact = combined_view[combined_view['geo'] == geo][
                    ['brand','total_ftd','avg_cpa','status']
                ].head(5)
                if not impact.empty:
                    st.markdown("**Your campaigns in this geo:**")
                    st.dataframe(impact, use_container_width=True, hide_index=True)
