"""
Synovia Flow -- BKD Pipeline Operations Dashboard v5
Clean readable theme, CSV upload, Create Declaration forms.
"""
import os, configparser, pyodbc, base64, io
import re
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ── Page config MUST be the first Streamlit command ───────────
st.set_page_config(
    page_title="BKD Pipeline Operations",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="collapsed",
)

LANDING_HTML_PATH = Path(__file__).with_name("index.html")
LANDING_LOGO_PATH = Path(__file__).with_name("birkdalelogo.png")


def get_view():
    view = st.query_params.get("view", "landing")
    if isinstance(view, list):
        view = view[0] if view else "landing"
    return str(view).strip().lower() or "landing"


def read_text_file(path):
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="cp1252")


def load_landing_document():
    if LANDING_HTML_PATH.exists():
        return read_text_file(LANDING_HTML_PATH)
    return None


def build_landing_logo_markup():
    if LANDING_LOGO_PATH.exists():
        logo_b64 = base64.b64encode(LANDING_LOGO_PATH.read_bytes()).decode("ascii")
        return f'<img class="logo" src="data:image/png;base64,{logo_b64}" alt="Birkdale" />'

    return """
<svg class="logo" viewBox="0 0 320 48" xmlns="http://www.w3.org/2000/svg" aria-label="Birkdale">
  <text x="0" y="40" font-size="44" font-family="'Segoe UI', system-ui, sans-serif"
        font-weight="800" letter-spacing="6" fill="#9B1553">BIRKDALE</text>
</svg>
"""


def prepare_landing_document(html_doc):
    return html_doc.replace("__BIRKDALE_LOGO_BLOCK__", build_landing_logo_markup())


def render_landing_page():
    fallback_html = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Birkdale QAS</title>
  <style>
    body { margin: 0; min-height: 100vh; background: #0d0d0f; display: flex; align-items: center; justify-content: center; padding: 2rem; color: #e8e8ec; font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; }
    .landing-card { width: 100%; max-width: 680px; border-radius: 24px; padding: 3.5rem 3rem; background: linear-gradient(180deg, rgba(22,22,26,0.96), rgba(14,14,18,0.98)); border: 1px solid rgba(155,21,83,0.22); text-align: center; }
    .landing-badge { display: inline-block; margin-bottom: 1.2rem; padding: 0.35rem 0.85rem; border-radius: 999px; border: 1px solid rgba(155,21,83,0.35); color: #d35f96; background: rgba(155,21,83,0.12); font-size: 0.74rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; }
    .logo { display: block; margin: 0 auto 2rem; width: min(100%, 520px); height: auto; }
    .landing-title { margin: 0 0 0.9rem 0; color: #f5f5f7; font-size: 2.3rem; line-height: 1.1; }
    .landing-title span { color: #d35f96; }
    .landing-copy { margin: 0 auto 2rem auto; max-width: 560px; color: #b6b6c3; font-size: 1rem; line-height: 1.75; }
    .landing-actions { display: flex; justify-content: center; }
    .landing-cta { display: inline-flex; align-items: center; justify-content: center; min-width: 220px; padding: 0.95rem 1.6rem; border-radius: 14px; background: #9b1553; color: #ffffff; text-decoration: none; font-weight: 700; }
  </style>
</head>
<body>
  <div class="landing-card">
    __BIRKDALE_LOGO_BLOCK__
    <div class="landing-badge">Quality Assurance System</div>
    <h1 class="landing-title">Birkdale <span>TSS Platform</span></h1>
    <p class="landing-copy">
      End-to-end pipeline for managing declarations through the Trader Support Service API,
      from staging and validation to monitoring and submission.
    </p>
    <div class="landing-actions">
      <a class="landing-cta" href="?view=dashboard" target="_top">Open Dashboard</a>
    </div>
  </div>
</body>
</html>
"""

    st.markdown(
        """
<style>
[data-testid="stHeader"], [data-testid="stToolbar"], [data-testid="stDecoration"] {
    display: none !important;
}
.block-container {
    max-width: 100% !important;
    padding: 0 !important;
}
div[data-testid="stButton"] {
    display: flex;
    justify-content: center;
    margin-top: -235px;
    margin-left: auto;
    margin-right: auto;
    position: relative;
    z-index: 9999;
}
div[data-testid="stButton"] > button {
    background: #9B1553 !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 12px !important;
    padding: 0.95rem 2.2rem !important;
    font-size: 1rem !important;
    font-weight: 700 !important;
    min-width: 240px !important;
    box-shadow: 0 4px 20px rgba(155,21,83,0.45) !important;
}

div[data-testid="stButton"] > button:hover {
    background: #7a1040 !important;
}
</style>
""",
        unsafe_allow_html=True,
    )

    html_doc = load_landing_document()
    rendered_html = (
        prepare_landing_document(html_doc)
        if html_doc
        else prepare_landing_document(fallback_html)
    )

    components.html(rendered_html, height=920, scrolling=False)

    # ── Streamlit button overlaid on the landing page ─────────
    left, center, right = st.columns([1, 1.2, 1])
    with center:
        clicked = st.button(
            "Open Dashboard", key="open_dashboard_btn", use_container_width=True
        )
    if clicked:
        st.query_params["view"] = "dashboard"
        st.rerun()


# ── Route: landing vs dashboard ───────────────────────────────
if get_view() != "dashboard":
    render_landing_page()
    st.stop()

# ══════════════════════════════════════════════════════════════
#  EVERYTHING BELOW IS DASHBOARD — UNCHANGED
# ══════════════════════════════════════════════════════════════

st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

html, body, .stApp, [data-testid="stAppViewContainer"] {
    font-family: 'Inter', -apple-system, sans-serif !important;
    background: #FAFBFC !important; color: #1F2937 !important;
}
[data-testid="stHeader"] { display: none; }
.block-container { padding-top: 0 !important; max-width: 1440px; }

/* Tabs */
div[data-testid="stTabs"] button[role="tab"] {
    font-family: 'Inter' !important; font-weight: 600 !important;
    font-size: 14px !important; color: #6B7280 !important;
}
div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] { color: #1D4ED8 !important; }

/* Expanders */
div[data-testid="stExpander"] { border: 1px solid #E5E7EB !important; border-radius: 8px !important; margin-bottom: 8px !important; }
div[data-testid="stExpander"] summary { font-size: 14px !important; font-weight: 600 !important; color: #1F2937 !important; }
div[data-testid="stExpander"] summary:hover { color: #1D4ED8 !important; }

/* Dataframes */
[data-testid="stDataFrame"] { border: 1px solid #E5E7EB; border-radius: 8px; }

/* Custom classes */
.kpi { background: #FFFFFF; border: 1px solid #E5E7EB; border-radius: 10px; padding: 20px 16px; text-align: center; }
.kpi .num { font-size: 32px; font-weight: 800; font-family: 'JetBrains Mono', monospace; line-height: 1; }
.kpi .lbl { font-size: 11px; font-weight: 700; color: #6B7280; text-transform: uppercase; letter-spacing: .8px; margin-top: 6px; }
.kpi .sub { font-size: 12px; color: #9CA3AF; margin-top: 3px; }
.kpi-bar { height: 3px; border-radius: 10px 10px 0 0; margin: -20px -16px 16px -16px; }
.tag { display: inline-block; padding: 3px 10px; border-radius: 6px; font-size: 11px; font-weight: 600; }
.tag-green { background: #ECFDF5; color: #065F46; }
.tag-amber { background: #FFFBEB; color: #92400E; }
.tag-blue  { background: #EFF6FF; color: #1E40AF; }
.tag-red   { background: #FEF2F2; color: #991B1B; }
.tag-purple{ background: #F5F3FF; color: #5B21B6; }
.tag-grey  { background: #F3F4F6; color: #6B7280; }
.sec { font-size: 13px; font-weight: 700; color: #374151; text-transform: uppercase; letter-spacing: .8px; border-left: 3px solid #3B82F6; padding-left: 12px; margin: 24px 0 12px 0; }
.card { background: #FFF; border: 1px solid #E5E7EB; border-radius: 8px; padding: 14px 18px; margin: 6px 0; font-size: 13px; color: #374151; }
.card-indent { margin-left: 28px; border-left: 2px solid #D1D5DB; }
.card-sfd { border-left: 3px solid #059669; margin-left: 28px; }
.card-goods { background: #F9FAFB; border: 1px solid #F3F4F6; margin-left: 56px; border-radius: 6px; padding: 10px 14px; font-size: 12px; }
.mono { font-family: 'JetBrains Mono', monospace; font-size: 12px; }
.muted { color: #9CA3AF; }
.hdr-bar { height: 3px; background: linear-gradient(90deg, #3B82F6, #60A5FA, transparent); margin: 0 -1rem 20px -1rem; }
.footer { text-align: center; font-size: 11px; color: #9CA3AF; padding: 20px; margin-top: 32px; border-top: 1px solid #E5E7EB; }
</style>""", unsafe_allow_html=True)

S = 'BKD'
DB_NAME = os.environ.get('DB_NAME', 'Fusion_TSS')
INI_CANDIDATES = [r'D:\confguration\fusion_TSS.ini', r'D:\Configuration\Fusion_TSS.ini', os.environ.get('INI_PATH','')]

@st.cache_resource(ttl=300)
def get_conn_str():
    for p in INI_CANDIDATES:
        if p and os.path.exists(p):
            cfg = configparser.ConfigParser(); cfg.read(p)
            d = cfg['database']
            return (f"DRIVER={d['driver']};SERVER={d['server']};DATABASE={DB_NAME};"
                    f"UID={d['user']};PWD={d['password']};"
                    f"Encrypt={d.get('encrypt','yes')};TrustServerCertificate={d.get('trust_server_certificate','no')};")
    if os.environ.get('DB_SERVER'):
        drv = os.environ.get('DB_DRIVER','{ODBC Driver 17 for SQL Server}')
        return (f"DRIVER={drv};SERVER={os.environ['DB_SERVER']};DATABASE={DB_NAME};"
                f"UID={os.environ['DB_USER']};PWD={os.environ['DB_PASSWORD']};Encrypt=yes;TrustServerCertificate=no;")
    return None

def get_conn():
    cs = get_conn_str()
    if not cs: return None
    return pyodbc.connect(cs, autocommit=False)

def q(sql, p=None):
    cs = get_conn_str()
    if not cs: return []
    try:
        conn = pyodbc.connect(cs, autocommit=False); cur = conn.cursor()
        cur.execute(sql, p or [])
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = cur.fetchall(); conn.close()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        st.error(f"DB: {e}"); return []

def q_val(sql, p=None):
    rows = q(sql, p); return list(rows[0].values())[0] if rows else 0

def q_df(sql, p=None):
    rows = q(sql, p); return pd.DataFrame(rows) if rows else pd.DataFrame()

def db_insert(sql, params):
    """Insert and commit. Returns True on success."""
    try:
        conn = get_conn()
        if not conn: return False
        cur = conn.cursor(); cur.execute(sql, params)
        conn.commit(); conn.close(); return True
    except Exception as e:
        st.error(f"Insert failed: {e}"); return False

def s(v, default=''):
    if v is None: return default
    t = str(v).strip(); return t if t else default

def tag(status):
    if not status: return '<span class="tag tag-grey">—</span>'
    sl = str(status).lower().strip()
    if sl in ('arrived','closed','created'): c = 'green'
    elif sl == 'draft': c = 'purple'
    elif sl in ('pending','submitted','processing'): c = 'amber'
    elif sl in ('trader input required','amendment required'): c = 'amber'
    elif sl in ('invalid','cancelled','error','fail'): c = 'red'
    elif 'authorised' in sl: c = 'green'
    else: c = 'blue'
    return f'<span class="tag tag-{c}">{status}</span>'

def kpi(value, label, sub="", color="#3B82F6"):
    return f'<div class="kpi"><div class="kpi-bar" style="background:{color}"></div><div class="num" style="color:{color}">{value}</div><div class="lbl">{label}</div><div class="sub">{sub}</div></div>'


# ── Header ────────────────────────────────────────────────────
logo_path = Path(r"D:\Applications\Fusion_Release_4\Fusion_TSS\FLow_Birkdale_QAS\Render\assets")
logo_b64 = None
for lp in [logo_path, Path(r"D:\Graphics")]:
    if lp.exists():
        for f in lp.rglob("*"):
            if f.suffix.lower() in ('.png',) and any(k in f.name.lower() for k in ('synovia','fusion','logo')):
                logo_b64 = base64.b64encode(f.read_bytes()).decode(); break
    if logo_b64: break

logo_img = f'<img src="data:image/png;base64,{logo_b64}" height="32">' if logo_b64 else '<div style="width:34px;height:34px;border-radius:8px;background:#3B82F6;display:flex;align-items:center;justify-content:center;font-size:16px;font-weight:800;color:#fff">S</div>'

st.markdown(f"""
<div style="background:linear-gradient(135deg,#0F172A 0%,#1E3A5F 60%,#1E5F99 100%);padding:16px 28px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin:-1rem -1rem 0 -1rem">
    <div style="display:flex;align-items:center;gap:14px">
        {logo_img}
        <div>
            <div style="font-size:17px;font-weight:800;color:#fff;letter-spacing:.2px">BKD Pipeline Operations</div>
            <div style="font-size:10px;color:#93C5FD;letter-spacing:1px;text-transform:uppercase">Birkdale Sales · Synovia Flow · Fusion_TSS</div>
        </div>
    </div>
    <div style="display:flex;align-items:center;gap:16px;font-size:12px;color:#94A3B8">
        Schema: <strong style="color:#93C5FD">{S}</strong> &nbsp;
        Env: <strong style="color:#86EFAC">TST</strong> &nbsp;
        <span class="mono">{datetime.utcnow():%H:%M} UTC · {datetime.utcnow():%Y-%m-%d}</span>
    </div>
</div>
<div class="hdr-bar"></div>
""", unsafe_allow_html=True)


# ── Tabs ──────────────────────────────────────────────────────
tab_chain, tab_submit, tab_create, tab_csv, tab_updates = st.tabs([
    "🔗 Declaration Chain", "🚀 Submission Queue", "➕ Create Declaration", "📄 CSV Upload", "📡 TSS Updates"
])


# ══════════════════════════════════════════════════════════════
#  TAB: DECLARATION CHAIN
# ══════════════════════════════════════════════════════════════
with tab_chain:
    c1,c2,c3,c4,c5 = st.columns(5)
    with c1: st.markdown(kpi(q_val(f"SELECT COUNT(*) FROM {S}.EnsHeaders"), "ENS Headers", "", "#3B82F6"), unsafe_allow_html=True)
    with c2: st.markdown(kpi(q_val(f"SELECT COUNT(*) FROM {S}.Consignments"), "Consignments", "", "#059669"), unsafe_allow_html=True)
    with c3: st.markdown(kpi(q_val(f"SELECT COUNT(*) FROM {S}.Sfds"), "SFDs", "", "#D97706"), unsafe_allow_html=True)
    with c4: st.markdown(kpi(q_val(f"SELECT COUNT(*) FROM {S}.SfdGoodsItems"), "Goods Items", "", "#7C3AED"), unsafe_allow_html=True)
    with c5: st.markdown(kpi(q_val(f"SELECT COUNT(*) FROM {S}.SupplementaryDeclarations"), "Sup Decs", "", "#0891B2"), unsafe_allow_html=True)

    st.markdown('<div class="sec">Declaration Chain — Click to Expand</div>', unsafe_allow_html=True)

    for ens in q(f"SELECT declaration_number, tss_status, movement_type, arrival_port, arrival_date_time, carrier_name, route, identity_no_transport, place_of_loading, place_of_unloading FROM {S}.EnsHeaders ORDER BY id"):
        ref = ens['declaration_number']
        with st.expander(f"🚢  {ref}  —  {s(ens['tss_status'])}  |  {s(ens.get('arrival_port'))}  |  {s(ens.get('carrier_name'))}", expanded=False):
            e1,e2,e3,e4 = st.columns(4)
            with e1: st.markdown(f"**Movement:** `{s(ens.get('movement_type'))}`")
            with e2: st.markdown(f"**Route:** `{s(ens.get('route'))}`")
            with e3: st.markdown(f"**Arrival:** `{s(ens.get('arrival_date_time'))}`")
            with e4: st.markdown(f"**Vessel:** `{s(ens.get('identity_no_transport'))}`")
            st.markdown(f"**Loading:** {s(ens.get('place_of_loading'))} → **Unloading:** {s(ens.get('place_of_unloading'))}")
            st.divider()

            for cons in q(f"SELECT consignment_number, tss_status, goods_description, controlled_goods, total_packages, gross_mass_kg, movement_reference_number, transport_document_number FROM {S}.Consignments WHERE declaration_number=? ORDER BY id", [ref]):
                cref = s(cons['consignment_number'])
                mrn = s(cons.get('movement_reference_number'))
                mrn_str = f" · MRN: `{mrn}`" if mrn else ""
                st.markdown(f"📦 **`{cref}`** &nbsp; {tag(s(cons.get('tss_status')))} &nbsp; ctrl=`{s(cons.get('controlled_goods'))}` &nbsp; {s(cons.get('total_packages'))} pkg · {s(cons.get('gross_mass_kg'))} kg{mrn_str}", unsafe_allow_html=True)
                st.caption(f"{s(cons.get('goods_description'))} · Doc: {s(cons.get('transport_document_number'))}")

                for sfd in q(f"SELECT sfd_number, tss_status, eori_for_eidr, goods_domestic_status, total_packages, gross_mass_kg FROM {S}.Sfds WHERE ens_consignment_reference=? ORDER BY id", [cref]):
                    sref = s(sfd['sfd_number'])
                    st.markdown(f"&emsp; 📄 **`{sref}`** &nbsp; {tag(s(sfd.get('tss_status')))} &nbsp; EIDR=`{s(sfd.get('eori_for_eidr'))}` · dom=`{s(sfd.get('goods_domestic_status'))}` · {s(sfd.get('total_packages'))} pkg · {s(sfd.get('gross_mass_kg'))} kg", unsafe_allow_html=True)

                    for gi in q(f"SELECT commodity_code, goods_description, number_of_packages, type_of_packages, gross_mass_kg, country_of_origin, item_invoice_amount, item_invoice_currency, procedure_code FROM {S}.SfdGoodsItems WHERE sfd_number=? ORDER BY id", [sref]):
                        inv = s(gi.get('item_invoice_amount'))
                        ccy = s(gi.get('item_invoice_currency'))
                        inv_str = f"£{inv}" if ccy=='GBP' and inv else f"{ccy} {inv}" if inv else ""
                        st.markdown(f"&emsp;&emsp; 🏷️ `{s(gi.get('commodity_code'))}` {s(gi.get('goods_description',''))[:45]} · {s(gi.get('number_of_packages'))}×{s(gi.get('type_of_packages'))} · {s(gi.get('gross_mass_kg'))} kg · {s(gi.get('country_of_origin'))} {('· '+inv_str) if inv_str else ''} · proc `{s(gi.get('procedure_code'))}`")

                if not q(f"SELECT 1 FROM {S}.Sfds WHERE ens_consignment_reference=?", [cref]):
                    st.caption("&emsp; _No SFD generated yet_")
                st.markdown("---")


# ══════════════════════════════════════════════════════════════
#  TAB: SUBMISSION QUEUE
# ══════════════════════════════════════════════════════════════
with tab_submit:
    ens_p = q_val(f"SELECT COUNT(*) FROM {S}.StagingEnsHeaders WHERE status='PENDING'")
    ens_i = q_val(f"SELECT COUNT(*) FROM {S}.StagingEnsHeaders WHERE status='INVALID'")
    cons_p = q_val(f"SELECT COUNT(*) FROM {S}.StagingConsignments WHERE status='PENDING'")
    goods_p = q_val(f"SELECT COUNT(*) FROM {S}.StagingGoodsItems WHERE status='PENDING'")

    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(kpi(ens_p+cons_p+goods_p, "Total Pending", "Awaiting submission", "#D97706"), unsafe_allow_html=True)
    with c2: st.markdown(kpi(ens_i, "Invalid", "Need correction", "#DC2626"), unsafe_allow_html=True)
    with c3: st.markdown(kpi(q_val(f"SELECT COUNT(*) FROM {S}.StagingEnsHeaders"), "ENS Staged", f"{ens_p} pending", "#3B82F6"), unsafe_allow_html=True)
    with c4: st.markdown(kpi(goods_p, "Goods Staged", "Lines to submit", "#059669"), unsafe_allow_html=True)

    sub = st.radio("", ["📊 Pipeline","🚢 ENS","📦 Consignments","🏷️ Goods","🛬 IMMIs","📋 SFDs","📝 Supp Decs"], horizontal=True, label_visibility="collapsed", key="sq")

    sec_map = {
        "📊 Pipeline": ("PIPELINE STATUS", f"SELECT 'ENS_HEADER' AS Type, status AS Status, COUNT(*) AS Count FROM {S}.StagingEnsHeaders GROUP BY status UNION ALL SELECT 'CONSIGNMENT', status, COUNT(*) FROM {S}.StagingConsignments GROUP BY status UNION ALL SELECT 'GOODS_ITEM', status, COUNT(*) FROM {S}.StagingGoodsItems GROUP BY status UNION ALL SELECT 'IMMI', status, COUNT(*) FROM {S}.StagingImmis GROUP BY status UNION ALL SELECT 'SFD', status, COUNT(*) FROM {S}.StagingSfds GROUP BY status UNION ALL SELECT 'SUPP_DEC', status, COUNT(*) FROM {S}.StagingSupplementaryDeclarations GROUP BY status"),
        "🚢 ENS": ("STAGING ENS HEADERS", f"SELECT staging_id AS [#], label, status, identity_no_of_transport AS [Vessel], arrival_date_time AS Arrival, arrival_port AS Port, carrier_name AS Carrier, ens_reference AS [ENS Ref], api_status AS API, http_status AS HTTP FROM {S}.StagingEnsHeaders ORDER BY staging_id"),
        "📦 Consignments": ("STAGING CONSIGNMENTS", f"SELECT staging_id AS [#], staging_ens_id AS [ENS#], label, goods_description AS Goods, controlled_goods AS Ctrl, status, dec_reference AS [DEC Ref], api_status AS API FROM {S}.StagingConsignments ORDER BY staging_id"),
        "🏷️ Goods": ("STAGING GOODS", f"SELECT staging_id AS [#], staging_cons_id AS [Cons#], label, goods_description AS Description, commodity_code AS [HS Code], number_of_packages AS Qty, gross_mass_kg AS [Gross KG], status, goods_id AS [Goods ID], api_status AS API FROM {S}.StagingGoodsItems ORDER BY staging_id"),
        "🛬 IMMIs": ("STAGING IMMIS", f"SELECT staging_id AS [#], label, status, arrival_date_time AS Arrival, arrival_port AS Port, immi_reference AS [IMMI Ref], api_status AS API FROM {S}.StagingImmis ORDER BY staging_id"),
        "📋 SFDs": ("STAGING SFDS", f"SELECT staging_id AS [#], label, status, commodity_code AS [HS], gross_mass_kg AS [Gross KG], sfd_reference AS [SFD Ref], api_status AS API FROM {S}.StagingSfds ORDER BY staging_id"),
        "📝 Supp Decs": ("STAGING SUPP DECS", f"SELECT staging_id AS [#], label, status, declaration_type AS Type, goods_description AS Goods, supp_dec_reference AS [SD Ref], api_status AS API FROM {S}.StagingSupplementaryDeclarations ORDER BY staging_id"),
    }
    title, sql = sec_map[sub]
    st.markdown(f'<div class="sec">{title}</div>', unsafe_allow_html=True)
    df = q_df(sql)
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No records")


# ══════════════════════════════════════════════════════════════
#  TAB: CREATE DECLARATION
# ══════════════════════════════════════════════════════════════
with tab_create:
    st.markdown('<div class="sec">Create New Declaration</div>', unsafe_allow_html=True)
    st.caption("Fill in the form and click Submit. The record will be saved as PENDING in the staging table for validation before API submission.")

    dec_type = st.selectbox("Declaration Type", [
        "ENS Header", "Consignment", "Goods Item", "IMMI"
    ], key="create_type")

    if dec_type == "ENS Header":
        st.markdown("##### ENS Header — Movement Details")
        with st.form("form_ens", clear_on_submit=True):
            lbl = st.text_input("Label", placeholder="e.g. ENS-BKD-004 Ferry DUB-BEL")
            c1,c2 = st.columns(2)
            with c1:
                mvt = st.selectbox("Movement Type", ["1","1a","3","3a"], help="1=Maritime, 1a=RoRo Unaccompanied, 3=RoRo, 3a=RoRo Accompanied")
                identity = st.text_input("Identity of Transport", placeholder="e.g. IMO9007855")
                nationality = st.text_input("Nationality", value="GB", max_chars=2)
                arrival_dt = st.text_input("Arrival Date/Time (dd/mm/yyyy HH:MM:SS)", placeholder="10/04/2026 06:00:00")
            with c2:
                port = st.text_input("Arrival Port", value="GBAUBELBELBEL")
                loading = st.text_input("Place of Loading", placeholder="e.g. Dublin")
                unloading = st.text_input("Place of Unloading", placeholder="e.g. Belfast")
                seal = st.text_input("Seal Number", placeholder="e.g. SEAL-001 or NO SEAL")
            c3,c4 = st.columns(2)
            with c3:
                charges = st.selectbox("Transport Charges", ["A","Y","N"])
                carrier_eori = st.text_input("Carrier EORI", value="XI000012340005")
            with c4:
                carrier_name = st.text_input("Carrier Name", placeholder="e.g. Irish Continental Group")

            submitted = st.form_submit_button("📤 Submit to Staging (PENDING)", type="primary", use_container_width=True)
            if submitted:
                if not lbl or not identity or not arrival_dt:
                    st.error("Label, Identity of Transport, and Arrival Date/Time are required.")
                else:
                    ok = db_insert(f"""INSERT INTO {S}.StagingEnsHeaders
                        (label, movement_type, identity_no_of_transport, nationality_of_transport,
                         arrival_date_time, arrival_port, place_of_loading, place_of_unloading,
                         seal_number, transport_charges, carrier_eori, carrier_name,
                         status, retry_count, max_retries, created_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',0,3,SYSUTCDATETIME())""",
                        [lbl, mvt, identity, nationality, arrival_dt, port, loading, unloading,
                         seal, charges, carrier_eori, carrier_name])
                    if ok:
                        st.success(f"ENS Header '{lbl}' saved as PENDING. It will be validated and moved to STAGED.")

    elif dec_type == "Consignment":
        st.markdown("##### Consignment — Linked to an ENS Header")
        ens_opts = q(f"SELECT staging_id, label, ens_reference FROM {S}.StagingEnsHeaders WHERE status IN ('PENDING','CREATED') ORDER BY staging_id")
        ens_labels = {f"{r['staging_id']}: {r['label']} ({s(r.get('ens_reference'),'not yet created')})": r['staging_id'] for r in ens_opts}

        with st.form("form_cons", clear_on_submit=True):
            ens_pick = st.selectbox("Link to ENS Header", list(ens_labels.keys()) if ens_labels else ["No ENS headers available"])
            lbl = st.text_input("Label", placeholder="e.g. CONS-005 Golf Equipment")
            c1,c2 = st.columns(2)
            with c1:
                desc = st.text_input("Goods Description", placeholder="e.g. Complete golf club sets")
                tdoc = st.text_input("Transport Document Number", placeholder="e.g. BOL-IE-2026-4410")
                ctrl = st.selectbox("Controlled Goods", ["no","yes"])
                gds = st.selectbox("Goods Domestic Status", ["","D","I"])
            with c2:
                dest = st.text_input("Destination Country", value="GB", max_chars=2)
                imp_eori = st.text_input("Importer EORI", value="XI000012340005")
                imp_name = st.text_input("Importer Name", placeholder="e.g. Birkdale Sales Ltd")
                consignor = st.text_input("Consignor Name", placeholder="e.g. McGuigan Sports Ireland")
            exp_eori = st.text_input("Exporter EORI", placeholder="e.g. XI000012340005")

            submitted = st.form_submit_button("📤 Submit to Staging (PENDING)", type="primary", use_container_width=True)
            if submitted and ens_labels:
                ens_id = ens_labels.get(ens_pick, 0)
                ok = db_insert(f"""INSERT INTO {S}.StagingConsignments
                    (staging_ens_id, label, goods_description, transport_document_number,
                     controlled_goods, goods_domestic_status, destination_country,
                     importer_eori, importer_name, consignor_name, exporter_eori,
                     buyer_same_as_importer, seller_same_as_exporter,
                     status, retry_count, max_retries, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,'yes','yes','PENDING',0,3,SYSUTCDATETIME())""",
                    [ens_id, lbl, desc, tdoc, ctrl, gds, dest, imp_eori, imp_name, consignor, exp_eori])
                if ok:
                    st.success(f"Consignment '{lbl}' saved as PENDING.")

    elif dec_type == "Goods Item":
        st.markdown("##### Goods Item — Linked to a Consignment")
        cons_opts = q(f"SELECT staging_id, label, dec_reference FROM {S}.StagingConsignments WHERE status IN ('PENDING','CREATED') ORDER BY staging_id")
        cons_labels = {f"{r['staging_id']}: {r['label']} ({s(r.get('dec_reference'),'not yet created')})": r['staging_id'] for r in cons_opts}

        with st.form("form_goods", clear_on_submit=True):
            cons_pick = st.selectbox("Link to Consignment", list(cons_labels.keys()) if cons_labels else ["No consignments available"])
            lbl = st.text_input("Label", placeholder="e.g. GI-007 Golf Gloves")
            c1,c2 = st.columns(2)
            with c1:
                gdesc = st.text_input("Goods Description", placeholder="e.g. Leather golf gloves, various sizes")
                commodity = st.text_input("Commodity Code (HS)", placeholder="e.g. 42032910")
                origin = st.text_input("Country of Origin", value="IE", max_chars=2)
                pkg_type = st.text_input("Package Type", value="BG", max_chars=10)
                pkg_num = st.number_input("Number of Packages", min_value=1, value=10)
            with c2:
                gross = st.number_input("Gross Mass KG", min_value=0.0, value=50.0, step=0.1, format="%.3f")
                net = st.number_input("Net Mass KG", min_value=0.0, value=45.0, step=0.1, format="%.3f")
                invoice = st.number_input("Invoice Amount", min_value=0.0, value=1000.0, step=0.01, format="%.2f")
                currency = st.text_input("Currency", value="GBP", max_chars=3)
                procedure = st.text_input("Procedure Code", value="4000", max_chars=10)
            ctrl_g = st.selectbox("Controlled Goods", ["no","yes"], key="ctrl_goods")

            submitted = st.form_submit_button("📤 Submit to Staging (PENDING)", type="primary", use_container_width=True)
            if submitted and cons_labels:
                cons_id = cons_labels.get(cons_pick, 0)
                ok = db_insert(f"""INSERT INTO {S}.StagingGoodsItems
                    (staging_cons_id, label, goods_description, commodity_code,
                     type_of_packages, number_of_packages, gross_mass_kg, net_mass_kg,
                     country_of_origin, item_invoice_amount, item_invoice_currency,
                     procedure_code, controlled_goods,
                     status, retry_count, max_retries, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',0,3,SYSUTCDATETIME())""",
                    [cons_id, lbl, gdesc, commodity, pkg_type, pkg_num, gross, net,
                     origin, invoice, currency, procedure, ctrl_g])
                if ok:
                    st.success(f"Goods Item '{lbl}' saved as PENDING.")

    elif dec_type == "IMMI":
        st.markdown("##### Internal Market Movement (IMMI)")
        ens_opts = q(f"SELECT staging_id, label FROM {S}.StagingEnsHeaders WHERE status IN ('PENDING','CREATED') ORDER BY staging_id")
        ens_labels = {f"{r['staging_id']}: {r['label']}": r['staging_id'] for r in ens_opts}

        with st.form("form_immi", clear_on_submit=True):
            ens_pick = st.selectbox("Link to ENS", list(ens_labels.keys()) if ens_labels else ["No ENS available"])
            lbl = st.text_input("Label", placeholder="e.g. IMMI-004 Belfast Arrival")
            c1,c2 = st.columns(2)
            with c1:
                identity = st.text_input("Identity of Transport", placeholder="e.g. IMO9007855")
                arrival_dt = st.text_input("Arrival Date/Time", placeholder="10/04/2026 06:30:00")
                port = st.text_input("Arrival Port", value="GBAUBELBELBEL")
            with c2:
                carrier_eori = st.text_input("Carrier EORI", value="XI000012340005")
                gmr = st.text_input("GMR ID", placeholder="e.g. GMR-BEL-20260410-001")

            submitted = st.form_submit_button("📤 Submit to Staging (PENDING)", type="primary", use_container_width=True)
            if submitted and ens_labels:
                ens_id = ens_labels.get(ens_pick, 0)
                ok = db_insert(f"""INSERT INTO {S}.StagingImmis
                    (staging_ens_id, label, identity_no_of_transport, arrival_date_time,
                     arrival_port, carrier_eori, gmr_id,
                     status, retry_count, max_retries, created_at)
                    VALUES (?,?,?,?,?,?,?,'PENDING',0,3,SYSUTCDATETIME())""",
                    [ens_id, lbl, identity, arrival_dt, port, carrier_eori, gmr])
                if ok:
                    st.success(f"IMMI '{lbl}' saved as PENDING.")


# ══════════════════════════════════════════════════════════════
#  TAB: CSV UPLOAD
# ══════════════════════════════════════════════════════════════
with tab_csv:
    st.markdown('<div class="sec">CSV / Excel Upload</div>', unsafe_allow_html=True)
    st.caption("Upload a CSV or Excel file to bulk-load declarations into staging tables. Each row becomes a PENDING record.")

    target = st.selectbox("Target Staging Table", [
        "StagingEnsHeaders", "StagingConsignments", "StagingGoodsItems", "StagingImmis"
    ], key="csv_target")

    # Show expected columns
    cols_map = {
        "StagingEnsHeaders": "label, movement_type, identity_no_of_transport, nationality_of_transport, arrival_date_time, arrival_port, place_of_loading, place_of_unloading, seal_number, transport_charges, carrier_eori, carrier_name",
        "StagingConsignments": "staging_ens_id, label, goods_description, transport_document_number, controlled_goods, goods_domestic_status, destination_country, importer_eori, importer_name, consignor_name, exporter_eori",
        "StagingGoodsItems": "staging_cons_id, label, goods_description, commodity_code, type_of_packages, number_of_packages, gross_mass_kg, net_mass_kg, country_of_origin, item_invoice_amount, item_invoice_currency, procedure_code, controlled_goods",
        "StagingImmis": "staging_ens_id, label, identity_no_of_transport, arrival_date_time, arrival_port, carrier_eori, gmr_id",
    }
    st.info(f"**Expected columns:** {cols_map[target]}")

    uploaded = st.file_uploader("Choose CSV or Excel file", type=["csv","xlsx","xls"], key="csv_upload")

    if uploaded:
        try:
            if uploaded.name.endswith('.csv'):
                df = pd.read_csv(uploaded)
            else:
                df = pd.read_excel(uploaded)

            st.markdown(f"**Preview:** {len(df)} rows × {len(df.columns)} columns")
            st.dataframe(df.head(20), use_container_width=True, hide_index=True)

            expected = [c.strip() for c in cols_map[target].split(',')]
            found = [c for c in expected if c in df.columns]
            missing = [c for c in expected if c not in df.columns]

            if missing:
                st.warning(f"Missing columns (will be NULL): {', '.join(missing)}")
            st.success(f"Matched columns: {', '.join(found)}")

            if st.button(f"📤 Insert {len(df)} rows into {S}.{target} as PENDING", type="primary", key="csv_go"):
                inserted = 0
                conn = get_conn()
                if conn:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cols_present = [c for c in found if pd.notna(row.get(c))]
                        vals = [str(row[c]) if pd.notna(row.get(c)) else None for c in cols_present]
                        col_str = ','.join(cols_present) + ',status,retry_count,max_retries,created_at'
                        ph = ','.join(['?']*len(cols_present)) + ",'PENDING',0,3,SYSUTCDATETIME()"
                        try:
                            cur.execute(f"INSERT INTO {S}.[{target}] ({col_str}) VALUES ({ph})", vals)
                            inserted += 1
                        except Exception as e:
                            st.error(f"Row {inserted+1}: {e}")
                            break
                    conn.commit(); conn.close()
                    st.success(f"Inserted {inserted} rows into {S}.{target} as PENDING.")
        except Exception as e:
            st.error(f"Error reading file: {e}")


# ══════════════════════════════════════════════════════════════
#  TAB: TSS UPDATES
# ══════════════════════════════════════════════════════════════
with tab_updates:
    tables_info = [
        ("ENS Headers", f"SELECT COUNT(*) FROM {S}.EnsHeaders", f"SELECT MAX(downloaded_at) FROM {S}.EnsHeaders"),
        ("Consignments", f"SELECT COUNT(*) FROM {S}.Consignments", f"SELECT MAX(downloaded_at) FROM {S}.Consignments"),
        ("SFDs", f"SELECT COUNT(*) FROM {S}.Sfds", f"SELECT MAX(downloaded_at) FROM {S}.Sfds"),
        ("SFD Goods", f"SELECT COUNT(*) FROM {S}.SfdGoodsItems", f"SELECT MAX(downloaded_at) FROM {S}.SfdGoodsItems"),
        ("Sup Decs", f"SELECT COUNT(*) FROM {S}.SupplementaryDeclarations", f"SELECT MAX(downloaded_at) FROM {S}.SupplementaryDeclarations"),
        ("SD Goods", f"SELECT COUNT(*) FROM {S}.SupDecGoodsItems", f"SELECT MAX(downloaded_at) FROM {S}.SupDecGoodsItems"),
    ]
    total_synced = sum(q_val(t[1]) for t in tables_info)
    last_t = q_val(f"SELECT FORMAT(MAX(downloaded_at),'HH:mm') FROM {S}.EnsHeaders")
    last_d = q_val(f"SELECT FORMAT(MAX(downloaded_at),'yyyy-MM-dd') FROM {S}.EnsHeaders")

    c1,c2,c3,c4 = st.columns(4)
    with c1: st.markdown(kpi(total_synced, "Total Synced", "Records from TSS", "#3B82F6"), unsafe_allow_html=True)
    with c2: st.markdown(kpi(q_val(f"SELECT COUNT(*) FROM {S}.EnsHeaders"), "ENS Headers", f"{q_val(f'SELECT COUNT(*) FROM {S}.EnsHeaders WHERE tss_status=''Arrived''')} arrived", "#059669"), unsafe_allow_html=True)
    with c3: st.markdown(kpi(q_val(f"SELECT COUNT(*) FROM {S}.Consignments"), "Consignments", "", "#D97706"), unsafe_allow_html=True)
    with c4: st.markdown(kpi(s(last_t,"—"), "Last Sync", s(last_d,"Never"), "#6B7280"), unsafe_allow_html=True)

    sub2 = st.radio("", ["🔄 Sync","🚢 ENS","📦 Consignments","📋 SFDs"], horizontal=True, label_visibility="collapsed", key="tu")

    queries = {
        "🔄 Sync": None,
        "🚢 ENS": f"SELECT declaration_number AS [Declaration #], tss_status AS Status, movement_type AS Mvt, identity_no_transport AS Vessel, arrival_date_time AS Arrival, arrival_port AS Port, carrier_name AS Carrier, downloaded_at AS Downloaded FROM {S}.EnsHeaders ORDER BY id",
        "📦 Consignments": f"SELECT consignment_number AS [Consignment #], declaration_number AS [ENS #], tss_status AS Status, goods_description AS Goods, total_packages AS Pkgs, gross_mass_kg AS [KG] FROM {S}.Consignments ORDER BY id",
        "📋 SFDs": f"SELECT sfd_number AS [SFD #], ens_consignment_reference AS [ENS Cons Ref], tss_status AS Status, goods_domestic_status AS [Dom], total_packages AS Pkgs, gross_mass_kg AS [KG] FROM {S}.Sfds ORDER BY id",
    }
    if sub2 == "🔄 Sync":
        st.markdown('<div class="sec">Sync Summary</div>', unsafe_allow_html=True)
        sync_rows = []
        for name, cnt_sql, dt_sql in tables_info:
            cnt = q_val(cnt_sql); dt = q_val(dt_sql)
            sync_rows.append({"Table": name, "Rows": cnt, "Last Sync": str(dt)[:19] if dt else "—"})
        st.dataframe(pd.DataFrame(sync_rows), use_container_width=True, hide_index=True)
    else:
        st.markdown(f'<div class="sec">{sub2[2:]}</div>', unsafe_allow_html=True)
        df = q_df(queries[sub2])
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No records")


# Footer
api_n = q_val(f"SELECT COUNT(*) FROM {S}.ApiLog")
tbl_n = q_val(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{S}' AND TABLE_TYPE='BASE TABLE'")
st.markdown(f'<div class="footer"><strong>Synovia Fusion – TSS Module</strong> · Synovia Digital Ltd · {DB_NAME} · {tbl_n} tables · {api_n} API log entries</div>', unsafe_allow_html=True)