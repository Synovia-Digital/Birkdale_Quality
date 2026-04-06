
"""
================================================================================
  Synovia Flow -- TSS Birkdale Test Environment Probe
  Licensed Component: Synovia Digital Ltd
================================================================================

  Product:      Synovia Flow (Customs Declaration Management)
  Module:       TSS Birkdale Test Environment Probe
  Version:      1.0.0
  Database:     Fusion_TSS
  Schema:       BRK (Birkdale)
  API:          TSS Declaration API v2.9.4 (TEST)

  Description:
  ------------
  Probes the TSS TEST environment to discover what declaration data
  already exists for the Birkdale credentials.  This is a READ-ONLY
  discovery script — it does not create, update, or delete anything.

  The probe sweeps every filterable resource in the Declaration API:

      Phase 1:  ENS Headers       (filter by status)
      Phase 2:  SFDs              (filter by status)
      Phase 3:  Supplementary Declarations  (filter by status)
      Phase 4:  Full Frontier Declarations  (filter by status)
      Phase 5:  Internal Market Movements   (filter by status)
      Phase 6:  GVMS GMRs         (filter by status)
      Phase 7:  Permission Grant   (check EORI access)
      Phase 8:  Cross-reference    (link discovered refs across types)
      Phase 9:  Summary dashboard + JSON/HTML output

  API Navigation:
      GET /<resource>?filter=status=<STATUS>      → discover refs
      GET /<resource>?reference=<REF>&fields=...  → read details

  Outputs (to D:\\TSS_Madrid\\Birkdale):
  --------------------------------------
  - brk_probe_<timestamp>.json     Full discovery results
  - brk_probe_<timestamp>.html     Interactive viewer

  Prerequisites:
  --------------
  - D:\\confguration\\fusion_TSS.ini
  - CFG.Credentials + CFG.Environments for BRK/TST in Fusion_TSS
  - pip install rich requests pyodbc

  Usage:
      python TSS_BRK_Probe.py

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

__version__ = '1.0.0'
__product__ = 'Synovia Flow'
__module__  = 'TSS Birkdale Test Environment Probe'

import base64
import configparser
import json
import os
import sys
import time
from datetime import datetime, timezone

import pyodbc
import requests
from rich.console import Console
from rich.panel   import Panel
from rich.table   import Table
from rich.rule    import Rule
from rich         import box

con = Console(highlight=False, width=140)


# ==============================================================
#  CLIENT CONFIG
# ==============================================================
CLIENT_CODE = 'BRK'
CLIENT_NAME = 'Birkdale'
ENV_CODE    = 'TST'
DB_NAME     = 'Fusion_TSS'
INI_PATH    = r'D:\confguration\fusion_TSS.ini'

RATE_LIMIT  = 0.20          # polite pause between API calls (seconds)
API_TIMEOUT = 30

TIMESTAMP   = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_DIR  = r'D:\TSS_Madrid\Birkdale'
JSON_FILE   = os.path.join(OUTPUT_DIR, f'brk_probe_{TIMESTAMP}.json')
HTML_FILE   = os.path.join(OUTPUT_DIR, f'brk_probe_{TIMESTAMP}.html')

# ──────────────────────────────────────────────────────────────
#  STATUSES TO PROBE PER RESOURCE
#  (confirmed valid against TSS API v2.9.4)
# ──────────────────────────────────────────────────────────────

# ENS Header statuses — from [TSS].[CV_ens_status]
ENS_STATUSES = [
    'Draft',
    'Submitted',
    'Processing',
    'Trader Input Required',
    'Authorised for Movement',
    'Authorised for movement',     # API is case-sensitive
    'Arrived',
    'Cancelled',
]

# SFD statuses — all 9 return HTTP 200 (confirmed by CWF probe)
SFD_STATUSES = [
    'Draft',
    'Submitted',
    'Processing',
    'Trader Input Required',
    'Authorised for Movement',
    'Authorised for movement',
    'Amendment Required',
    'Arrived',
    'Cancelled',
]

# Supplementary Declaration statuses — from [TSS].[CV_sd_status]
# Removed: Accepted, Rejected, Cleared, Amendment Required (HTTP 400)
SUP_DEC_STATUSES = [
    'closed',
    'trader input required',
    'draft',
    'submitted',
    'processing',
    'final processing',
    'pending payment',
    'payment received',
    'on hold',
    'in periodic',
    'reconciliation',
    'fiscal hold',
    'tax calculation verification',
    'in conversion to immi',
    'cancelled',
]

# FFD statuses
FFD_STATUSES = [
    'Draft',
    'Submitted',
    'Processing',
    'Trader Input Required',
    'Arrived',
    'Cancelled',
]

# IMMI statuses
IMMI_STATUSES = [
    'Draft',
    'Submitted',
    'Processing',
    'Trader Input Required',
    'Cancelled',
]

# GVMS GMR statuses
GVMS_STATUSES = [
    'Draft',
    'Submitted',
    'Cancelled',
]


# ──────────────────────────────────────────────────────────────
#  FIELD SETS FOR READS
# ──────────────────────────────────────────────────────────────
ENS_READ_FIELDS = (
    'status,movement_type,identity_no_of_transport,'
    'nationality_of_transport,arrival_date_time,arrival_port,'
    'place_of_loading,place_of_unloading,seal_number,route,'
    'carrier_eori,carrier_name,haulier_eori,error_message'
)

SFD_READ_FIELDS = (
    'status,goods_description,transport_document_number,'
    'importer_eori,movement_reference_number,'
    'ens_consignment_reference,trader_reference,'
    'error_message'
)

SUP_DEC_READ_FIELDS = (
    'status,movement_reference_number,trader_reference,'
    'importer_eori,arrival_date_time,port_of_arrival,'
    'transport_document_number,submission_due_date,'
    'total_packages,clear_date_time,goods_description,'
    'error_message'
)

FFD_READ_FIELDS = (
    'status,movement_type,declaration_category,'
    'arrival_date_time,arrival_port,importer_eori,'
    'goods_description,location_of_goods,error_message'
)

IMMI_READ_FIELDS = (
    'status,declaration_category,arrival_date_time,'
    'importer_eori,representation_type,mode_of_transport,'
    'transport_document_reference,trader_reference,error_message'
)


# ==============================================================
#  DATABASE
# ==============================================================
def make_conn():
    cfg = configparser.ConfigParser()
    cfg.read(INI_PATH)
    d = cfg['database']
    return pyodbc.connect(
        f"DRIVER={d['driver']};SERVER={d['server']};DATABASE={DB_NAME};"
        f"UID={d['user']};PWD={d['password']};"
        f"Encrypt={d.get('encrypt','yes')};"
        f"TrustServerCertificate={d.get('trust_server_certificate','no')};",
        autocommit=False)


def query(sql, params=None):
    conn = make_conn()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    cols = [c[0] for c in cur.description] if cur.description else []
    rows = cur.fetchall()
    conn.close()
    return [dict(zip(cols, r)) for r in rows]


def load_credentials():
    rows = query("""
        SELECT e.base_url, cr.tss_username, cr.tss_password
        FROM CFG.Credentials cr
        JOIN CFG.Environments e ON e.env_code = cr.env_code
        WHERE cr.client_code=? AND cr.env_code=? AND cr.active=1
    """, [CLIENT_CODE, ENV_CODE])
    if not rows:
        con.print(f'[red]No active {ENV_CODE} credentials for {CLIENT_CODE}[/red]')
        sys.exit(1)
    return rows[0]


# ==============================================================
#  TSS API CLIENT
# ==============================================================
class TssApi:
    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip('/') + '/x_fhmrc_tss_api/v1/tss_api'
        self.session = requests.Session()
        b64 = base64.b64encode(f'{username}:{password}'.encode()).decode()
        self.session.headers.update({
            'Accept': 'application/json',
            'Authorization': f'Basic {b64}',
        })
        self.total_calls = 0
        self.errors = []

    def _get(self, endpoint, params):
        url = f'{self.base_url}/{endpoint}'
        display_params = {k: v for k, v in params.items() if k != 'fields'}
        param_str = '&'.join(f'{k}={v}' for k, v in display_params.items())
        con.print(f'    [dim]GET /{endpoint}?{param_str}[/dim]')
        t0 = time.time()
        try:
            r = self.session.get(url, params=params, timeout=API_TIMEOUT)
            self.total_calls += 1
            ms = int((time.time() - t0) * 1000)
            time.sleep(RATE_LIMIT)
            if r.status_code == 200:
                return 200, r.json().get('result'), r.text, ms
            self.errors.append(
                f'{endpoint}?{param_str} → HTTP {r.status_code}')
            return r.status_code, None, r.text[:500], ms
        except requests.exceptions.ReadTimeout:
            self.total_calls += 1
            ms = int((time.time() - t0) * 1000)
            self.errors.append(f'{endpoint}?{param_str} → TIMEOUT')
            return 0, None, 'TIMEOUT', ms
        except Exception as e:
            self.total_calls += 1
            ms = int((time.time() - t0) * 1000)
            self.errors.append(f'{endpoint}?{param_str} → {str(e)[:80]}')
            return 0, None, str(e)[:500], ms

    # ── Filter (discover refs) ────────────────────────────────
    def filter_resource(self, resource, status):
        return self._get(resource, {'filter': f'status={status}'})

    # ── Read (get detail) ─────────────────────────────────────
    def read_resource(self, resource, reference, fields):
        return self._get(resource, {
            'reference': reference, 'fields': fields})

    # ── Permission Grant ──────────────────────────────────────
    def check_permission(self, importer_eori):
        return self._get('permission_grant', {
            'importer_eori': importer_eori})


# ==============================================================
#  HELPERS
# ==============================================================
def sc(status):
    """Rich colour tag based on status keyword."""
    s = (status or '').lower()
    if any(k in s for k in ('authorised', 'arrived', 'accepted', 'cleared', 'closed')):
        return 'green'
    if any(k in s for k in ('submitted', 'processing', 'draft')):
        return 'yellow'
    if any(k in s for k in ('required', 'amendment')):
        return 'bright_yellow'
    if any(k in s for k in ('rejected', 'error', 'failed')):
        return 'red'
    if 'cancelled' in s:
        return 'dim red'
    return 'white'


def trunc(v, n=30):
    s = str(v or '')
    return s[:n] + '..' if len(s) > n else s


def extract_refs_from_filter(result):
    """
    Extract reference numbers from a TSS filter response.
    Handles all known response shapes:
      - List of dicts with 'number' key (most common)
      - List of dicts with 'reference'/'sfd_number'/'sup_dec_number' etc.
      - List of strings
      - Single dict
      - None / empty
    """
    if not result:
        return []

    if isinstance(result, list):
        refs = []
        for item in result:
            if isinstance(item, str):
                refs.append(item.strip())
            elif isinstance(item, dict):
                r = (item.get('number')
                     or item.get('reference')
                     or item.get('sfd_number')
                     or item.get('sup_dec_number')
                     or item.get('ffd_number')
                     or item.get('glr_number')
                     or item.get('dec_number')
                     or item.get('declaration_number')
                     or '')
                if r:
                    refs.append(r.strip())
        return refs

    if isinstance(result, dict):
        r = (result.get('number')
             or result.get('reference')
             or result.get('sfd_number')
             or result.get('sup_dec_number')
             or result.get('ffd_number')
             or result.get('glr_number')
             or result.get('dec_number')
             or result.get('declaration_number')
             or '')
        return [r.strip()] if r else []

    return []


# ==============================================================
#  PROBE ENGINE
# ==============================================================
def probe_resource(api, resource, statuses, label):
    """
    Probe a resource across all valid statuses.
    Returns: { status: [refs], ... }, set(all_refs)
    """
    con.print()
    con.rule(f'[bold cyan]{label}[/bold cyan]')
    con.print(f'  [dim]GET /{resource}?filter=status=<STATUS>[/dim]')
    con.print()

    by_status = {}
    all_refs = set()

    for status in statuses:
        http, result, raw, ms = api.filter_resource(resource, status)

        if http == 200:
            refs = extract_refs_from_filter(result)
            new = [r for r in refs if r not in all_refs]
            all_refs.update(refs)
            by_status[status] = refs
            colour = 'green' if refs else 'dim'
            con.print(
                f'  [{colour}]{status:<35}  '
                f'{len(refs):>5} refs  '
                f'({len(new)} new)  {ms}ms[/{colour}]')
        elif http == 400:
            by_status[status] = []
            con.print(
                f'  [dim red]{status:<35}  '
                f'HTTP 400 (invalid filter)  {ms}ms[/dim red]')
        else:
            by_status[status] = []
            con.print(
                f'  [red]{status:<35}  '
                f'HTTP {http}  {ms}ms[/red]')

    con.print(
        f'\n  [bold]Total unique {label} refs: {len(all_refs)}[/bold]')

    return by_status, all_refs


def read_sample(api, resource, refs, fields, label, max_sample=5):
    """
    Read a small sample of discovered refs to show data shape.
    Returns list of (ref, status, summary_dict) tuples.
    """
    if not refs:
        return []

    sample = sorted(refs)[:max_sample]
    results = []

    con.print(f'\n  [dim]Reading sample ({len(sample)} of {len(refs)})...[/dim]')

    for i, ref in enumerate(sample, 1):
        http, result, raw, ms = api.read_resource(resource, ref, fields)
        if http == 200 and result:
            status = result.get('status', '?')
            results.append((ref, status, result))
            # Build a one-line summary from a few key fields
            parts = [f'[{sc(status)}]{status}[/{sc(status)}]']
            for k in ['importer_eori', 'carrier_eori', 'arrival_port',
                       'arrival_date_time', 'goods_description',
                       'movement_type', 'declaration_category',
                       'port_of_arrival', 'mode_of_transport']:
                v = result.get(k)
                if v:
                    parts.append(f'{k}={trunc(v, 20)}')
                    if len(parts) >= 5:
                        break
            con.print(
                f'    {i:>2}  [cyan]{ref}[/cyan]  '
                + '  '.join(parts)
                + f'  [dim]{ms}ms[/dim]')
        else:
            con.print(
                f'    {i:>2}  [red]{ref}  HTTP {http}  {ms}ms[/red]')

    return results


# ==============================================================
#  HTML REPORT GENERATOR
# ==============================================================
def write_html(discovery, summary):
    """Generate an interactive HTML probe report."""
    import html as html_mod

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    html_parts = [f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Birkdale TSS Probe – {TIMESTAMP}</title>
<style>
  :root {{ --bg:#0b0e14; --bg2:#161b22; --panel:#1a2235; --border:#30363d;
           --accent:#3d7eff; --green:#22c55e; --yellow:#f59e0b; --red:#ef4444;
           --text:#c9d1d9; --dim:#6b7fa8; --mono:'Consolas',monospace; }}
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ font-family:-apple-system,sans-serif; background:var(--bg); color:var(--text); }}
  .container {{ max-width:1100px; margin:0 auto; padding:24px; }}
  h1 {{ color:#f0f6fc; font-size:22px; margin-bottom:4px; }}
  h2 {{ color:#f0f6fc; font-size:16px; margin:18px 0 10px; }}
  .subtitle {{ font-size:13px; color:var(--dim); margin-bottom:20px; }}
  .card {{ background:var(--panel); border:1px solid var(--border); border-radius:10px;
           padding:16px 20px; margin-bottom:14px; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ text-align:left; padding:6px 10px; color:var(--dim); font-size:11px;
        text-transform:uppercase; letter-spacing:.5px; border-bottom:1px solid var(--border); }}
  td {{ padding:6px 10px; border-bottom:1px solid rgba(48,54,61,.5); }}
  .mono {{ font-family:var(--mono); font-size:12px; }}
  .green {{ color:var(--green); }} .yellow {{ color:var(--yellow); }}
  .red {{ color:var(--red); }} .dim {{ color:var(--dim); }}
  .pill {{ display:inline-block; font-size:11px; font-weight:700; padding:2px 8px;
           border-radius:10px; }}
  .pill-green {{ background:rgba(34,197,94,.15); color:var(--green); }}
  .pill-yellow {{ background:rgba(245,158,11,.15); color:var(--yellow); }}
  .pill-red {{ background:rgba(239,68,68,.15); color:var(--red); }}
  .pill-dim {{ background:rgba(107,127,168,.15); color:var(--dim); }}
  .big-num {{ font-size:28px; font-weight:800; color:var(--accent); }}
  .stat-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; }}
  .stat-box {{ text-align:center; }}
  .stat-label {{ font-size:11px; color:var(--dim); text-transform:uppercase; }}
</style></head><body><div class="container">
<h1>TSS Birkdale – Test Environment Probe</h1>
<p class="subtitle">{CLIENT_NAME} ({CLIENT_CODE}) · {ENV_CODE} · {summary['api_base']} · {summary['generated']}</p>
"""]

    # Summary stats
    html_parts.append('<div class="card"><div class="stat-grid">')
    for label, count in summary['totals'].items():
        html_parts.append(
            f'<div class="stat-box">'
            f'<div class="big-num">{count}</div>'
            f'<div class="stat-label">{html_mod.escape(label)}</div></div>')
    html_parts.append(
        f'<div class="stat-box">'
        f'<div class="big-num">{summary["api_calls"]}</div>'
        f'<div class="stat-label">API Calls</div></div>')
    html_parts.append(
        f'<div class="stat-box">'
        f'<div class="big-num">{summary["elapsed"]:.0f}s</div>'
        f'<div class="stat-label">Runtime</div></div>')
    html_parts.append('</div></div>')

    # Per-resource breakdown
    for resource_key, data in discovery.items():
        by_status = data.get('by_status', {})
        total = data.get('total', 0)
        sample = data.get('sample', [])

        html_parts.append(
            f'<div class="card"><h2>{html_mod.escape(resource_key)} '
            f'<span class="dim">({total} refs)</span></h2>')

        # Status table
        html_parts.append(
            '<table><thead><tr><th>Status</th><th>Count</th></tr></thead><tbody>')
        for status, refs in by_status.items():
            n = len(refs)
            pill_class = 'pill-green' if n > 0 else 'pill-dim'
            html_parts.append(
                f'<tr><td>{html_mod.escape(status)}</td>'
                f'<td><span class="pill {pill_class}">{n}</span></td></tr>')
        html_parts.append('</tbody></table>')

        # Sample reads
        if sample:
            html_parts.append(
                '<h2 style="margin-top:14px">Sample Reads</h2>'
                '<table><thead><tr><th>Reference</th><th>Status</th>'
                '<th>Key Fields</th></tr></thead><tbody>')
            for ref, status, rec in sample:
                # Pick interesting fields
                fields_str = ', '.join(
                    f'{k}={trunc(v,25)}'
                    for k, v in rec.items()
                    if v and k != 'status' and not k.startswith('_')
                )[:200]
                scl = 'green' if 'closed' in (status or '').lower() or 'arrived' in (status or '').lower() else 'yellow'
                html_parts.append(
                    f'<tr><td class="mono">{html_mod.escape(str(ref))}</td>'
                    f'<td class="{scl}">{html_mod.escape(str(status))}</td>'
                    f'<td class="dim" style="font-size:11px">'
                    f'{html_mod.escape(fields_str)}</td></tr>')
            html_parts.append('</tbody></table>')

        html_parts.append('</div>')

    # Errors
    if summary.get('errors'):
        html_parts.append(
            '<div class="card"><h2>Errors / Warnings</h2><ul>')
        for err in summary['errors'][:30]:
            html_parts.append(
                f'<li class="dim mono" style="font-size:12px">'
                f'{html_mod.escape(err)}</li>')
        html_parts.append('</ul></div>')

    html_parts.append('</div></body></html>')

    with open(HTML_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html_parts))
    return HTML_FILE


# ==============================================================
#  MAIN
# ==============================================================
def main():
    t0 = time.time()

    con.print(Panel.fit(
        f'[bold yellow]{__product__}[/bold yellow]  |  '
        f'[bold white]{__module__}[/bold white]  v{__version__}\n'
        f'[bold cyan]{CLIENT_NAME}[/bold cyan]  |  '
        f'[dim]{CLIENT_CODE}  |  {ENV_CODE}  |  {DB_NAME}  |  '
        f'{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC[/dim]\n'
        f'[dim]Read-only probe — no data will be created or modified[/dim]',
        border_style='blue', padding=(0, 2)))

    # ── Preflight ─────────────────────────────────────────────
    con.print()
    con.rule('[bold cyan]Preflight[/bold cyan]')
    con.print()

    if not os.path.exists(INI_PATH):
        con.print(f'  [red]INI missing: {INI_PATH}[/red]')
        return
    con.print(f'  INI:    [green]OK[/green]')

    try:
        srv = query("SELECT @@SERVERNAME AS s")[0]['s']
        con.print(f'  DB:     [green]OK[/green]  {srv} / {DB_NAME}')
    except Exception as e:
        con.print(f'  DB:     [red]FAIL {e}[/red]')
        return

    creds = load_credentials()
    api_base = creds['base_url']
    con.print(f'  API:    [green]OK[/green]  {creds["tss_username"]}')
    con.print(f'  Base:   [dim]{api_base}[/dim]')
    con.print(f'  Env:    [bold yellow]{ENV_CODE}[/bold yellow]')

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    con.print(f'  Output: [dim]{OUTPUT_DIR}[/dim]')

    api = TssApi(api_base, creds['tss_username'], creds['tss_password'])

    # ── Connectivity Test ─────────────────────────────────────
    con.print()
    con.rule('[bold cyan]API Connectivity Test[/bold cyan]')
    con.print()

    http, result, raw, ms = api.filter_resource(
        'simplified_frontier_declarations', 'Arrived')
    if http == 200:
        refs = extract_refs_from_filter(result)
        con.print(
            f'  [green]API responding: HTTP 200  {ms}ms  '
            f'({len(refs)} SFDs with status "Arrived")[/green]')
    else:
        con.print(f'  [red]API test failed: HTTP {http}  {ms}ms[/red]')
        try:
            msg = json.loads(raw).get('result', {}).get('process_message', '')
            if msg:
                con.print(f'  [red]{msg}[/red]')
        except:
            con.print(f'  [red]{(raw or "")[:200]}[/red]')
        con.print(f'  [dim]Check credentials in CFG.Credentials '
                  f'for {CLIENT_CODE}/{ENV_CODE}[/dim]')
        return

    # ── Collection structure ──────────────────────────────────
    discovery = {}

    # ==========================================================
    #  PHASE 1: ENS Headers
    # ==========================================================
    by_status, all_refs = probe_resource(
        api, 'headers', ENS_STATUSES,
        'Phase 1 — ENS Headers')
    samples = read_sample(
        api, 'headers', all_refs, ENS_READ_FIELDS, 'ENS Headers')
    discovery['ENS Headers'] = {
        'resource': 'headers',
        'by_status': {k: v for k, v in by_status.items()},
        'total': len(all_refs),
        'refs': sorted(all_refs),
        'sample': samples,
    }

    # ==========================================================
    #  PHASE 2: SFDs
    # ==========================================================
    by_status, all_refs = probe_resource(
        api, 'simplified_frontier_declarations', SFD_STATUSES,
        'Phase 2 — Simplified Frontier Declarations')
    samples = read_sample(
        api, 'simplified_frontier_declarations', all_refs,
        SFD_READ_FIELDS, 'SFDs')
    discovery['SFDs'] = {
        'resource': 'simplified_frontier_declarations',
        'by_status': {k: v for k, v in by_status.items()},
        'total': len(all_refs),
        'refs': sorted(all_refs),
        'sample': samples,
    }

    # ==========================================================
    #  PHASE 3: Supplementary Declarations
    # ==========================================================
    by_status, all_refs = probe_resource(
        api, 'supplementary_declarations', SUP_DEC_STATUSES,
        'Phase 3 — Supplementary Declarations')
    samples = read_sample(
        api, 'supplementary_declarations', all_refs,
        SUP_DEC_READ_FIELDS, 'Sup Decs')
    discovery['Supplementary Declarations'] = {
        'resource': 'supplementary_declarations',
        'by_status': {k: v for k, v in by_status.items()},
        'total': len(all_refs),
        'refs': sorted(all_refs),
        'sample': samples,
    }

    # ==========================================================
    #  PHASE 4: Full Frontier Declarations
    # ==========================================================
    by_status, all_refs = probe_resource(
        api, 'full_frontier_declarations', FFD_STATUSES,
        'Phase 4 — Full Frontier Declarations')
    samples = read_sample(
        api, 'full_frontier_declarations', all_refs,
        FFD_READ_FIELDS, 'FFDs')
    discovery['Full Frontier Declarations'] = {
        'resource': 'full_frontier_declarations',
        'by_status': {k: v for k, v in by_status.items()},
        'total': len(all_refs),
        'refs': sorted(all_refs),
        'sample': samples,
    }

    # ==========================================================
    #  PHASE 5: Internal Market Movements
    # ==========================================================
    by_status, all_refs = probe_resource(
        api, 'internal_market_movements', IMMI_STATUSES,
        'Phase 5 — Internal Market Movements')
    samples = read_sample(
        api, 'internal_market_movements', all_refs,
        IMMI_READ_FIELDS, 'IMMIs')
    discovery['Internal Market Movements'] = {
        'resource': 'internal_market_movements',
        'by_status': {k: v for k, v in by_status.items()},
        'total': len(all_refs),
        'refs': sorted(all_refs),
        'sample': samples,
    }

    # ==========================================================
    #  PHASE 6: GVMS GMRs
    # ==========================================================
    by_status, all_refs = probe_resource(
        api, 'gvms', GVMS_STATUSES,
        'Phase 6 — GVMS GMRs')
    # GVMS read uses different params — skip sample reads for now
    discovery['GVMS GMRs'] = {
        'resource': 'gvms',
        'by_status': {k: v for k, v in by_status.items()},
        'total': len(all_refs),
        'refs': sorted(all_refs),
        'sample': [],
    }

    # ==========================================================
    #  PHASE 7: Permission Grant
    # ==========================================================
    con.print()
    con.rule('[bold cyan]Phase 7 — Permission Grant[/bold cyan]')
    con.print()

    # Try to discover the EORI from any declaration we found
    test_eoris = set()
    for key, data in discovery.items():
        for ref, status, rec in data.get('sample', []):
            for eori_field in ['importer_eori', 'carrier_eori']:
                eori = rec.get(eori_field)
                if eori:
                    test_eoris.add(eori)

    permissions = []
    if test_eoris:
        con.print(f'  [dim]Testing {len(test_eoris)} discovered EORIs...[/dim]')
        for eori in sorted(test_eoris)[:5]:
            http, result, raw, ms = api.check_permission(eori)
            if http == 200 and result:
                perms = result.get('permissions', [])
                con.print(
                    f'  [cyan]{eori}[/cyan]  '
                    f'[green]{len(perms)} permission(s)[/green]  {ms}ms')
                permissions.append({
                    'eori': eori, 'permissions': perms})
            else:
                con.print(
                    f'  [dim]{eori}  HTTP {http}  {ms}ms[/dim]')
    else:
        con.print(f'  [yellow]No EORIs discovered from samples — '
                  f'skipping permission check[/yellow]')

    discovery['Permission Grants'] = {
        'resource': 'permission_grant',
        'by_status': {},
        'total': len(permissions),
        'refs': [],
        'sample': [],
        'permissions': permissions,
    }

    # ==========================================================
    #  PHASE 8: Cross-Reference Summary
    # ==========================================================
    con.print()
    con.rule('[bold cyan]Phase 8 — Cross-Reference[/bold cyan]')
    con.print()

    # Count unique EORIs across all samples
    all_eoris = set()
    for key, data in discovery.items():
        for ref, status, rec in data.get('sample', []):
            for k, v in rec.items():
                if 'eori' in k.lower() and v:
                    all_eoris.add(v)

    con.print(f'  Unique EORIs discovered:  [cyan]{len(all_eoris)}[/cyan]')
    for eori in sorted(all_eoris):
        con.print(f'    [dim]{eori}[/dim]')

    # ==========================================================
    #  PHASE 9: Output + Summary
    # ==========================================================
    elapsed = time.time() - t0

    summary = {
        'product': __product__,
        'module': __module__,
        'version': __version__,
        'generated': datetime.now(timezone.utc).isoformat(),
        'client': CLIENT_CODE,
        'client_name': CLIENT_NAME,
        'env': ENV_CODE,
        'api_base': api_base,
        'api_calls': api.total_calls,
        'elapsed': elapsed,
        'errors': api.errors,
        'totals': {
            k: v['total'] for k, v in discovery.items()
        },
        'eoris': sorted(all_eoris),
    }

    # ── JSON ──────────────────────────────────────────────────
    con.print()
    con.rule('[bold cyan]Output Files[/bold cyan]')
    con.print()

    # Serialise (strip raw result dicts from samples for JSON)
    json_discovery = {}
    for key, data in discovery.items():
        json_discovery[key] = {
            'resource': data['resource'],
            'by_status': {
                s: len(refs) for s, refs in data.get('by_status', {}).items()
            },
            'total': data['total'],
            'refs': data.get('refs', []),
            'sample': [
                {'reference': ref, 'status': status,
                 'fields': {k: str(v)[:200] for k, v in rec.items()
                            if v and not k.startswith('_')}}
                for ref, status, rec in data.get('sample', [])
            ],
        }
        if 'permissions' in data:
            json_discovery[key]['permissions'] = data['permissions']

    json_out = {'metadata': summary, 'discovery': json_discovery}
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(json_out, f, indent=2, default=str, ensure_ascii=False)
    con.print(f'  JSON:  [green]{JSON_FILE}[/green]')

    # ── HTML ──────────────────────────────────────────────────
    html_path = write_html(discovery, summary)
    con.print(f'  HTML:  [green]{html_path}[/green]')

    # ── Summary Table ─────────────────────────────────────────
    con.print()
    con.rule('[bold yellow]Probe Complete[/bold yellow]')
    con.print()

    tbl = Table(
        box=box.ROUNDED,
        title=f'[bold]{CLIENT_NAME} ({CLIENT_CODE}) — '
              f'{ENV_CODE} Test Environment Probe[/bold]',
        border_style='green')
    tbl.add_column('Resource', style='cyan', min_width=38)
    tbl.add_column('Refs Found', justify='right', style='green')

    for key, data in discovery.items():
        total = data['total']
        style = '[bold green]' if total > 0 else '[dim]'
        tbl.add_row(key, f'{style}{total}[/]')

    tbl.add_row('')
    tbl.add_row('[bold]Unique EORIs[/bold]', str(len(all_eoris)))
    tbl.add_row('')
    tbl.add_row('[bold]Total API Calls[/bold]',
                f'[bold]{api.total_calls}[/bold]')
    tbl.add_row('Runtime', f'{elapsed:.0f}s')
    rate = api.total_calls / elapsed if elapsed > 0 else 0
    tbl.add_row('Throughput', f'{rate:.1f} calls/s')
    tbl.add_row('')
    tbl.add_row('JSON', JSON_FILE)
    tbl.add_row('HTML', HTML_FILE)

    if api.errors:
        tbl.add_row('')
        tbl.add_row('[red]Errors / Warnings[/red]',
                    f'[red]{len(api.errors)}[/red]')

    con.print(tbl)

    # ── Verdict ───────────────────────────────────────────────
    con.print()
    total_all = sum(d['total'] for d in discovery.values())
    if total_all > 0:
        con.print(
            f'  [bold green]Data found![/bold green]  '
            f'{total_all} total declaration references across '
            f'{sum(1 for d in discovery.values() if d["total"] > 0)} '
            f'resource types.')
        con.print(
            f'  [dim]Next: populate BRK schema tables from discovered refs, '
            f'or create new test declarations.[/dim]')
    else:
        con.print(
            f'  [bold yellow]Clean slate.[/bold yellow]  '
            f'No existing declarations found for {CLIENT_CODE}/{ENV_CODE}.')
        con.print(
            f'  [dim]This account is ready for fresh test data. '
            f'Use the Job upload scripts to create declarations.[/dim]')

    con.print()
    con.print(
        f'  [dim]{__product__} v{__version__} — {__module__} — '
        f'{CLIENT_NAME} — Synovia Digital Ltd[/dim]')
    con.print()


if __name__ == '__main__':
    main()
