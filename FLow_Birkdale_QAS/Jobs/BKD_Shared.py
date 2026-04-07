"""
================================================================================
  Synovia Flow -- BKD Shared Configuration & Utilities
  Licensed Component: Synovia Digital Ltd
================================================================================

  Version:  1.0.0
  Schema:   BKD (Birkdale)
  API:      TSS Declaration API v2.9.4

  Shared module imported by all BKD_Create_*.py scripts.
  Provides: DB access, API client, ApiLog writer, Rich console helpers.

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

__version__ = '1.0.0'
__product__ = 'Synovia Flow'
__suite__   = 'BKD TSS Declaration Scripts'

import base64, configparser, json, os, sys, time
from datetime import datetime, timezone
import pyodbc, requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# ──────────────────────────────────────────────────────────────
#  CONFIGURATION
# ──────────────────────────────────────────────────────────────
CLIENT_CODE  = 'BKD'
CLIENT_NAME  = 'Birkdale'
ENV_CODE     = 'TST'                           # TST | PRD
INI_PATH     = r'D:\Configuration\Fusion_TSS.ini'
S            = CLIENT_CODE                     # schema alias
OUTPUT_DIR   = r'D:\Flow_Testing\BKD'
DRY_RUN      = '--dry-run' in sys.argv
RATE_LIMIT   = 0.3                             # seconds between API calls
API_TIMEOUT  = 30                              # seconds

# Environment → INI section + database mapping
#   TST  →  [QAS_Database]  →  Fusion_TSS
#   PRD  →  [database]      →  Fusion_TSS_PRD
ENV_MAP = {
    'TST': {'ini_section': 'QAS_Database', 'db_name': 'Fusion_TSS'},
    'PRD': {'ini_section': 'database',     'db_name': 'Fusion_TSS_PRD'},
}
DB_NAME = ENV_MAP[ENV_CODE]['db_name']

# act_as configuration  --  ENS Headers can be read without actAs,
# but Consignments and Supplementary Declarations require it.
ACT_AS_EORI     = None   # set per-client if needed, e.g. 'XI123456789000'
ACT_AS_CUSTOMER = None   # set per-client if needed, e.g. 'Birkdale'

INVALID_PATTERNS = [
    'invalid format', 'invalid value', 'is required', 'must be',
    'cannot be blank', 'not a valid', 'choice field', 'regex validation',
    'mandatory field',
]

con = Console(highlight=False, width=140)


# ──────────────────────────────────────────────────────────────
#  DATABASE
# ──────────────────────────────────────────────────────────────
def make_conn():
    """Create a new pyodbc connection from INI config."""
    cfg = configparser.ConfigParser()
    if not cfg.read(INI_PATH):
        con.print(f'[red]Cannot read INI file: {INI_PATH}[/red]')
        sys.exit(1)
    section = ENV_MAP[ENV_CODE]['ini_section']
    if section not in cfg:
        con.print(f'[red]INI section [{section}] not found for ENV_CODE={ENV_CODE}[/red]')
        con.print(f'[red]Sections found: {cfg.sections()}[/red]')
        sys.exit(1)
    d = cfg[section]
    return pyodbc.connect(
        f"DRIVER={d['driver']};SERVER={d['server']};DATABASE={DB_NAME};"
        f"UID={d['user']};PWD={d['password']};"
        f"Encrypt={d.get('encrypt', 'yes')};"
        f"TrustServerCertificate={d.get('trust_server_certificate', 'no')};",
        autocommit=False,
    )


def query(sql, params=None):
    """Execute SELECT, return list of dicts."""
    conn = make_conn()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    cols = [c[0] for c in cur.description] if cur.description else []
    rows = cur.fetchall()
    conn.close()
    return [dict(zip(cols, r)) for r in rows]


def execute(sql, params=None):
    """Execute INSERT/UPDATE/DELETE, return rowcount."""
    conn = make_conn()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected


def sget(row, key, default=''):
    """Safe-get a value from a dict row, returning default if None/missing."""
    v = row.get(key)
    return v if v is not None else default


# ──────────────────────────────────────────────────────────────
#  CREDENTIALS
# ──────────────────────────────────────────────────────────────
def load_credentials():
    """Load API credentials for current client + environment."""
    rows = query("""
        SELECT e.base_url, cr.tss_username, cr.tss_password
        FROM CFG.Credentials cr
        JOIN CFG.Environments e ON e.env_code = cr.env_code
        WHERE cr.client_code = ? AND cr.env_code = ? AND cr.active = 1
    """, [CLIENT_CODE, ENV_CODE])
    if not rows:
        con.print(f'[red]No active {ENV_CODE} credentials for {CLIENT_CODE}[/red]')
        sys.exit(1)
    return rows[0]


# ──────────────────────────────────────────────────────────────
#  API CLIENT
# ──────────────────────────────────────────────────────────────
class TssApi:
    """TSS Declaration API v2.9.4 client with Basic Auth."""

    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip('/') + '/x_fhmrc_tss_api/v1/tss_api'
        self.session = requests.Session()
        b64 = base64.b64encode(f'{username}:{password}'.encode()).decode()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Basic {b64}',
        })
        self.total_calls = 0

    def post(self, endpoint, payload):
        """POST to TSS API. Returns (http_status, result_dict, raw_text, duration_ms)."""
        url = f'{self.base_url}/{endpoint}'
        t0 = time.time()
        try:
            r = self.session.post(url, json=payload, timeout=API_TIMEOUT)
            self.total_calls += 1
            ms = int((time.time() - t0) * 1000)
            time.sleep(RATE_LIMIT)
            try:
                body = r.json()
            except Exception:
                body = {}
            return r.status_code, body.get('result', {}), r.text, ms
        except requests.exceptions.ReadTimeout:
            self.total_calls += 1
            return 0, {}, 'TIMEOUT', int((time.time() - t0) * 1000)
        except Exception as e:
            self.total_calls += 1
            return 0, {}, str(e)[:500], int((time.time() - t0) * 1000)

    def get(self, endpoint, params=None):
        """GET from TSS API. Returns (http_status, result_dict, raw_text, duration_ms)."""
        url = f'{self.base_url}/{endpoint}'
        t0 = time.time()
        try:
            r = self.session.get(url, params=params, timeout=API_TIMEOUT)
            self.total_calls += 1
            ms = int((time.time() - t0) * 1000)
            time.sleep(RATE_LIMIT)
            try:
                body = r.json()
            except Exception:
                body = {}
            return r.status_code, body.get('result', {}), r.text, ms
        except requests.exceptions.ReadTimeout:
            self.total_calls += 1
            return 0, {}, 'TIMEOUT', int((time.time() - t0) * 1000)
        except Exception as e:
            self.total_calls += 1
            return 0, {}, str(e)[:500], int((time.time() - t0) * 1000)


# ──────────────────────────────────────────────────────────────
#  API LOG  --  BKD.ApiLog
# ──────────────────────────────────────────────────────────────
def log_api_call(declaration_type, call_type, reference,
                 http_method, url, request_params,
                 http_status, response_status, response_message,
                 response_json, duration_ms, error_detail='', notes='',
                 act_as=None, act_as_customer=None):
    """
    Write a comprehensive row to BKD.ApiLog capturing every detail
    of the API interaction.
    """
    execute(f"""
        INSERT INTO {S}.ApiLog (
            logged_at, declaration_type, call_type, reference,
            act_as, act_as_customer,
            http_method, url, request_params,
            http_status, response_status, response_message, response_json,
            duration_ms, error_detail, notes
        ) VALUES (
            SYSUTCDATETIME(), ?, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?
        )
    """, [
        declaration_type,
        call_type,
        (reference or '')[:200],
        act_as or ACT_AS_EORI or '',
        act_as_customer or ACT_AS_CUSTOMER or '',
        http_method,
        (url or '')[:500],
        json.dumps(request_params, separators=(',', ':'), default=str)[:4000] if request_params else '',
        http_status,
        (response_status or '')[:100],
        (response_message or '')[:500],
        (response_json or '')[:4000],
        duration_ms,
        (error_detail or '')[:4000],
        (notes or '')[:500],
    ])


# ──────────────────────────────────────────────────────────────
#  JSON FILE LOGGER  (per-run diagnostic dump)
# ──────────────────────────────────────────────────────────────
class JsonLogger:
    """Writes per-call JSON files + a master summary to OUTPUT_DIR."""

    def __init__(self, run_id):
        self.run_id = run_id
        self.calls_dir = os.path.join(OUTPUT_DIR, f'{run_id}_calls')
        os.makedirs(self.calls_dir, exist_ok=True)
        self.all_calls = []
        self.call_counter = 0

    def log_call(self, decl_type, call_type, reference, endpoint, method,
                 request_payload, http_status, response_body, raw_text,
                 duration_ms, result_status, notes=''):
        self.call_counter += 1
        entry = {
            'call_number': self.call_counter,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'run_id': self.run_id,
            'declaration_type': decl_type,
            'call_type': call_type,
            'reference': reference,
            'endpoint': endpoint,
            'http_method': method,
            'request': {
                'url': f'<base>/x_fhmrc_tss_api/v1/tss_api/{endpoint}',
                'payload': request_payload,
            },
            'response': {
                'http_status': http_status,
                'status': result_status,
                'body': None,
                'raw': raw_text[:4000] if raw_text else '',
            },
            'duration_ms': duration_ms,
            'success': http_status == 200 and result_status in ('created', 'updated', 'success'),
            'notes': notes,
        }
        try:
            entry['response']['body'] = json.loads(raw_text) if raw_text else {}
        except Exception:
            entry['response']['body'] = {'raw': raw_text[:2000] if raw_text else ''}
        self.all_calls.append(entry)
        fname = f'{self.call_counter:03d}_{decl_type}_{call_type}_{http_status}.json'
        with open(os.path.join(self.calls_dir, fname), 'w', encoding='utf-8') as f:
            json.dump(entry, f, indent=2, default=str)
        return entry

    def write_summary(self, extra=None):
        summary = {
            'run_id': self.run_id,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'product': __product__,
            'suite': __suite__,
            'version': __version__,
            'client': CLIENT_NAME,
            'client_code': CLIENT_CODE,
            'environment': ENV_CODE,
            'database': DB_NAME,
            'total_api_calls': self.call_counter,
            'calls': self.all_calls,
        }
        if extra:
            summary.update(extra)
        master = os.path.join(OUTPUT_DIR, f'{self.run_id}.json')
        with open(master, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)
        return master


# ──────────────────────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────────────────────
def is_retryable(msg):
    """Returns True if the error message does NOT contain known invalid-data patterns."""
    return not any(p in (msg or '').lower() for p in INVALID_PATTERNS)


def sc(status):
    """Rich colour tag for a pipeline status."""
    s = (status or '').lower()
    if s in ('created', 'synced', 'success'):
        return 'green'
    if s == 'pending':
        return 'yellow'
    if s == 'submitted':
        return 'cyan'
    if s == 'failed':
        return 'red'
    if s == 'invalid':
        return 'dim red'
    return 'white'


def make_run_id(prefix):
    """Generate a timestamped run ID, e.g. BKD_ENS_20260406_143012."""
    return f'{CLIENT_CODE}_{prefix}_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}'


def print_banner(script_name, run_id):
    """Print the standard Synovia Flow banner."""
    con.print(Panel.fit(
        f'[bold yellow]{__product__}[/bold yellow]  |  '
        f'[bold white]{script_name}[/bold white]  v{__version__}\n'
        f'[bold cyan]{CLIENT_NAME}[/bold cyan]  |  '
        f'[dim]{CLIENT_CODE}  |  {ENV_CODE}  |  {DB_NAME}  |  Run: {run_id}[/dim]\n'
        f'[dim]JSON output: {OUTPUT_DIR}[/dim]'
        f'{"  [bold red]DRY RUN[/bold red]" if DRY_RUN else ""}',
        border_style='blue', padding=(0, 2),
    ))


def print_creds(creds):
    """Print credential check."""
    con.print()
    con.print(f'  API:    [green]OK[/green]  {creds["tss_username"]}')
    con.print(f'  Base:   {creds["base_url"]}')
    con.print(f'  Env:    [bold yellow]{ENV_CODE}[/bold yellow]')
    if ACT_AS_EORI:
        con.print(f'  ActAs:  [cyan]{ACT_AS_EORI}[/cyan]  ({ACT_AS_CUSTOMER or "—"})')
    con.print()


def print_summary_table(title, rows_data):
    """
    Print a summary table.
    rows_data: list of (label, created_count, failed_count)
    """
    tbl = Table(box=box.ROUNDED, title=f'[bold]{title}[/bold]', border_style='green')
    tbl.add_column('Declaration', style='cyan', min_width=25)
    tbl.add_column('Created', justify='right', style='green')
    tbl.add_column('Failed', justify='right', style='red')
    for label, ok, fail in rows_data:
        tbl.add_row(label, str(ok), str(fail))
    con.print(tbl)


def print_run_footer(run_id, jlog, errors, elapsed):
    """Print JSON output paths and footer."""
    con.print()
    con.rule('[bold blue]JSON Output[/bold blue]')
    master = os.path.join(OUTPUT_DIR, f'{run_id}.json')
    con.print(f'  [green]Master:[/green]  {master}')
    con.print(f'  [green]Calls:[/green]   {jlog.calls_dir}  ({jlog.call_counter} files)')
    if errors:
        con.print()
        con.print('  [bold red]Errors:[/bold red]')
        for e in errors:
            con.print(f'    [red]{e.get("type","?")} #{e.get("staging_id","?")} '
                      f'HTTP {e.get("http","?")} {(e.get("message",""))[:80]}[/red]')
    con.print()
    con.print(f'  [dim]Elapsed: {elapsed:.1f}s  |  API calls: {jlog.call_counter}[/dim]')
    con.print(f'  [dim]{__product__} v{__version__} -- {CLIENT_NAME} -- Synovia Digital Ltd[/dim]')
    con.print()
