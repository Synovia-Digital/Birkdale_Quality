"""
================================================================================
  Synovia Flow -- TSS Birkdale DB-Driven Spider v2.0.0
  Licensed Component: Synovia Digital Ltd
================================================================================

  Version:      2.0.0
  Changes from v1:
    - Downloads choice values LIVE from TSS API (not hardcoded)
    - Updates TSS.CV_* tables with old/new change tracking
    - Seeds include new staging-created refs (ENS 421669/670, DEC 72647-72649)
    - Run log tracks every change, every discovery
    - Full field reads on all discovered refs

  Phase 0:  Download choice values from API choice_values endpoint
  Phase 1:  Compare + update TSS.CV_* tables (log changes)
  Phase 2:  Filter sweep: every resource x every fresh status
  Phase 3:  Full reads + cross-reference chase
  Phase 4:  Goods lookup from every parent
  Phase 5:  Permission Grant
  Phase 6:  Output JSON + run log

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

__version__ = '2.0.0'
__product__ = 'Synovia Flow'
__module__  = 'TSS Birkdale DB-Driven Spider'

import base64, configparser, json, os, sys, time
from datetime import datetime, timezone
import pyodbc, requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

con = Console(highlight=False, width=140)

CLIENT_CODE = 'BKD'
CLIENT_NAME = 'Birkdale'
ENV_CODE    = 'TST'
DB_NAME     = 'Fusion_TSS'
INI_PATH    = r'D:\confguration\fusion_TSS.ini'
S           = CLIENT_CODE

RATE_LIMIT  = 0.15
API_TIMEOUT = 30
LOG_BATCH   = 20

TIMESTAMP   = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_DIR  = r'D:\TSS_Madrid\Birkdale'
JSON_FILE   = os.path.join(OUTPUT_DIR, f'brk_spider_{TIMESTAMP}.json')

# ── Seeds: original + staging-created ─────────────────────────
SEED_REFS = {
    'sfd':         ['DEC000000001072379','DEC000000001072382'],
    'consignment': ['DEC000000001072378','DEC000000001072377',
                    'DEC000000001072647','DEC000000001072648','DEC000000001072649'],
    'ens_header':  ['ENS000000000421555','ENS000000000421669','ENS000000000421670'],
    'eori':        ['XI000012340005'],
}

# ── Choice values to download from API ────────────────────────
# Format: (api_field_name, cv_table_name, is_status_for_filter, filter_resource, ref_type)
CHOICE_VALUE_FIELDS = [
    ('sd_status',       'CV_sd_status',       True,  'supplementary_declarations', 'sup_dec'),
    ('movement_type',   'CV_movement_type',   False, None, None),
    ('port',            'CV_port',            False, None, None),
    ('route',           'CV_route',           False, None, None),
    ('transport_charge','CV_transport_charge', False, None, None),
    ('type_of_package', 'CV_type_of_package', False, None, None),
    ('incoterm',        'CV_incoterm',        False, None, None),
    ('procedure_code',  'CV_procedure_code',  False, None, None),
    ('controlled_goods_type','CV_controlled_goods_type', False, None, None),
    ('passive_transport_types','CV_passive_transport_types', False, None, None),
    ('sd_location_of_goods','CV_sd_location_of_goods', False, None, None),
    ('ffd_location_of_goods','CV_ffd_location_of_goods', False, None, None),
    ('document_status', 'CV_document_status', False, None, None),
    ('goods_domestic_status','CV_goods_domestic_status', False, None, None),
    ('currency',        'CV_currency',        False, None, None),
    ('preference',      'CV_preference',      False, None, None),
    ('valuation_method','CV_valuation_method', False, None, None),
]

# ── Statuses not in choice_values API (from field specs) ──────
SPEC_STATUSES = {
    'simplified_frontier_declarations': ('sfd', [
        'Draft','Submitted','Processing','Trader Input Required',
        'Authorised for Movement','Authorised for movement',
        'Amendment Required','Arrived','Cancelled']),
    'headers': ('ens_header', [
        'Draft','Submitted','Processing','Trader Input Required',
        'Authorised for Movement','Authorised for movement',
        'Arrived','Cancelled']),
    'full_frontier_declarations': ('ffd', [
        'Draft','Submitted','Processing','Trader Input Required',
        'Arrived','Cancelled']),
    'internal_market_movements': ('immi', [
        'Draft','Submitted','Processing','Trader Input Required',
        'Cancelled']),
    'gvms': ('gvms', ['Draft','Submitted','Cancelled']),
}

# ── Field sets ────────────────────────────────────────────────
SFD_FIELDS = ('status,goods_description,trader_reference,transport_document_number,'
    'controlled_goods,goods_domestic_status,destination_country,'
    'consignor_eori,consignor_name,consignee_eori,consignee_name,'
    'importer_eori,importer_name,exporter_eori,exporter_name,'
    'total_packages,gross_mass_kg,movement_reference_number,eori_for_eidr,'
    'ens_consignment_reference,error_message,declaration_choice')
SD_FIELDS = ('status,movement_reference_number,error_message,trader_reference,'
    'duty_totals,importer_eori,importer_name,arrival_date_time,port_of_arrival,'
    'transport_document_number,submission_due_date,total_packages,clear_date_time,'
    'declaration_choice,goods_description,exporter_eori,exporter_name')
CONS_FIELDS = ('status,declaration_number,goods_description,trader_reference,'
    'transport_document_number,controlled_goods,consignor_eori,consignor_name,'
    'importer_eori,importer_name,total_packages,gross_mass_kg,'
    'movement_reference_number,error_message')
ENS_FIELDS = ('status,movement_type,identity_no_of_transport,'
    'nationality_of_transport,arrival_date_time,arrival_port,'
    'place_of_loading,place_of_unloading,seal_number,route,'
    'carrier_eori,carrier_name,haulier_eori,error_message')
GOODS_FIELDS = ('consignment_number,goods_description,commodity_code,'
    'type_of_packages,number_of_packages,gross_mass_kg,net_mass_kg,'
    'country_of_origin,item_invoice_amount,item_invoice_currency,'
    'procedure_code,additional_procedure_codes,customs_value,'
    'controlled_goods,package_marks')


# ── Database ──────────────────────────────────────────────────
def make_conn():
    cfg = configparser.ConfigParser(); cfg.read(INI_PATH)
    d = cfg['database']
    return pyodbc.connect(
        f"DRIVER={d['driver']};SERVER={d['server']};DATABASE={DB_NAME};"
        f"UID={d['user']};PWD={d['password']};"
        f"Encrypt={d.get('encrypt','yes')};"
        f"TrustServerCertificate={d.get('trust_server_certificate','no')};",
        autocommit=False)

def query(sql, params=None):
    conn = make_conn(); cur = conn.cursor()
    cur.execute(sql, params or [])
    cols = [c[0] for c in cur.description] if cur.description else []
    rows = cur.fetchall(); conn.close()
    return [dict(zip(cols, r)) for r in rows]

def execute(sql, params=None):
    conn = make_conn(); cur = conn.cursor()
    cur.execute(sql, params or []); conn.commit(); conn.close()

def load_credentials():
    rows = query("""
        SELECT e.base_url, cr.tss_username, cr.tss_password
        FROM CFG.Credentials cr JOIN CFG.Environments e ON e.env_code=cr.env_code
        WHERE cr.client_code=? AND cr.env_code=? AND cr.active=1
    """, [CLIENT_CODE, ENV_CODE])
    if not rows: con.print(f'[red]No creds[/red]'); sys.exit(1)
    return rows[0]


class ApiLogger:
    def __init__(self):
        self.buffer = []; self.total_flushed = 0
    def log(self, dt, ref, ep, params, http, raw, ms, notes=''):
        self.buffer.append((
            (dt or '')[:50], 'READ', (ref or '')[:200], None, CLIENT_CODE, 'GET',
            (ep or '')[:500], json.dumps(params, separators=(',',':'))[:4000],
            http, 'OK' if http==200 else 'FAIL',
            (raw or '')[:500], (raw or '')[:4000], ms,
            '' if http==200 else (raw or '')[:4000],
            (notes or f'Spider v{__version__}')[:200]))
        if len(self.buffer) >= LOG_BATCH: self.flush()
    def flush(self):
        if not self.buffer: return
        try:
            conn = make_conn(); cur = conn.cursor()
            cur.executemany(f"""
                INSERT INTO {S}.ApiLog (declaration_type,call_type,reference,
                    act_as,act_as_customer,http_method,url,request_params,
                    http_status,response_status,response_message,response_json,
                    duration_ms,error_detail,notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", self.buffer)
            conn.commit(); conn.close()
            self.total_flushed += len(self.buffer)
        except Exception as e:
            con.print(f'    [dim red]ApiLog: {e}[/dim red]')
        self.buffer.clear()


class TssApi:
    def __init__(self, base_url, username, password, logger):
        self.api_base = base_url.rstrip('/') + '/x_fhmrc_tss_api/v1'
        self.tss_base = self.api_base + '/tss_api'
        self.cv_base  = self.api_base + '/choice_values'
        self.logger = logger; self.session = requests.Session()
        b64 = base64.b64encode(f'{username}:{password}'.encode()).decode()
        self.session.headers.update({'Accept':'application/json','Authorization':f'Basic {b64}'})
        self.total_calls = 0

    def _get(self, ep, params, dt='', ref='', notes='', silent=False):
        url = f'{self.tss_base}/{ep}'
        dp = {k:v for k,v in params.items() if k!='fields'}
        ps = '&'.join(f'{k}={v}' for k,v in dp.items())
        if not silent: con.print(f'    [dim]GET /{ep}?{ps}[/dim]')
        t0 = time.time()
        try:
            r = self.session.get(url, params=params, timeout=API_TIMEOUT)
            self.total_calls += 1; ms = int((time.time()-t0)*1000); time.sleep(RATE_LIMIT)
            self.logger.log(dt, ref, ep, params, r.status_code, r.text[:4000], ms, notes=notes)
            if r.status_code == 200: return 200, r.json().get('result'), r.text, ms
            return r.status_code, None, r.text[:500], ms
        except Exception as e:
            self.total_calls += 1; ms = int((time.time()-t0)*1000)
            self.logger.log(dt, ref, ep, params, 0, str(e)[:500], ms, notes=notes)
            return 0, None, str(e)[:500], ms

    def get_choice_values(self, field_name):
        """Download choice values from /choice_values/<field_name>"""
        url = f'{self.cv_base}/{field_name}'
        con.print(f'    [dim]GET /choice_values/{field_name}[/dim]')
        t0 = time.time()
        try:
            r = self.session.get(url, timeout=API_TIMEOUT)
            self.total_calls += 1; ms = int((time.time()-t0)*1000); time.sleep(RATE_LIMIT)
            self.logger.log('CV_DOWNLOAD', field_name, f'choice_values/{field_name}',
                           {}, r.status_code, r.text[:4000], ms,
                           notes=f'Download CV {field_name}')
            if r.status_code == 200:
                result = r.json().get('result', [])
                if isinstance(result, list): return result, ms
                return [], ms
            return [], ms
        except Exception as e:
            self.total_calls += 1; ms = int((time.time()-t0)*1000)
            return [], ms


def sget(d, k, default=''):
    return d.get(k, default) if isinstance(d, dict) else default
def trunc(v, n=30):
    s = str(v or ''); return s[:n]+'..' if len(s)>n else s
def sc(st):
    s = (st or '').lower()
    if any(k in s for k in ('authorised','arrived','accepted','cleared','closed')): return 'green'
    if any(k in s for k in ('submitted','processing','draft')): return 'yellow'
    if any(k in s for k in ('required','amendment')): return 'bright_yellow'
    if any(k in s for k in ('rejected','error','failed')): return 'red'
    if 'cancelled' in s: return 'dim red'
    return 'white'
def extract_refs(result):
    if not result: return []
    if isinstance(result, list):
        return [str(i.get('number') or i.get('reference') or i.get('sfd_number')
                or i.get('sup_dec_number') or i.get('ffd_number') or i.get('declaration_number') or '').strip()
                for i in result if isinstance(i, dict)
                and (i.get('number') or i.get('reference') or i.get('sfd_number')
                     or i.get('sup_dec_number') or i.get('ffd_number') or i.get('declaration_number'))]
    if isinstance(result, dict):
        r = (result.get('number') or result.get('reference') or result.get('sfd_number')
             or result.get('sup_dec_number') or result.get('declaration_number') or '')
        return [r.strip()] if r else []
    return []
def extract_goods_ids(result):
    if not result: return []
    items = result if isinstance(result, list) else [result]
    if len(items)==1 and isinstance(items[0], dict):
        for key in ('goods','goods_items'):
            if key in items[0]:
                sub = items[0][key]; items = sub if isinstance(sub, list) else [sub]; break
    return [str(i.get('goods_id') or i.get('reference') or '') for i in items
            if isinstance(i, dict) and (i.get('goods_id') or i.get('reference'))]
def extract_eoris(rec):
    return {v.strip() for k, v in rec.items() if 'eori' in k.lower() and v and isinstance(v, str)} if isinstance(rec, dict) else set()


def main():
    t0 = time.time()
    run_log = []  # [(timestamp, action, detail)]

    def log_event(action, detail=''):
        ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
        run_log.append((ts, action, detail))
        con.print(f'  [{("green" if "OK" in action or "Updated" in action or "Created" in action else "dim")}]{ts}  {action}  {detail}[/]')

    con.print(Panel.fit(
        f'[bold yellow]{__product__}[/bold yellow]  |  '
        f'[bold white]{__module__}[/bold white]  v{__version__}\n'
        f'[bold cyan]{CLIENT_NAME}[/bold cyan]  |  '
        f'[dim]{CLIENT_CODE}  |  {ENV_CODE}  |  {DB_NAME}  |  '
        f'{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC[/dim]\n'
        f'[dim]Live choice values download + full spider[/dim]',
        border_style='blue', padding=(0, 2)))

    con.print(); con.rule('[bold cyan]Preflight[/bold cyan]'); con.print()
    if not os.path.exists(INI_PATH): con.print(f'  [red]INI missing[/red]'); return
    try: lc = query(f"SELECT COUNT(*) AS c FROM {S}.ApiLog")[0]['c']
    except: con.print(f'  [red]ApiLog missing[/red]'); return
    con.print(f'  ApiLog: {lc} rows')
    creds = load_credentials()
    con.print(f'  API: {creds["tss_username"]}')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger = ApiLogger()
    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'], logger)
    log_event('Preflight OK', f'ApiLog={lc} rows')

    # ══════════════════════════════════════════════════════════
    #  PHASE 0: Download choice values from API
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase 0 -- Download Choice Values from API[/bold cyan]'); con.print()

    cv_downloads = {}  # {table_name: [api_rows]}
    cv_changes = {}    # {table_name: {added:[], removed:[], unchanged:int}}
    db_statuses = {}   # {api_resource: [values]} for filter sweep

    for api_field, table_name, is_status, filter_resource, ref_type in CHOICE_VALUE_FIELDS:
        api_rows, ms = api.get_choice_values(api_field)
        api_values = [r.get('value','') for r in api_rows if isinstance(r, dict) and r.get('value')]

        cv_downloads[table_name] = api_rows
        log_event(f'CV Download: {api_field}', f'{len(api_rows)} values, {ms}ms')

        if not api_values:
            con.print(f'    [yellow]{api_field}: 0 values (endpoint may not exist)[/yellow]')
            continue

        # Compare with existing table
        try:
            existing = query(f"SELECT [value] FROM TSS.[{table_name}] ORDER BY [id]")
            old_values = set(r['value'] for r in existing)
        except:
            old_values = set()
            log_event(f'CV Table Missing: {table_name}', 'Will skip update')

        new_values = set(api_values)
        added = new_values - old_values
        removed = old_values - new_values
        unchanged = len(old_values & new_values)

        cv_changes[table_name] = {
            'added': sorted(added), 'removed': sorted(removed),
            'unchanged': unchanged, 'total_api': len(api_values),
            'total_db': len(old_values),
        }

        if added or removed:
            con.print(f'    [green]{table_name}: {len(api_values)} from API  '
                      f'(+{len(added)} new, -{len(removed)} removed, {unchanged} unchanged)[/green]')
            # Update table: drop and reload
            try:
                execute(f"DELETE FROM TSS.[{table_name}]")
                conn = make_conn(); cur = conn.cursor()
                for i, row in enumerate(api_rows, 1):
                    if isinstance(row, dict) and row.get('value'):
                        cur.execute(f"INSERT INTO TSS.[{table_name}] (id, value, name, loaded_at) VALUES (?,?,?,SYSUTCDATETIME())",
                                   [i, row['value'], row.get('name', row['value'])])
                conn.commit(); conn.close()
                log_event(f'CV Updated: {table_name}', f'+{len(added)} -{len(removed)}')
                for v in sorted(added)[:5]:
                    con.print(f'      [green]+{v}[/green]')
                for v in sorted(removed)[:5]:
                    con.print(f'      [red]-{v}[/red]')
            except Exception as e:
                log_event(f'CV Update Failed: {table_name}', str(e)[:100])
        else:
            con.print(f'    [dim]{table_name}: {len(api_values)} values (no changes)[/dim]')

        # If this is a filterable status field, use it
        if is_status and filter_resource:
            db_statuses[filter_resource] = api_values
    logger.flush()

    # Add spec-based statuses for resources without CV endpoints
    for resource, (ref_type, statuses) in SPEC_STATUSES.items():
        if resource not in db_statuses:
            db_statuses[resource] = statuses
            log_event(f'Spec statuses: {resource}', f'{len(statuses)} from API field spec')

    # ══════════════════════════════════════════════════════════
    #  PHASE 1: Filter sweep
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase 1 -- Filter Sweep[/bold cyan]'); con.print()

    found = {
        'sfd': set(SEED_REFS['sfd']), 'consignment': set(SEED_REFS['consignment']),
        'ens_header': set(SEED_REFS['ens_header']), 'sup_dec': set(),
        'goods': set(), 'ffd': set(), 'immi': set(), 'gvms': set(),
        'eori': set(SEED_REFS['eori']),
    }
    read_data = {t: {} for t in found}
    filter_results = {}
    resource_to_ref = {
        'simplified_frontier_declarations':'sfd', 'supplementary_declarations':'sup_dec',
        'headers':'ens_header', 'full_frontier_declarations':'ffd',
        'internal_market_movements':'immi', 'gvms':'gvms'}

    for resource, statuses in db_statuses.items():
        ref_type = resource_to_ref.get(resource)
        if not ref_type: continue
        con.print(f'\n  [bold]{resource}[/bold] ({len(statuses)} statuses)')
        filter_results[resource] = {}
        for status in statuses:
            http, result, raw, ms = api._get(resource, {'filter':f'status={status}'},
                dt=f'{resource[:25].upper()}_FILTER', ref=status, notes=f'Filter {status}', silent=True)
            if http == 200:
                refs = extract_refs(result)
                new = [r for r in refs if r not in found[ref_type]]
                found[ref_type].update(refs)
                filter_results[resource][status] = len(refs)
                if refs:
                    con.print(f'    [green]{status:<40} {len(refs):>3} refs ({len(new)} new)  {ms}ms[/green]')
                    log_event(f'Filter hit: {resource}', f'{status} -> {len(refs)} refs')
            elif http == 400:
                filter_results[resource][status] = -1
            else:
                filter_results[resource][status] = -http
        total_found = len(found[ref_type]) - len(SEED_REFS.get(ref_type, []))
        if total_found > 0:
            log_event(f'Filter new: {resource}', f'{total_found} new refs from filters')
    logger.flush()

    # ══════════════════════════════════════════════════════════
    #  PHASE 2: Full reads + cross-reference chase
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase 2 -- Read + Chase[/bold cyan]'); con.print()

    def read_all(ref_type, resource, fields, label):
        refs = sorted(found[ref_type])
        if not refs: return
        con.print(f'\n  [bold]{label} ({len(refs)})[/bold]')
        for ref in refs:
            http, result, raw, ms = api._get(resource,
                {'reference':ref,'fields':fields}, dt=f'{label[:20].upper()}_READ', ref=ref, silent=True)
            if http == 200 and result:
                read_data[ref_type][ref] = result
                found['eori'].update(extract_eoris(result))
                cons = sget(result,'ens_consignment_reference')
                if cons: found['consignment'].add(cons)
                ens = sget(result,'declaration_number')
                if ens and ens.startswith('ENS'): found['ens_header'].add(ens)
                con.print(f'    [cyan]{ref}[/cyan]  [{sc(sget(result,"status"))}]{sget(result,"status")}[/{sc(sget(result,"status"))}]  {ms}ms')
                log_event(f'Read OK: {label}', f'{ref} -> {sget(result,"status")}')
        logger.flush()

    read_all('sfd','simplified_frontier_declarations', SFD_FIELDS, 'SFDs')

    # SD lookups
    con.print(f'\n  [bold]SD Lookups[/bold]')
    for ref in sorted(found['sfd']):
        http, result, raw, ms = api._get('supplementary_declarations', {'sfd_number':ref},
            dt='SD_LOOKUP_SFD', ref=ref, silent=True)
        if http==200 and result:
            sup = sget(result,'sup_dec_number','')
            for s in sup.split(','):
                s = s.strip()
                if s:
                    found['sup_dec'].add(s)
                    log_event(f'SD Found: {s}', f'from SFD {ref}')
                    con.print(f'    {ref} -> [green]{s}[/green]')
    logger.flush()

    read_all('sup_dec','supplementary_declarations', SD_FIELDS, 'Sup Decs')
    read_all('consignment','consignments', CONS_FIELDS, 'Consignments')
    read_all('ens_header','headers', ENS_FIELDS, 'ENS Headers')

    # ══════════════════════════════════════════════════════════
    #  PHASE 3: Goods
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase 3 -- Goods[/bold cyan]'); con.print()
    parents = ([(  'sfd_number',r,'SFD') for r in sorted(found['sfd'])]
             + [('sup_dec_number',r,'SD') for r in sorted(found['sup_dec'])]
             + [('ens_number',r,'ENS') for r in sorted(found['consignment'])])
    for pn, pr, lb in parents:
        http, result, raw, ms = api._get('goods', {pn:pr}, dt=f'GOODS_LOOKUP_{lb}', ref=pr, silent=True)
        gids = extract_goods_ids(result) if http==200 else []
        new_g = [g for g in gids if g not in found['goods']]
        found['goods'].update(gids)
        if gids: con.print(f'  {lb} {pr}: [green]{len(gids)} goods ({len(new_g)} new)[/green]')
    logger.flush()

    con.print(f'\n  Reading {len(found["goods"])} goods...')
    for gid in sorted(found['goods']):
        http, result, raw, ms = api._get('goods', {'reference':gid,'fields':GOODS_FIELDS},
            dt='GOODS_READ', ref=gid, silent=True)
        if http==200 and result:
            read_data['goods'][gid] = result
            con.print(f'    {gid[:24]}  {sget(result,"commodity_code","--------")[:10]}  '
                      f'{trunc(sget(result,"goods_description"),25)}  {ms}ms')
    logger.flush()

    # ══════════════════════════════════════════════════════════
    #  PHASE 4: Permissions
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase 4 -- Permissions[/bold cyan]'); con.print()
    permissions = {}
    for eori in sorted(found['eori']):
        http, result, raw, ms = api._get('permission_grant', {'importer_eori':eori},
            dt='PERMISSION', ref=eori)
        if http==200 and result:
            perms = result if isinstance(result, list) else []
            permissions[eori] = perms
            con.print(f'  [cyan]{eori}[/cyan]  {len(perms)} perm(s)  {ms}ms')
    logger.flush()

    # ══════════════════════════════════════════════════════════
    #  PHASE 5: Output
    # ══════════════════════════════════════════════════════════
    elapsed = time.time() - t0; logger.flush()
    try: rows_written = query(f"SELECT COUNT(*) AS c FROM {S}.ApiLog")[0]['c'] - lc
    except: rows_written = logger.total_flushed

    log_event('Complete', f'{api.total_calls} calls, {rows_written} logged, {elapsed:.0f}s')

    json_out = {
        'metadata': {'product':__product__,'module':__module__,'version':__version__,
            'generated':datetime.now(timezone.utc).isoformat(),
            'client':CLIENT_CODE,'env':ENV_CODE,
            'api_calls':api.total_calls,'log_rows':rows_written,'elapsed':elapsed},
        'cv_changes': cv_changes,
        'db_statuses': db_statuses,
        'filter_results': filter_results,
        'discovery_counts': {t:len(v) for t,v in found.items()},
        'refs': {t:sorted(v) for t,v in found.items()},
        'data': {t:{r:d for r,d in recs.items()} for t,recs in read_data.items()},
        'permissions': permissions,
        'run_log': [{'time':ts,'action':a,'detail':d} for ts,a,d in run_log],
    }
    with open(JSON_FILE,'w',encoding='utf-8') as f:
        json.dump(json_out, f, indent=2, default=str, ensure_ascii=False)

    con.print(); con.rule('[bold yellow]Spider v2 Complete[/bold yellow]'); con.print()
    con.print(f'  JSON:   [green]{JSON_FILE}[/green]')
    con.print(f'  ApiLog: [green]{rows_written} rows[/green]')

    tbl = Table(box=box.ROUNDED, title=f'[bold]{CLIENT_NAME} -- Spider v{__version__}[/bold]', border_style='green')
    tbl.add_column('Type', style='cyan', min_width=22)
    tbl.add_column('Statuses', justify='right')
    tbl.add_column('Found', justify='right', style='green')
    tbl.add_column('Read', justify='right', style='green')
    for rt, res in [('ens_header','headers'),('consignment','consignments'),
                    ('sfd','simplified_frontier_declarations'),('sup_dec','supplementary_declarations'),
                    ('goods','goods'),('ffd','full_frontier_declarations'),
                    ('immi','internal_market_movements'),('gvms','gvms'),('eori','permission_grant')]:
        n = len(found.get(rt,set())); r = len(read_data.get(rt,{}))
        s = len(db_statuses.get(res,[])); st = '[bold green]' if n>0 else '[dim]'
        tbl.add_row(rt.replace('_',' ').title(), str(s) if s else '-',
                    f'{st}{n}[/]', f'{st}{r}[/]' if rt!='eori' else f'{len(permissions)}/{n}')
    tbl.add_row(''); tbl.add_row('[bold]CV Downloads[/bold]',f'{len(cv_downloads)}','','')
    tbl.add_row('[bold]CV Updated[/bold]',f'{sum(1 for c in cv_changes.values() if c.get("added") or c.get("removed"))}','','')
    tbl.add_row('[bold]API Calls[/bold]','',f'[bold]{api.total_calls}[/bold]','')
    tbl.add_row('Runtime','',f'{elapsed:.0f}s','')
    con.print(tbl)

    # Chain map
    con.print(f'\n  [bold]Chain Map:[/bold]')
    for ens in sorted(found['ens_header']):
        ed = read_data['ens_header'].get(ens,{})
        con.print(f'  [bold cyan]ENS {ens}[/bold cyan]  {sget(ed,"status")}  port={sget(ed,"arrival_port")}')
        for cons in sorted(found['consignment']):
            cd = read_data['consignment'].get(cons,{})
            if sget(cd,'declaration_number')==ens:
                con.print(f'    [green]Cons {cons}[/green]  {sget(cd,"status")}  ctrl={sget(cd,"controlled_goods")}  {sget(cd,"total_packages")}pkg')
                for sfd in sorted(found['sfd']):
                    sd = read_data['sfd'].get(sfd,{})
                    if sget(sd,'ens_consignment_reference')==cons:
                        con.print(f'      [yellow]SFD {sfd}[/yellow]  {sget(sd,"status")}')
                for gid,gd in read_data['goods'].items():
                    if sget(gd,'consignment_number')==cons:
                        con.print(f'      Goods {gid[:20]}..  {sget(gd,"commodity_code")}  {trunc(sget(gd,"goods_description"),30)}')

    con.print(f'\n  [bold]Run Log ({len(run_log)} events):[/bold]')
    for ts, action, detail in run_log[-15:]:
        con.print(f'    [dim]{ts}  {action}  {detail}[/dim]')

    con.print(f'\n  [dim]{__product__} v{__version__} -- {__module__} -- {CLIENT_NAME} -- Synovia Digital Ltd[/dim]\n')


if __name__ == '__main__':
    main()
