"""
================================================================================
  Synovia Flow -- TSS Birkdale Submit Consignments
  Licensed Component: Synovia Digital Ltd
================================================================================

  Product:      Synovia Flow (Customs Declaration Management)
  Module:       TSS Birkdale Submit Consignments
  Version:      1.0.0
  Database:     Fusion_TSS
  Schema:       BKD (Birkdale)
  API:          TSS Declaration API v2.9.4 (TEST)

  Description:
  ------------
  Reads all Draft consignments from BKD.Consignments, submits each
  via POST op_type=submit, then polls for status changes and chases
  any auto-generated SFDs/SDs.

  Step 1:  Load Draft consignments from DB
  Step 2:  Submit each via POST
  Step 3:  Poll for status transitions (up to 60s per consignment)
  Step 4:  Chase SFD + SD generation
  Step 5:  Update DB with new statuses + new refs

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

__version__ = '1.0.0'
__product__ = 'Synovia Flow'
__module__  = 'TSS Birkdale Submit Consignments'

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

RATE_LIMIT  = 0.25
API_TIMEOUT = 30
LOG_BATCH   = 10
POLL_INTERVAL = 5   # seconds between polls
POLL_MAX_WAIT = 60  # max seconds to poll per consignment

TIMESTAMP   = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_DIR  = r'D:\TSS_Madrid\Birkdale'
JSON_FILE   = os.path.join(OUTPUT_DIR, f'brk_submit_{TIMESTAMP}.json')


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

def exec_sql(sql, params=None):
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
    def log(self, dt, ref, ep, params, http, raw, ms, method='GET', notes=''):
        self.buffer.append((
            (dt or '')[:50], 'WRITE' if method=='POST' else 'READ',
            (ref or '')[:200], None, CLIENT_CODE, method,
            (ep or '')[:500], json.dumps(params, separators=(',',':'))[:4000],
            http, 'OK' if http in (200,201) else 'FAIL',
            (raw or '')[:500], (raw or '')[:4000], ms,
            '' if http in (200,201) else (raw or '')[:4000],
            (notes or f'Submit v{__version__}')[:200]))
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
        self.base_url = base_url.rstrip('/') + '/x_fhmrc_tss_api/v1/tss_api'
        self.logger = logger; self.session = requests.Session()
        b64 = base64.b64encode(f'{username}:{password}'.encode()).decode()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Basic {b64}'})
        self.total_calls = 0

    def post(self, ep, payload, dt='', ref='', notes=''):
        url = f'{self.base_url}/{ep}'
        t0 = time.time()
        try:
            r = self.session.post(url, json=payload, timeout=API_TIMEOUT)
            self.total_calls += 1; ms = int((time.time()-t0)*1000)
            time.sleep(RATE_LIMIT)
            self.logger.log(dt, ref, ep, payload, r.status_code, r.text[:4000], ms,
                           method='POST', notes=notes)
            if r.status_code == 200:
                return 200, r.json().get('result'), r.text, ms
            return r.status_code, None, r.text[:500], ms
        except Exception as e:
            self.total_calls += 1; ms = int((time.time()-t0)*1000)
            self.logger.log(dt, ref, ep, payload, 0, str(e)[:500], ms,
                           method='POST', notes=notes)
            return 0, None, str(e)[:500], ms

    def get(self, ep, params, dt='', ref='', notes=''):
        url = f'{self.base_url}/{ep}'
        t0 = time.time()
        try:
            r = self.session.get(url, params=params, timeout=API_TIMEOUT)
            self.total_calls += 1; ms = int((time.time()-t0)*1000)
            time.sleep(RATE_LIMIT)
            self.logger.log(dt, ref, ep, params, r.status_code, r.text[:4000], ms, notes=notes)
            if r.status_code == 200:
                return 200, r.json().get('result'), r.text, ms
            return r.status_code, None, r.text[:500], ms
        except Exception as e:
            self.total_calls += 1; ms = int((time.time()-t0)*1000)
            self.logger.log(dt, ref, ep, params, 0, str(e)[:500], ms, notes=notes)
            return 0, None, str(e)[:500], ms


def sget(d, k, default=''):
    return d.get(k, default) if isinstance(d, dict) else default

def sc(st):
    s = (st or '').lower()
    if any(k in s for k in ('authorised','arrived','closed')): return 'green'
    if any(k in s for k in ('submitted','processing','draft')): return 'yellow'
    if any(k in s for k in ('required','amendment')): return 'bright_yellow'
    if any(k in s for k in ('error','failed','cancelled')): return 'red'
    return 'white'


def main():
    t0 = time.time()
    results = []  # [(ref, submit_status, final_status, sfd_ref, sd_ref, errors)]

    con.print(Panel.fit(
        f'[bold yellow]{__product__}[/bold yellow]  |  '
        f'[bold white]{__module__}[/bold white]  v{__version__}\n'
        f'[bold cyan]{CLIENT_NAME}[/bold cyan]  |  '
        f'[dim]{CLIENT_CODE}  |  {ENV_CODE}  |  {DB_NAME}  |  '
        f'{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC[/dim]',
        border_style='blue', padding=(0, 2)))

    creds = load_credentials()
    con.print(f'\n  API: {creds["tss_username"]}  Base: {creds["base_url"]}')
    logger = ApiLogger()
    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'], logger)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Step 1: Load Draft consignments ───────────────────────
    con.print(); con.rule('[bold cyan]Step 1 -- Load Draft Consignments[/bold cyan]'); con.print()

    drafts = query(f"""
        SELECT consignment_number, declaration_number, tss_status
        FROM {S}.Consignments
        WHERE tss_status = 'Draft'
        ORDER BY id
    """)
    con.print(f'  Found [bold]{len(drafts)}[/bold] Draft consignments to submit')
    for d in drafts:
        con.print(f'    {d["consignment_number"]}  ENS={d["declaration_number"]}')

    if not drafts:
        con.print(f'  [yellow]Nothing to submit[/yellow]')
        return

    # ── Step 2: Submit each ───────────────────────────────────
    con.print(); con.rule('[bold green]Step 2 -- Submit[/bold green]'); con.print()

    for d in drafts:
        ref = d['consignment_number']
        con.print(f'\n  [bold yellow]━━ {ref} ━━[/bold yellow]')

        payload = {'op_type': 'submit', 'consignment_number': ref}
        http, result, raw, ms = api.post('consignments', payload,
            dt='CONS_SUBMIT', ref=ref, notes=f'Submit {ref}')

        res = {'ref': ref, 'ens': d['declaration_number'],
               'submit_http': http, 'submit_status': '', 'submit_msg': '',
               'final_status': '', 'mrn': '',
               'sfd_ref': '', 'sd_ref': '', 'errors': []}

        if http == 200 and result:
            sub_status = sget(result, 'status', sget(result, 'process_message', ''))
            res['submit_status'] = sub_status
            res['submit_msg'] = sget(result, 'process_message', '')
            con.print(f'  [green]Submitted: {sub_status}  {sget(result,"process_message","")}  {ms}ms[/green]')

            # Update DB
            exec_sql(f"UPDATE {S}.Consignments SET tss_status=?, downloaded_at=SYSUTCDATETIME() WHERE consignment_number=?",
                     [sub_status if sub_status != 'SUCCESS' else 'Submitted', ref])
        else:
            error_msg = ''
            try:
                body = json.loads(raw) if raw else {}
                error_msg = (body.get('result',{}).get('process_message','')
                            or body.get('error',{}).get('message','')
                            or raw[:200])
            except: error_msg = (raw or '')[:200]
            res['errors'].append(f'Submit HTTP {http}: {error_msg}')
            con.print(f'  [red]FAILED: HTTP {http}  {error_msg[:80]}  {ms}ms[/red]')

        results.append(res)
        logger.flush()

    # ── Step 3: Poll for status changes ───────────────────────
    con.print(); con.rule('[bold green]Step 3 -- Poll Status[/bold green]'); con.print()

    for res in results:
        if res['errors']: continue  # skip failed submissions
        ref = res['ref']
        con.print(f'\n  [bold]Polling {ref}...[/bold]')
        poll_start = time.time()
        last_status = res['submit_status']

        while (time.time() - poll_start) < POLL_MAX_WAIT:
            time.sleep(POLL_INTERVAL)
            http, result, raw, ms = api.get('consignments',
                {'reference': ref, 'fields': 'status,movement_reference_number,error_message'},
                dt='CONS_POLL', ref=ref, notes=f'Poll {ref}')
            if http == 200 and result:
                new_status = sget(result, 'status')
                mrn = sget(result, 'movement_reference_number')
                err = sget(result, 'error_message')
                elapsed_poll = int(time.time() - poll_start)

                if new_status.lower() != last_status.lower():
                    con.print(f'    [{sc(new_status)}]{last_status} -> {new_status}[/]  '
                              f'{elapsed_poll}s  MRN={mrn}')
                    last_status = new_status
                    exec_sql(f"UPDATE {S}.Consignments SET tss_status=?, downloaded_at=SYSUTCDATETIME() WHERE consignment_number=?",
                             [new_status, ref])
                else:
                    con.print(f'    [dim]{new_status}  {elapsed_poll}s[/dim]')

                if err:
                    con.print(f'    [red]Error: {err}[/red]')
                    res['errors'].append(err)

                # Stop polling if reached a terminal state
                terminal = ['arrived','cancelled','authorised for movement',
                           'trader input required','amendment required']
                if new_status.lower() in terminal:
                    con.print(f'    [green]Reached terminal: {new_status}[/green]')
                    break

                if mrn:
                    res['mrn'] = mrn

        res['final_status'] = last_status

    # ── Step 4: Chase SFDs + SDs ──────────────────────────────
    con.print(); con.rule('[bold green]Step 4 -- Chase SFDs + SDs[/bold green]'); con.print()

    for res in results:
        if res['errors'] and not res['final_status']: continue
        ref = res['ref']

        # SFD lookup
        http, result, raw, ms = api.get('simplified_frontier_declarations',
            {'consignment_number': ref}, dt='SFD_CHASE', ref=ref, notes=f'SFD from {ref}')
        if http == 200 and result:
            sfd_num = sget(result, 'sfd_number', '')
            if sfd_num:
                res['sfd_ref'] = sfd_num
                con.print(f'  {ref} -> [green]SFD {sfd_num}[/green]')

                # Read SFD status
                http2, result2, raw2, ms2 = api.get('simplified_frontier_declarations',
                    {'reference': sfd_num, 'fields': 'status,controlled_goods,eori_for_eidr'},
                    dt='SFD_READ', ref=sfd_num)
                if http2 == 200 and result2:
                    con.print(f'    Status: [{sc(sget(result2,"status"))}]{sget(result2,"status")}[/]  '
                              f'ctrl={sget(result2,"controlled_goods")}')

                # Insert SFD to DB
                try:
                    exec_sql(f"""
                        IF NOT EXISTS (SELECT 1 FROM {S}.Sfds WHERE sfd_number=?)
                        INSERT INTO {S}.Sfds (sfd_number, ens_consignment_reference, tss_status, raw_json)
                        VALUES (?,?,?,?)""",
                        [sfd_num, sfd_num, ref,
                         sget(result2,'status','') if http2==200 and result2 else '',
                         json.dumps(result2 or result, default=str)])
                except: pass

                # SD lookup
                http3, result3, raw3, ms3 = api.get('supplementary_declarations',
                    {'sfd_number': sfd_num}, dt='SD_CHASE', ref=sfd_num, notes=f'SD from {sfd_num}')
                if http3 == 200 and result3:
                    sd_num = sget(result3, 'sup_dec_number', '')
                    if sd_num:
                        res['sd_ref'] = sd_num
                        con.print(f'    -> [green]SD {sd_num}[/green]')
                    else:
                        con.print(f'    [dim]No SD yet[/dim]')
            else:
                con.print(f'  {ref} -> [dim]No SFD yet[/dim]')
        logger.flush()

    # ── Output ────────────────────────────────────────────────
    elapsed = time.time() - t0
    logger.flush()

    json_out = {
        'metadata': {
            'product': __product__, 'module': __module__, 'version': __version__,
            'generated': datetime.now(timezone.utc).isoformat(),
            'client': CLIENT_CODE, 'env': ENV_CODE,
            'api_calls': api.total_calls, 'elapsed': elapsed,
        },
        'results': results,
    }
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(json_out, f, indent=2, default=str)

    # ── Summary ───────────────────────────────────────────────
    con.print(); con.rule('[bold yellow]Submit Complete[/bold yellow]'); con.print()

    tbl = Table(box=box.ROUNDED,
        title=f'[bold]{CLIENT_NAME} -- Submit Consignments v{__version__}[/bold]',
        border_style='green')
    tbl.add_column('Consignment', style='cyan', min_width=22)
    tbl.add_column('ENS', min_width=22)
    tbl.add_column('Submit', justify='center')
    tbl.add_column('Final Status', min_width=20)
    tbl.add_column('MRN', min_width=20)
    tbl.add_column('SFD', min_width=22)
    tbl.add_column('SD', min_width=22)
    tbl.add_column('Errors', justify='center')

    for r in results:
        sub = '[green]OK[/]' if r['submit_http'] == 200 else f'[red]HTTP {r["submit_http"]}[/]'
        fs = r['final_status'] or r['submit_status']
        tbl.add_row(
            r['ref'], r['ens'], sub,
            f'[{sc(fs)}]{fs}[/]',
            r['mrn'] or '-',
            r['sfd_ref'] or '-',
            r['sd_ref'] or '-',
            str(len(r['errors'])) if r['errors'] else '[green]0[/]')

    tbl.add_row('')
    tbl.add_row('[bold]API Calls[/bold]', '', '', '', f'[bold]{api.total_calls}[/bold]', '', '', '')
    tbl.add_row('Runtime', '', '', '', f'{elapsed:.0f}s', '', '', '')
    con.print(tbl)

    con.print(f'\n  JSON: [green]{JSON_FILE}[/green]')
    con.print(f'\n  [dim]{__product__} v{__version__} -- {__module__} -- '
              f'{CLIENT_NAME} -- Synovia Digital Ltd[/dim]\n')


if __name__ == '__main__':
    main()
