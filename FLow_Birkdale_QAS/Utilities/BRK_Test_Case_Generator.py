"""
================================================================================
  Synovia Flow -- TSS Birkdale Test Case Generator
  Licensed Component: Synovia Digital Ltd
================================================================================

  Product:      Synovia Flow (Customs Declaration Management)
  Module:       TSS Birkdale Test Case Generator
  Version:      1.0.0
  Database:     Fusion_TSS_PRD (read PLE) + Fusion_TSS (read BKD seeds)
  Schema:       PLE (source), BKD (target)
  API:          TSS Declaration API v2.9.4 (TEST)

  Description:
  ------------
  Phase A:  Inventory — scan every PLE table in both Fusion_TSS_PRD
            and Fusion_TSS, report row counts and sample data.
  Phase B:  Extract patterns from PLE real data — SFDs, Sup Decs,
            goods items, ENS headers, consignments.
  Phase C:  Generate test declaration payloads for BKD TEST covering:
            1. ENS + Consignment + Goods (controlled=yes) -> SFD -> SD
            2. ENS + Consignment + Goods (controlled=no, EIDR path)
            3. ENS + Consignment + Goods (IMMI internal_market=confirmed)
            4. Standalone FFD + Goods
  Phase D:  POST each test case to TSS TEST API, log to BKD.ApiLog
  Phase E:  Read back created refs, verify chain generation
  Phase F:  Output JSON summary of all created test data

  READ-ONLY against PLE/PRD.  WRITE against TSS TEST API only.

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

__version__ = '1.0.0'
__product__ = 'Synovia Flow'
__module__  = 'TSS Birkdale Test Case Generator'

import base64, configparser, json, os, sys, time
from datetime import datetime, timezone, timedelta
import pyodbc, requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

con = Console(highlight=False, width=140)

CLIENT_CODE = 'BKD'
CLIENT_NAME = 'Birkdale'
ENV_CODE    = 'TST'
INI_PATH    = r'D:\confguration\fusion_TSS.ini'
S           = CLIENT_CODE

RATE_LIMIT  = 0.25
API_TIMEOUT = 30
LOG_BATCH   = 10

TIMESTAMP   = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_DIR  = r'D:\TSS_Madrid\Birkdale'
JSON_FILE   = os.path.join(OUTPUT_DIR, f'brk_testcases_{TIMESTAMP}.json')

# BKD's known EORI (from spider)
BKD_EORI = 'XI000012340005'


# ── Database ──────────────────────────────────────────────────
def make_conn(db_name='Fusion_TSS'):
    cfg = configparser.ConfigParser(); cfg.read(INI_PATH)
    d = cfg['database']
    return pyodbc.connect(
        f"DRIVER={d['driver']};SERVER={d['server']};DATABASE={db_name};"
        f"UID={d['user']};PWD={d['password']};"
        f"Encrypt={d.get('encrypt','yes')};"
        f"TrustServerCertificate={d.get('trust_server_certificate','no')};",
        autocommit=False)

def query(sql, params=None, db='Fusion_TSS'):
    conn = make_conn(db); cur = conn.cursor()
    cur.execute(sql, params or [])
    cols = [c[0] for c in cur.description] if cur.description else []
    rows = cur.fetchall(); conn.close()
    return [dict(zip(cols, r)) for r in rows]

def load_credentials():
    rows = query("""
        SELECT e.base_url, cr.tss_username, cr.tss_password
        FROM CFG.Credentials cr JOIN CFG.Environments e ON e.env_code=cr.env_code
        WHERE cr.client_code=? AND cr.env_code=? AND cr.active=1
    """, [CLIENT_CODE, ENV_CODE])
    if not rows:
        con.print(f'[red]No active {ENV_CODE} creds for {CLIENT_CODE}[/red]'); sys.exit(1)
    return rows[0]


# ── ApiLogger ─────────────────────────────────────────────────
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
            (notes or f'TestGen v{__version__}')[:200]))
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


# ── API Client ────────────────────────────────────────────────
class TssApi:
    def __init__(self, base_url, username, password, logger):
        self.base_url = base_url.rstrip('/') + '/x_fhmrc_tss_api/v1/tss_api'
        self.logger = logger; self.session = requests.Session()
        b64 = base64.b64encode(f'{username}:{password}'.encode()).decode()
        self.session.headers.update({
            'Accept':'application/json',
            'Content-Type':'application/json',
            'Authorization':f'Basic {b64}'})
        self.total_calls = 0

    def _post(self, ep, payload, dt='', ref='', notes=''):
        url = f'{self.base_url}/{ep}'
        con.print(f'    [bold]POST /{ep}[/bold]  {json.dumps(payload, separators=(",",":"))[:80]}...')
        t0 = time.time()
        try:
            r = self.session.post(url, json=payload, timeout=API_TIMEOUT)
            self.total_calls += 1; ms = int((time.time()-t0)*1000)
            time.sleep(RATE_LIMIT)
            self.logger.log(dt, ref, ep, payload, r.status_code, r.text[:4000], ms, method='POST', notes=notes)
            if r.status_code == 200:
                return 200, r.json().get('result'), r.text, ms
            return r.status_code, None, r.text[:500], ms
        except Exception as e:
            self.total_calls += 1; ms = int((time.time()-t0)*1000)
            self.logger.log(dt, ref, ep, payload, 0, str(e)[:500], ms, method='POST', notes=notes)
            return 0, None, str(e)[:500], ms

    def _get(self, ep, params, dt='', ref='', notes=''):
        url = f'{self.base_url}/{ep}'
        dp = {k:v for k,v in params.items() if k!='fields'}
        ps = '&'.join(f'{k}={v}' for k,v in dp.items())
        con.print(f'    [dim]GET /{ep}?{ps}[/dim]')
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


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
def main():
    t0 = time.time()

    con.print(Panel.fit(
        f'[bold yellow]{__product__}[/bold yellow]  |  '
        f'[bold white]{__module__}[/bold white]  v{__version__}\n'
        f'[bold cyan]{CLIENT_NAME}[/bold cyan]  |  '
        f'[dim]{CLIENT_CODE}  |  {ENV_CODE}  |  '
        f'{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC[/dim]\n'
        f'[dim]Reads PLE patterns, creates BKD test declarations[/dim]',
        border_style='blue', padding=(0, 2)))

    # ══════════════════════════════════════════════════════════
    #  PHASE A: Inventory every PLE table in both databases
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase A -- PLE Table Inventory[/bold cyan]'); con.print()

    inventory = {}
    for db_name in ['Fusion_TSS_PRD', 'Fusion_TSS']:
        con.print(f'  [bold]{db_name}[/bold]')
        try:
            tables = query("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'PLE' AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """, db=db_name)
        except Exception as e:
            con.print(f'  [red]{db_name}: {e}[/red]')
            continue

        for t in tables:
            tn = f"PLE.{t['TABLE_NAME']}"
            try:
                cnt = query(f"SELECT COUNT(*) AS c FROM {tn}", db=db_name)[0]['c']
                cols = query(f"""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA='PLE' AND TABLE_NAME=?
                    ORDER BY ORDINAL_POSITION
                """, [t['TABLE_NAME']], db=db_name)
                col_names = [c['COLUMN_NAME'] for c in cols]
                con.print(f'    [cyan]{tn}[/cyan]  {cnt:>6} rows  '
                          f'[dim]({len(col_names)} cols)[/dim]')
                inventory[f'{db_name}.{tn}'] = {
                    'rows': cnt, 'columns': col_names}
            except Exception as e:
                con.print(f'    [dim]{tn}: {e}[/dim]')

    # ══════════════════════════════════════════════════════════
    #  PHASE B: Extract sample data patterns from PLE
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase B -- PLE Data Patterns[/bold cyan]'); con.print()

    patterns = {}

    # Try PRD first, then Fusion_TSS
    for db_name in ['Fusion_TSS_PRD', 'Fusion_TSS']:
        # SFDs
        try:
            sfds = query("""
                SELECT TOP 5 sfd_number, ens_consignment_reference, tss_status,
                    goods_description, transport_document_number,
                    controlled_goods, goods_domestic_status, destination_country,
                    importer_eori, consignor_eori, consignee_eori, exporter_eori,
                    total_packages, gross_mass_kg, movement_reference_number,
                    eori_for_eidr, ducr, declaration_choice
                FROM PLE.Sfds ORDER BY id DESC
            """, db=db_name)
            if sfds:
                patterns['sfds'] = sfds
                con.print(f'  [green]SFDs ({db_name}): {len(sfds)} samples[/green]')
                for s in sfds[:3]:
                    con.print(f'    {s["sfd_number"]}  {s["tss_status"]}  '
                              f'ctrl={s.get("controlled_goods","")}  '
                              f'{s.get("goods_description","")[:30]}')
        except: pass

        # Sup Decs
        try:
            sds = query("""
                SELECT TOP 5 sup_dec_number, tss_status,
                    goods_description, declaration_choice,
                    importer_eori, importer_name,
                    exporter_eori, exporter_name,
                    arrival_date_time, port_of_arrival,
                    movement_reference_number, total_packages,
                    controlled_goods, duty_amount, vat_amount, duty_total
                FROM PLE.SupplementaryDeclarations ORDER BY id DESC
            """, db=db_name)
            if sds:
                patterns['sup_decs'] = sds
                con.print(f'  [green]Sup Decs ({db_name}): {len(sds)} samples[/green]')
                for s in sds[:3]:
                    con.print(f'    {s["sup_dec_number"]}  {s["tss_status"]}  '
                              f'duty={s.get("duty_total","")}  '
                              f'{s.get("goods_description","")[:30]}')
        except: pass

        # SFD Goods
        try:
            sg = query("""
                SELECT TOP 5 goods_id, sfd_number, goods_description,
                    commodity_code, type_of_packages, number_of_packages,
                    gross_mass_kg, net_mass_kg, country_of_origin,
                    item_invoice_amount, item_invoice_currency,
                    procedure_code, controlled_goods
                FROM PLE.SfdGoodsItems ORDER BY id DESC
            """, db=db_name)
            if sg:
                patterns['sfd_goods'] = sg
                con.print(f'  [green]SFD Goods ({db_name}): {len(sg)} samples[/green]')
                for g in sg[:3]:
                    con.print(f'    {g.get("commodity_code","")}  '
                              f'{str(g.get("goods_description",""))[:30]}  '
                              f'{g.get("gross_mass_kg","")} kg')
        except: pass

        # SD Goods
        try:
            sdg = query("""
                SELECT TOP 5 goods_id, sup_dec_number, goods_description,
                    commodity_code, type_of_packages, number_of_packages,
                    gross_mass_kg, net_mass_kg, country_of_origin,
                    item_invoice_amount, item_invoice_currency,
                    procedure_code, customs_value, controlled_goods
                FROM PLE.SupDecGoodsItems ORDER BY id DESC
            """, db=db_name)
            if sdg:
                patterns['sd_goods'] = sdg
                con.print(f'  [green]SD Goods ({db_name}): {len(sdg)} samples[/green]')
        except: pass

        # ENS Headers
        try:
            ens = query("""
                SELECT TOP 5 declaration_number, tss_status,
                    movement_type, arrival_port, route,
                    carrier_eori, carrier_name
                FROM PLE.EnsHeaders ORDER BY id DESC
            """, db=db_name)
            if ens:
                patterns['ens_headers'] = ens
                con.print(f'  [green]ENS Headers ({db_name}): {len(ens)} samples[/green]')
                for e in ens[:3]:
                    con.print(f'    {e["declaration_number"]}  {e["tss_status"]}  '
                              f'port={e.get("arrival_port","")}  '
                              f'route={e.get("route","")}')
        except: pass

        # ENS Consignments
        try:
            ec = query("""
                SELECT TOP 5 declaration_number, ens_declaration_number,
                    tss_status, consignor_name, importer_eori,
                    total_packages, gross_mass_kg
                FROM PLE.EnsConsignments ORDER BY id DESC
            """, db=db_name)
            if ec:
                patterns['ens_consignments'] = ec
                con.print(f'  [green]ENS Consignments ({db_name}): {len(ec)} samples[/green]')
        except: pass

    if not patterns:
        con.print(f'  [red]No PLE data found in either database[/red]')

    # ══════════════════════════════════════════════════════════
    #  PHASE C: Generate test declaration payloads for BKD TEST
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase C -- Generate Test Payloads[/bold cyan]'); con.print()

    # Arrival time: 2 minutes from now (GMT) for non-controlled,
    # or in the past for "arrived" testing
    future_arrival = (datetime.now(timezone.utc) + timedelta(minutes=5)).strftime('%d/%m/%Y %H:%M:%S')
    past_arrival = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%d/%m/%Y %H:%M:%S')

    test_cases = []

    # ── Test Case 1: Controlled goods (triggers SFD + SD chain) ──
    tc1 = {
        'name': 'TC1_Controlled_Goods_SFD_SD',
        'description': 'ENS + Consignment with controlled_goods=yes -> SFD submitted to CDS -> Arrival -> Draft SD generated',
        'header_payload': {
            'op_type': 'create',
            'movement_type': '1a',
            'arrival_port': 'GBAUBELBELBEL',
            'arrival_date_time': past_arrival,
            'route': 'gb-ni',
            'identity_no_of_transport': 'IMO9999999#BKD-TC1',
            'nationality_of_transport': 'GB',
            'place_of_loading': 'Birkenhead',
            'place_of_unloading': 'Belfast',
            'seal_number': 'SEAL-TC1',
            'carrier_eori': BKD_EORI,
            'carrier_name': 'Birkdale Test Carrier',
            'carrier_street_number': '1 Test Street',
            'carrier_city': 'London',
            'carrier_postcode': 'EC1A 1BB',
            'carrier_country': 'GB',
            'transport_charges': 'Y',
        },
        'consignment_payload': {
            'op_type': 'create',
            'declaration_number': '{ENS_REF}',
            'controlled_goods': 'yes',
            'goods_domestic_status': '',
            'destination_country': 'GB',
            'goods_description': 'Electronic Components (Controlled Test)',
            'transport_document_number': 'BKD-TC1-TDOC',
            'trader_reference': 'BKD-TC1-REF',
            'consignor_eori': BKD_EORI,
            'consignor_name': 'BKD Test Consignor Ltd',
            'consignor_street_number': '10 Export Road',
            'consignor_city': 'Manchester',
            'consignor_postcode': 'M1 1AA',
            'consignor_country': 'GB',
            'consignee_eori': BKD_EORI,
            'consignee_name': 'BKD Test Consignee NI',
            'consignee_street_number': '20 Import Lane',
            'consignee_city': 'Belfast',
            'consignee_postcode': 'BT1 1AA',
            'consignee_country': 'GB',
            'importer_eori': BKD_EORI,
            'exporter_eori': BKD_EORI,
            'exporter_name': 'BKD Test Exporter',
            'exporter_street_number': '10 Export Road',
            'exporter_city': 'Manchester',
            'exporter_postcode': 'M1 1AA',
            'exporter_country': 'GB',
            'total_packages': '10',
            'gross_mass_kg': '250.00',
        },
        'goods_payloads': [{
            'op_type': 'create',
            'consignment_number': '{CONS_REF}',
            'commodity_code': '8542310000',
            'goods_description': 'Integrated circuits, electronic (controlled)',
            'type_of_packages': 'CT',
            'number_of_packages': '10',
            'gross_mass_kg': '250.00',
            'net_mass_kg': '200.00',
            'country_of_origin': 'CN',
            'item_invoice_amount': '5000.00',
            'item_invoice_currency': 'GBP',
            'number_of_individual_pieces': '',
        }],
    }
    test_cases.append(tc1)

    # ── Test Case 2: Uncontrolled goods (EIDR path, no SD) ──
    tc2 = {
        'name': 'TC2_Uncontrolled_EIDR',
        'description': 'ENS + Consignment with controlled_goods=no -> EIDR path, no SD generated',
        'header_payload': {
            'op_type': 'create',
            'movement_type': '1a',
            'arrival_port': 'GBAUBELBELBEL',
            'arrival_date_time': past_arrival,
            'route': 'gb-ni',
            'identity_no_of_transport': 'IMO9999999#BKD-TC2',
            'nationality_of_transport': 'GB',
            'place_of_loading': 'Holyhead',
            'place_of_unloading': 'Belfast',
            'seal_number': 'SEAL-TC2',
            'carrier_eori': BKD_EORI,
            'carrier_name': 'Birkdale Test Carrier',
            'carrier_street_number': '1 Test Street',
            'carrier_city': 'London',
            'carrier_postcode': 'EC1A 1BB',
            'carrier_country': 'GB',
            'transport_charges': 'Y',
        },
        'consignment_payload': {
            'op_type': 'create',
            'declaration_number': '{ENS_REF}',
            'controlled_goods': 'no',
            'goods_domestic_status': 'D',
            'destination_country': 'GB',
            'goods_description': 'Office Supplies (Uncontrolled Test)',
            'transport_document_number': 'BKD-TC2-TDOC',
            'trader_reference': 'BKD-TC2-REF',
            'consignor_eori': BKD_EORI,
            'consignor_name': 'BKD Test Consignor',
            'consignor_street_number': '10 Export Road',
            'consignor_city': 'Birmingham',
            'consignor_postcode': 'B1 1AA',
            'consignor_country': 'GB',
            'consignee_eori': BKD_EORI,
            'consignee_name': 'BKD Test Consignee NI',
            'consignee_street_number': '20 Import Lane',
            'consignee_city': 'Belfast',
            'consignee_postcode': 'BT1 1AA',
            'consignee_country': 'GB',
            'importer_eori': BKD_EORI,
            'exporter_eori': BKD_EORI,
            'exporter_name': 'BKD Test Exporter',
            'exporter_street_number': '10 Export Road',
            'exporter_city': 'Birmingham',
            'exporter_postcode': 'B1 1AA',
            'exporter_country': 'GB',
            'total_packages': '5',
            'gross_mass_kg': '50.00',
        },
        'goods_payloads': [{
            'op_type': 'create',
            'consignment_number': '{CONS_REF}',
            'commodity_code': '4820100000',
            'goods_description': 'Office paper notebooks',
            'type_of_packages': 'BX',
            'number_of_packages': '5',
            'gross_mass_kg': '50.00',
            'net_mass_kg': '45.00',
            'country_of_origin': 'GB',
            'item_invoice_amount': '150.00',
            'item_invoice_currency': 'GBP',
        }],
    }
    test_cases.append(tc2)

    # ── Test Case 3: Standalone FFD ──
    tc3 = {
        'name': 'TC3_Full_Frontier_Declaration',
        'description': 'Standalone FFD with goods items — separate from ENS',
        'ffd_payload': {
            'op_type': 'create',
            'arrival_date_time': past_arrival,
            'arrival_port': 'GBAUBELBELBEL',
            'movement_type': '3',
            'declaration_category': 'H1',
            'importer_eori': BKD_EORI,
            'importer_name': 'BKD Test Importer',
            'importer_street_number': '20 Import Lane',
            'importer_city': 'Belfast',
            'importer_postcode': 'BT1 1AA',
            'importer_country': 'GB',
            'exporter_eori': BKD_EORI,
            'exporter_name': 'BKD Test Exporter',
            'exporter_street_number': '10 Export Road',
            'exporter_city': 'London',
            'exporter_postcode': 'EC1A 1BB',
            'exporter_country': 'GB',
            'goods_description': 'Machinery parts (FFD Test)',
            'transport_document_number': 'BKD-TC3-FFD',
            'trader_reference': 'BKD-TC3-REF',
            'representation_type': '2',
            'total_packages': '3',
            'gross_mass_kg': '500.00',
            'destination_country': 'GB',
        },
        'goods_payloads': [{
            'op_type': 'create',
            'consignment_number': '{FFD_REF}',
            'commodity_code': '8431390000',
            'goods_description': 'Parts of machinery for lifting',
            'type_of_packages': 'PK',
            'number_of_packages': '3',
            'gross_mass_kg': '500.00',
            'net_mass_kg': '450.00',
            'country_of_origin': 'DE',
            'item_invoice_amount': '12000.00',
            'item_invoice_currency': 'GBP',
            'procedure_code': '4000',
            'country_of_preferential_origin': '',
        }],
    }
    test_cases.append(tc3)

    for tc in test_cases:
        con.print(f'  [cyan]{tc["name"]}[/cyan]  {tc["description"][:60]}')

    # ══════════════════════════════════════════════════════════
    #  PHASE D: POST test cases to TSS TEST API
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold green]Phase D -- Create Test Declarations[/bold green]'); con.print()

    creds = load_credentials()
    con.print(f'  API: {creds["tss_username"]}  Base: {creds["base_url"]}')
    logger = ApiLogger()
    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'], logger)

    results = []

    for tc in test_cases:
        con.print(f'\n  [bold yellow]━━ {tc["name"]} ━━[/bold yellow]')
        tc_result = {'name': tc['name'], 'refs': {}, 'errors': []}

        # ── ENS Header + Consignment + Goods path ──
        if 'header_payload' in tc:
            # Step 1: Create ENS Header
            con.print(f'  [bold]Step 1: Create ENS Header[/bold]')
            http, result, raw, ms = api._post('headers', tc['header_payload'],
                dt='ENS_CREATE', notes=f'{tc["name"]} header')
            if http == 200 and result:
                ens_ref = sget(result, 'reference', '')
                status = sget(result, 'status', '')
                con.print(f'  [green]Created: {ens_ref}  status={status}  {ms}ms[/green]')
                tc_result['refs']['ens_header'] = ens_ref

                # Step 2: Create Consignment
                con.print(f'  [bold]Step 2: Create Consignment[/bold]')
                cons_payload = {k: (v.replace('{ENS_REF}', ens_ref) if isinstance(v, str) else v)
                                for k, v in tc['consignment_payload'].items()}
                http2, result2, raw2, ms2 = api._post('consignments', cons_payload,
                    dt='CONS_CREATE', ref=ens_ref, notes=f'{tc["name"]} consignment')
                if http2 == 200 and result2:
                    cons_ref = sget(result2, 'reference', '')
                    con.print(f'  [green]Created: {cons_ref}  {ms2}ms[/green]')
                    tc_result['refs']['consignment'] = cons_ref

                    # Step 3: Create Goods
                    for gi, gp in enumerate(tc.get('goods_payloads', []), 1):
                        con.print(f'  [bold]Step 3.{gi}: Create Goods Item[/bold]')
                        goods_payload = {k: (v.replace('{CONS_REF}', cons_ref) if isinstance(v, str) else v)
                                         for k, v in gp.items()}
                        http3, result3, raw3, ms3 = api._post('goods', goods_payload,
                            dt='GOODS_CREATE', ref=cons_ref, notes=f'{tc["name"]} goods {gi}')
                        if http3 == 200 and result3:
                            gid = sget(result3, 'reference', '')
                            con.print(f'  [green]Created: {gid}  {ms3}ms[/green]')
                            tc_result['refs'].setdefault('goods', []).append(gid)
                        else:
                            err = f'Goods {gi}: HTTP {http3}'
                            con.print(f'  [red]{err}  {ms3}ms[/red]')
                            con.print(f'    [dim]{(raw3 or "")[:200]}[/dim]')
                            tc_result['errors'].append(err)

                    # Step 4: Submit Consignment
                    con.print(f'  [bold]Step 4: Submit Consignment[/bold]')
                    submit_payload = {
                        'op_type': 'submit',
                        'consignment_number': cons_ref,
                    }
                    http4, result4, raw4, ms4 = api._post('consignments', submit_payload,
                        dt='CONS_SUBMIT', ref=cons_ref, notes=f'{tc["name"]} submit')
                    if http4 == 200 and result4:
                        sub_status = sget(result4, 'status', '')
                        con.print(f'  [green]Submitted: {sub_status}  {ms4}ms[/green]')
                        tc_result['refs']['submit_status'] = sub_status
                    else:
                        err = f'Submit: HTTP {http4}'
                        con.print(f'  [red]{err}  {ms4}ms[/red]')
                        con.print(f'    [dim]{(raw4 or "")[:200]}[/dim]')
                        tc_result['errors'].append(err)
                else:
                    err = f'Consignment: HTTP {http2}'
                    con.print(f'  [red]{err}  {ms2}ms[/red]')
                    con.print(f'    [dim]{(raw2 or "")[:200]}[/dim]')
                    tc_result['errors'].append(err)
            else:
                err = f'Header: HTTP {http}'
                con.print(f'  [red]{err}  {ms}ms[/red]')
                con.print(f'    [dim]{(raw or "")[:200]}[/dim]')
                tc_result['errors'].append(err)

        # ── FFD path ──
        elif 'ffd_payload' in tc:
            con.print(f'  [bold]Step 1: Create FFD[/bold]')
            http, result, raw, ms = api._post('full_frontier_declarations', tc['ffd_payload'],
                dt='FFD_CREATE', notes=f'{tc["name"]} FFD')
            if http == 200 and result:
                ffd_ref = sget(result, 'reference', '')
                con.print(f'  [green]Created: {ffd_ref}  {ms}ms[/green]')
                tc_result['refs']['ffd'] = ffd_ref

                for gi, gp in enumerate(tc.get('goods_payloads', []), 1):
                    con.print(f'  [bold]Step 2.{gi}: Create FFD Goods[/bold]')
                    goods_payload = {k: (v.replace('{FFD_REF}', ffd_ref) if isinstance(v, str) else v)
                                     for k, v in gp.items()}
                    http2, result2, raw2, ms2 = api._post('goods', goods_payload,
                        dt='FFD_GOODS_CREATE', ref=ffd_ref, notes=f'{tc["name"]} goods {gi}')
                    if http2 == 200 and result2:
                        gid = sget(result2, 'reference', '')
                        con.print(f'  [green]Created: {gid}  {ms2}ms[/green]')
                        tc_result['refs'].setdefault('goods', []).append(gid)
                    else:
                        err = f'FFD Goods {gi}: HTTP {http2}'
                        con.print(f'  [red]{err}  {ms2}ms[/red]')
                        con.print(f'    [dim]{(raw2 or "")[:200]}[/dim]')
                        tc_result['errors'].append(err)
            else:
                err = f'FFD: HTTP {http}'
                con.print(f'  [red]{err}  {ms}ms[/red]')
                con.print(f'    [dim]{(raw or "")[:200]}[/dim]')
                tc_result['errors'].append(err)

        results.append(tc_result)
        logger.flush()

    # ══════════════════════════════════════════════════════════
    #  PHASE E: Read back and verify
    # ══════════════════════════════════════════════════════════
    con.print(); con.rule('[bold cyan]Phase E -- Verify Created Declarations[/bold cyan]'); con.print()

    for tc_result in results:
        name = tc_result['name']
        refs = tc_result['refs']
        con.print(f'  [bold]{name}[/bold]')

        if 'ens_header' in refs:
            http, result, raw, ms = api._get('headers',
                {'reference': refs['ens_header'], 'fields': 'status,movement_type,arrival_port'},
                dt='VERIFY_ENS', ref=refs['ens_header'])
            if http == 200 and result:
                con.print(f'    ENS {refs["ens_header"]}: status={sget(result,"status")}')

        if 'consignment' in refs:
            http, result, raw, ms = api._get('consignments',
                {'reference': refs['consignment'], 'fields': 'status,declaration_number,controlled_goods'},
                dt='VERIFY_CONS', ref=refs['consignment'])
            if http == 200 and result:
                con.print(f'    Cons {refs["consignment"]}: status={sget(result,"status")}  '
                          f'ctrl={sget(result,"controlled_goods")}')

            # Check if SFD was generated
            http, result, raw, ms = api._get('simplified_frontier_declarations',
                {'consignment_number': refs['consignment']},
                dt='VERIFY_SFD_LOOKUP', ref=refs['consignment'])
            if http == 200 and result:
                sfd_num = sget(result, 'sfd_number', '')
                if sfd_num:
                    con.print(f'    [green]SFD generated: {sfd_num}[/green]')
                    tc_result['refs']['sfd'] = sfd_num

                    # Check if SD was generated
                    http2, result2, raw2, ms2 = api._get('supplementary_declarations',
                        {'sfd_number': sfd_num}, dt='VERIFY_SD_LOOKUP', ref=sfd_num)
                    if http2 == 200 and result2:
                        sup = sget(result2, 'sup_dec_number', '')
                        if sup:
                            con.print(f'    [green]SD generated: {sup}[/green]')
                            tc_result['refs']['sup_dec'] = sup
                        else:
                            con.print(f'    [yellow]No SD yet (may need arrival)[/yellow]')
                else:
                    con.print(f'    [yellow]No SFD yet[/yellow]')

        if 'ffd' in refs:
            http, result, raw, ms = api._get('full_frontier_declarations',
                {'reference': refs['ffd'], 'fields': 'status,movement_type,importer_eori'},
                dt='VERIFY_FFD', ref=refs['ffd'])
            if http == 200 and result:
                con.print(f'    FFD {refs["ffd"]}: status={sget(result,"status")}')

        if tc_result['errors']:
            con.print(f'    [red]Errors: {len(tc_result["errors"])}[/red]')

    logger.flush()

    # ══════════════════════════════════════════════════════════
    #  PHASE F: Output
    # ══════════════════════════════════════════════════════════
    elapsed = time.time() - t0

    con.print(); con.rule('[bold cyan]Output[/bold cyan]'); con.print()

    json_out = {
        'metadata': {
            'product': __product__, 'module': __module__, 'version': __version__,
            'generated': datetime.now(timezone.utc).isoformat(),
            'client': CLIENT_CODE, 'env': ENV_CODE,
            'api_calls': api.total_calls, 'elapsed': elapsed,
        },
        'ple_inventory': inventory,
        'ple_patterns': {k: v for k, v in patterns.items()},
        'test_cases': [{
            'name': tc['name'],
            'description': tc['description'],
        } for tc in test_cases],
        'results': results,
    }
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(json_out, f, indent=2, default=str, ensure_ascii=False)
    con.print(f'  JSON: [green]{JSON_FILE}[/green]')

    # Summary
    con.print(); con.rule('[bold yellow]Test Case Generator Complete[/bold yellow]'); con.print()

    tbl = Table(box=box.ROUNDED,
        title=f'[bold]{CLIENT_NAME} -- Test Cases Created[/bold]',
        border_style='green')
    tbl.add_column('Test Case', style='cyan', min_width=35)
    tbl.add_column('ENS', justify='center')
    tbl.add_column('Cons', justify='center')
    tbl.add_column('SFD', justify='center')
    tbl.add_column('SD', justify='center')
    tbl.add_column('FFD', justify='center')
    tbl.add_column('Goods', justify='center')
    tbl.add_column('Errors', justify='center', style='red')

    for r in results:
        refs = r['refs']
        tbl.add_row(
            r['name'],
            refs.get('ens_header','')[:8] + '..' if refs.get('ens_header') else '-',
            refs.get('consignment','')[:8] + '..' if refs.get('consignment') else '-',
            refs.get('sfd','')[:8] + '..' if refs.get('sfd') else '-',
            refs.get('sup_dec','')[:8] + '..' if refs.get('sup_dec') else '-',
            refs.get('ffd','')[:8] + '..' if refs.get('ffd') else '-',
            str(len(refs.get('goods',[]))) if refs.get('goods') else '-',
            str(len(r['errors'])) if r['errors'] else '[green]0[/green]',
        )

    tbl.add_row('')
    tbl.add_row('[bold]Total API Calls[/bold]', '', '', '', '', '', f'[bold]{api.total_calls}[/bold]', '')
    tbl.add_row('Runtime', '', '', '', '', '', f'{elapsed:.0f}s', '')
    con.print(tbl)

    con.print()
    con.print(f'  [dim]{__product__} v{__version__} -- {__module__} -- '
              f'{CLIENT_NAME} -- Synovia Digital Ltd[/dim]')
    con.print()


if __name__ == '__main__':
    main()
