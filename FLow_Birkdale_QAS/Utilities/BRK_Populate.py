"""
================================================================================
  Synovia Flow -- TSS Birkdale Populate (API -> Database)
  Licensed Component: Synovia Digital Ltd
================================================================================

  Product:      Synovia Flow (Customs Declaration Management)
  Module:       TSS Birkdale Populate
  Version:      1.0.0
  Database:     Fusion_TSS
  Schema:       BKD (Birkdale)
  API:          TSS Declaration API v2.9.4 (TEST)

  Description:
  ------------
  Reads every declaration from the TSS API and persists to BKD schema
  tables. Creates tables if they don't exist (mirrors PLE structure).

  Step 1:  Ensure BKD schema tables exist (auto-create from PLE template)
  Step 2:  Read all ENS Headers
  Step 3:  Read all Consignments + extract SFD/ENS cross-refs
  Step 4:  SFD Lookup + Full Read + Goods
  Step 5:  SD Lookup + Full Read + Goods
  Step 6:  INSERT/UPDATE all into BKD tables
  Step 7:  Verification counts

  All API calls logged to BKD.ApiLog.

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

__version__ = '1.0.0'
__product__ = 'Synovia Flow'
__module__  = 'TSS Birkdale Populate'

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
JSON_FILE   = os.path.join(OUTPUT_DIR, f'brk_populate_{TIMESTAMP}.json')

# Known refs from all prior work
SEED_ENS = ['ENS000000000421555','ENS000000000421669','ENS000000000421670']
SEED_CONS = ['DEC000000001072377','DEC000000001072378',
             'DEC000000001072647','DEC000000001072648','DEC000000001072649']

# Full field sets
ENS_FIELDS = ('status,movement_type,identity_no_of_transport,'
    'nationality_of_transport,arrival_date_time,arrival_port,'
    'place_of_loading,place_of_unloading,seal_number,route,'
    'transport_charges,carrier_eori,carrier_name,'
    'carrier_street_number,carrier_city,carrier_postcode,carrier_country,'
    'haulier_eori,error_message')
CONS_FIELDS = ('status,declaration_number,goods_description,trader_reference,'
    'transport_document_number,controlled_goods,goods_domestic_status,'
    'destination_country,no_sfd_reason,'
    'consignor_eori,consignor_name,consignor_street_number,'
    'consignor_city,consignor_postcode,consignor_country,'
    'consignee_eori,consignee_name,consignee_street_number,'
    'consignee_city,consignee_postcode,consignee_country,'
    'importer_eori,importer_name,importer_street_number,'
    'importer_city,importer_postcode,importer_country,'
    'exporter_eori,total_packages,gross_mass_kg,'
    'movement_reference_number,control_status,error_message')
SFD_FIELDS = ('status,goods_description,trader_reference,transport_document_number,'
    'controlled_goods,goods_domestic_status,destination_country,'
    'consignor_eori,consignor_name,consignor_street_number,'
    'consignor_city,consignor_postcode,consignor_country,'
    'consignee_eori,consignee_name,consignee_street_number,'
    'consignee_city,consignee_postcode,consignee_country,'
    'importer_eori,importer_name,importer_street_number,'
    'importer_city,importer_postcode,importer_country,'
    'exporter_eori,exporter_name,exporter_street_number,'
    'exporter_city,exporter_postcode,exporter_country,'
    'importer_parent_organisation_eori,'
    'total_packages,gross_mass_kg,'
    'movement_reference_number,eori_for_eidr,'
    'ens_consignment_reference,error_code,error_message,'
    'ducr,supervising_customs_office,customs_warehouse_identifier,'
    'declaration_choice,use_importer_sde,align_ukims')
SD_FIELDS = ('status,movement_reference_number,error_code,error_message,'
    'trader_reference,duty_totals,duty_lines,'
    'importer_eori,importer_name,arrival_date_time,port_of_arrival,'
    'transport_document_number,submission_due_date,'
    'total_packages,clear_date_time,'
    'declaration_choice,representation_type,'
    'controlled_goods,goods_domestic_status,'
    'exporter_eori,exporter_name,'
    'movement_type,destination_country,goods_description')
GOODS_FIELDS = ('consignment_number,goods_description,commodity_code,'
    'type_of_packages,number_of_packages,gross_mass_kg,net_mass_kg,'
    'country_of_origin,item_invoice_amount,item_invoice_currency,'
    'procedure_code,additional_procedure_codes,'
    'controlled_goods,controlled_goods_type,package_marks,'
    'customs_value,statistical_value,valuation_indicator,valuation_method,'
    'preference,nature_of_transaction,invoice_number,'
    'document_references,additional_information,'
    'detail_previous_document,item_add_ded')


# ══════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════
#  TABLE CREATION (mirrors PLE structure)
# ══════════════════════════════════════════════════════════════
CREATE_TABLES = {
    'EnsHeaders': """
        CREATE TABLE {S}.EnsHeaders (
            id INT IDENTITY(1,1) PRIMARY KEY,
            declaration_number VARCHAR(40) NOT NULL,
            tss_status VARCHAR(50), movement_type VARCHAR(10),
            identity_no_transport VARCHAR(40), nationality_transport VARCHAR(5),
            arrival_date_time VARCHAR(30), arrival_date DATE,
            arrival_port VARCHAR(200),
            place_of_loading VARCHAR(40), place_of_unloading VARCHAR(40),
            seal_number VARCHAR(30), route VARCHAR(20),
            carrier_eori VARCHAR(200), carrier_name VARCHAR(40),
            error_message VARCHAR(500),
            raw_json NVARCHAR(MAX), downloaded_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT UQ_{S}_EnsHeaders UNIQUE(declaration_number))""",
    'Consignments': """
        CREATE TABLE {S}.Consignments (
            id INT IDENTITY(1,1) PRIMARY KEY,
            consignment_number VARCHAR(40) NOT NULL,
            declaration_number VARCHAR(40), tss_status VARCHAR(50),
            goods_description VARCHAR(300), transport_document_number VARCHAR(40),
            controlled_goods VARCHAR(10), goods_domestic_status VARCHAR(5),
            destination_country VARCHAR(5), no_sfd_reason VARCHAR(10),
            consignor_eori VARCHAR(200), consignor_name VARCHAR(100),
            consignor_street_number VARCHAR(100), consignor_city VARCHAR(50),
            consignor_postcode VARCHAR(12), consignor_country VARCHAR(5),
            consignee_eori VARCHAR(200), consignee_name VARCHAR(100),
            consignee_street_number VARCHAR(100), consignee_city VARCHAR(50),
            consignee_postcode VARCHAR(12), consignee_country VARCHAR(5),
            importer_eori VARCHAR(200), importer_name VARCHAR(100),
            importer_street_number VARCHAR(100), importer_city VARCHAR(50),
            importer_postcode VARCHAR(12), importer_country VARCHAR(5),
            exporter_eori VARCHAR(200),
            trader_reference VARCHAR(40), total_packages INT, gross_mass_kg DECIMAL(12,3),
            movement_reference_number VARCHAR(40), control_status VARCHAR(100),
            error_message VARCHAR(500),
            raw_json NVARCHAR(MAX), downloaded_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT UQ_{S}_Cons UNIQUE(consignment_number))""",
    'Sfds': """
        CREATE TABLE {S}.Sfds (
            id INT IDENTITY(1,1) PRIMARY KEY,
            sfd_number VARCHAR(40) NOT NULL,
            ens_consignment_reference VARCHAR(40), tss_status VARCHAR(50),
            goods_description VARCHAR(300), transport_document_number VARCHAR(40),
            controlled_goods VARCHAR(10), goods_domestic_status VARCHAR(5),
            destination_country VARCHAR(5), declaration_choice VARCHAR(10),
            consignor_eori VARCHAR(200), consignor_name VARCHAR(100),
            consignee_eori VARCHAR(200), consignee_name VARCHAR(100),
            importer_eori VARCHAR(200), importer_name VARCHAR(100),
            exporter_eori VARCHAR(200), exporter_name VARCHAR(100),
            trader_reference VARCHAR(40), total_packages INT, gross_mass_kg DECIMAL(12,3),
            movement_reference_number VARCHAR(40), eori_for_eidr VARCHAR(200),
            ducr VARCHAR(60), error_code VARCHAR(20), error_message VARCHAR(500),
            use_importer_sde VARCHAR(10), align_ukims VARCHAR(10),
            goods_item_count INT DEFAULT 0,
            raw_json NVARCHAR(MAX), downloaded_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT UQ_{S}_Sfds UNIQUE(sfd_number))""",
    'SfdGoodsItems': """
        CREATE TABLE {S}.SfdGoodsItems (
            id INT IDENTITY(1,1) PRIMARY KEY,
            goods_id VARCHAR(40) NOT NULL,
            sfd_number VARCHAR(40), consignment_number VARCHAR(40),
            goods_description VARCHAR(300), commodity_code VARCHAR(20),
            type_of_packages VARCHAR(20), number_of_packages INT,
            gross_mass_kg DECIMAL(12,3), net_mass_kg DECIMAL(12,3),
            country_of_origin VARCHAR(5),
            item_invoice_amount DECIMAL(14,2), item_invoice_currency VARCHAR(5),
            procedure_code VARCHAR(10), additional_procedure_codes VARCHAR(50),
            controlled_goods VARCHAR(10), controlled_goods_type VARCHAR(20),
            package_marks VARCHAR(500),
            customs_value DECIMAL(14,2), statistical_value DECIMAL(14,2),
            valuation_indicator VARCHAR(10), valuation_method VARCHAR(5),
            preference VARCHAR(10), nature_of_transaction VARCHAR(10),
            invoice_number VARCHAR(40),
            raw_json NVARCHAR(MAX), downloaded_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT UQ_{S}_SfdGoods UNIQUE(goods_id))""",
    'SupplementaryDeclarations': """
        CREATE TABLE {S}.SupplementaryDeclarations (
            id INT IDENTITY(1,1) PRIMARY KEY,
            sup_dec_number VARCHAR(40) NOT NULL,
            sfd_number VARCHAR(40), ens_consignment_reference VARCHAR(40),
            tss_status VARCHAR(50), declaration_choice VARCHAR(10),
            goods_description VARCHAR(300), transport_document_number VARCHAR(40),
            controlled_goods VARCHAR(10), goods_domestic_status VARCHAR(5),
            representation_type VARCHAR(5), movement_type VARCHAR(10),
            destination_country VARCHAR(5),
            importer_eori VARCHAR(200), importer_name VARCHAR(100),
            exporter_eori VARCHAR(200), exporter_name VARCHAR(100),
            trader_reference VARCHAR(40), movement_reference_number VARCHAR(40),
            arrival_date_time VARCHAR(30), port_of_arrival VARCHAR(200),
            submission_due_date VARCHAR(30), clear_date_time VARCHAR(30),
            total_packages INT,
            duty_totals_json NVARCHAR(MAX),
            error_code VARCHAR(20), error_message VARCHAR(500),
            goods_item_count INT DEFAULT 0,
            raw_json NVARCHAR(MAX), downloaded_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT UQ_{S}_SupDec UNIQUE(sup_dec_number))""",
    'SupDecGoodsItems': """
        CREATE TABLE {S}.SupDecGoodsItems (
            id INT IDENTITY(1,1) PRIMARY KEY,
            goods_id VARCHAR(40) NOT NULL,
            sup_dec_number VARCHAR(40), sfd_number VARCHAR(40),
            consignment_number VARCHAR(40),
            goods_description VARCHAR(300), commodity_code VARCHAR(20),
            type_of_packages VARCHAR(20), number_of_packages INT,
            gross_mass_kg DECIMAL(12,3), net_mass_kg DECIMAL(12,3),
            country_of_origin VARCHAR(5),
            item_invoice_amount DECIMAL(14,2), item_invoice_currency VARCHAR(5),
            procedure_code VARCHAR(10), additional_procedure_codes VARCHAR(50),
            controlled_goods VARCHAR(10), controlled_goods_type VARCHAR(20),
            package_marks VARCHAR(500),
            customs_value DECIMAL(14,2), statistical_value DECIMAL(14,2),
            valuation_indicator VARCHAR(10), valuation_method VARCHAR(5),
            preference VARCHAR(10), nature_of_transaction VARCHAR(10),
            invoice_number VARCHAR(40),
            raw_json NVARCHAR(MAX), downloaded_at DATETIME2 DEFAULT SYSUTCDATETIME(),
            CONSTRAINT UQ_{S}_SDGoods UNIQUE(goods_id))""",
}


# ══════════════════════════════════════════════════════════════
#  API + LOGGER (compact)
# ══════════════════════════════════════════════════════════════
class ApiLogger:
    def __init__(self):
        self.buffer = []; self.total_flushed = 0
    def log(self, dt, ref, ep, params, http, raw, ms, notes=''):
        self.buffer.append(((dt or '')[:50],'READ',(ref or '')[:200],None,CLIENT_CODE,'GET',
            (ep or '')[:500],json.dumps(params,separators=(',',':'))[:4000],
            http,'OK' if http==200 else 'FAIL',(raw or '')[:500],(raw or '')[:4000],ms,
            '' if http==200 else (raw or '')[:4000],(notes or f'Populate v{__version__}')[:200]))
        if len(self.buffer) >= LOG_BATCH: self.flush()
    def flush(self):
        if not self.buffer: return
        try:
            conn = make_conn(); cur = conn.cursor()
            cur.executemany(f"""INSERT INTO {S}.ApiLog (declaration_type,call_type,reference,
                act_as,act_as_customer,http_method,url,request_params,http_status,response_status,
                response_message,response_json,duration_ms,error_detail,notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", self.buffer)
            conn.commit(); conn.close(); self.total_flushed += len(self.buffer)
        except Exception as e: con.print(f'    [dim red]ApiLog: {e}[/dim red]')
        self.buffer.clear()

class TssApi:
    def __init__(self, base_url, username, password, logger):
        self.base_url = base_url.rstrip('/') + '/x_fhmrc_tss_api/v1/tss_api'
        self.logger = logger; self.session = requests.Session()
        b64 = base64.b64encode(f'{username}:{password}'.encode()).decode()
        self.session.headers.update({'Accept':'application/json','Authorization':f'Basic {b64}'})
        self.total_calls = 0
    def _get(self, ep, params, dt='', ref='', notes=''):
        url = f'{self.base_url}/{ep}'
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


def sget(d, k, default=''):
    return d.get(k, default) if isinstance(d, dict) else default

def safe_int(v):
    try: return int(float(str(v)))
    except: return None

def safe_dec(v):
    try: return float(v)
    except: return None

def extract_goods_ids(result):
    if not result: return []
    items = result if isinstance(result, list) else [result]
    if len(items)==1 and isinstance(items[0], dict):
        for key in ('goods','goods_items'):
            if key in items[0]:
                sub = items[0][key]; items = sub if isinstance(sub, list) else [sub]; break
    return [str(i.get('goods_id') or i.get('reference') or '') for i in items
            if isinstance(i, dict) and (i.get('goods_id') or i.get('reference'))]


# ══════════════════════════════════════════════════════════════
#  DYNAMIC UPSERT (discovers columns + types from INFORMATION_SCHEMA)
# ══════════════════════════════════════════════════════════════
_col_cache = {}  # {schema.table: {col_name_lower: data_type}}

def get_table_cols(schema, table):
    key = f'{schema}.{table}'
    if key not in _col_cache:
        rows = query("""
            SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=? AND TABLE_NAME=?""", [schema, table])
        _col_cache[key] = {r['COLUMN_NAME'].lower(): r['DATA_TYPE'].lower() for r in rows}
    return _col_cache[key]

def coerce(val, data_type):
    """Convert API string value to match the DB column type."""
    if val is None or val == '':
        return None
    if data_type in ('int','bigint','smallint','tinyint'):
        try: return int(float(str(val)))
        except: return None
    if data_type in ('decimal','numeric','float','real','money'):
        try: return float(str(val))
        except: return None
    if data_type in ('bit',):
        return 1 if str(val).lower() in ('true','1','yes') else 0
    # Everything else: string
    s = str(val)
    return s[:4000] if data_type in ('nvarchar','ntext') else s[:500]

def dynamic_upsert(cur, schema, table, pk_col, pk_val, data, extras=None):
    """Insert or update a row, only using columns that actually exist in the table.
    data = dict from API response
    extras = {col: value} for computed fields like goods_item_count, raw_json
    """
    col_types = get_table_cols(schema, table)
    if not col_types:
        return False

    # API key -> DB column renames
    RENAMES = {
        'status': 'tss_status',
        'identity_no_of_transport': 'identity_no_transport',
        'nationality_of_transport': 'nationality_transport',
    }
    SKIP = {'reference'}  # use pk_col instead

    # Build the column->value mapping from API data
    mapping = {}
    for api_key, val in data.items():
        db_col = RENAMES.get(api_key.lower(), api_key.lower())
        if db_col in SKIP: continue
        if db_col in col_types and db_col != pk_col.lower():
            # Skip complex nested objects (lists/dicts go into raw_json only)
            if isinstance(val, (list, dict)): continue
            mapping[db_col] = coerce(val, col_types[db_col])

    # Add extras (raw_json, goods_item_count, sfd_number, etc.)
    if extras:
        for col, val in extras.items():
            cl = col.lower()
            if cl in col_types:
                if cl == 'raw_json':
                    mapping[cl] = val  # already a string
                else:
                    mapping[cl] = coerce(val, col_types[cl])

    if not mapping:
        return False

    # Build INSERT
    ins_cols = [pk_col] + list(mapping.keys())
    ins_placeholders = ','.join(['?'] * len(ins_cols))
    ins_col_str = ','.join(f'[{c}]' for c in ins_cols)
    ins_vals = [pk_val] + list(mapping.values())

    # Build UPDATE (tss_status + raw_json + goods_item_count + downloaded_at)
    upd_sets = []
    upd_vals = []
    for uc in ('tss_status','raw_json','goods_item_count'):
        if uc in mapping:
            upd_sets.append(f'[{uc}]=?'); upd_vals.append(mapping[uc])
    if 'downloaded_at' in col_types:
        upd_sets.append('downloaded_at=SYSUTCDATETIME()')
    if not upd_sets:
        upd_sets.append('downloaded_at=SYSUTCDATETIME()')
    upd_vals.append(pk_val)

    sql = f"""
        IF NOT EXISTS (SELECT 1 FROM {schema}.[{table}] WHERE [{pk_col}]=?)
            INSERT INTO {schema}.[{table}] ({ins_col_str}) VALUES ({ins_placeholders})
        ELSE
            UPDATE {schema}.[{table}] SET {','.join(upd_sets)} WHERE [{pk_col}]=?"""

    params = [pk_val] + ins_vals + upd_vals
    cur.execute(sql, params)
    return True


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
def main():
    t0 = time.time()
    counts = {'ens':0,'cons':0,'sfd':0,'sfd_goods':0,'sd':0,'sd_goods':0}

    con.print(Panel.fit(
        f'[bold yellow]{__product__}[/bold yellow]  |  '
        f'[bold white]{__module__}[/bold white]  v{__version__}\n'
        f'[bold cyan]{CLIENT_NAME}[/bold cyan]  |  '
        f'[dim]{CLIENT_CODE}  |  {ENV_CODE}  |  {DB_NAME}  |  '
        f'{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC[/dim]\n'
        f'[dim]API -> BKD schema tables[/dim]',
        border_style='blue', padding=(0, 2)))

    # ── Step 1: Ensure tables exist ───────────────────────────
    con.print(); con.rule('[bold cyan]Step 1 -- Ensure BKD Tables[/bold cyan]'); con.print()
    for table_name, ddl in CREATE_TABLES.items():
        fqn = f'{S}.{table_name}'
        try:
            query(f"SELECT TOP 1 1 FROM {fqn}")
            cnt = query(f"SELECT COUNT(*) AS c FROM {fqn}")[0]['c']
            con.print(f'  [green]{fqn}[/green]  exists ({cnt} rows)')
        except:
            try:
                exec_sql(ddl.replace('{S}', S))
                con.print(f'  [yellow]{fqn}[/yellow]  CREATED')
            except Exception as e:
                con.print(f'  [red]{fqn}  CREATE FAILED: {e}[/red]')

    creds = load_credentials()
    logger = ApiLogger()
    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'], logger)
    con.print(f'  API: {creds["tss_username"]}')

    # ── Step 2: Read + Insert ENS Headers ─────────────────────
    con.print(); con.rule('[bold green]Step 2 -- ENS Headers[/bold green]'); con.print()
    conn = make_conn(); cur = conn.cursor()
    for ref in SEED_ENS:
        http, result, raw, ms = api._get('headers',
            {'reference':ref,'fields':ENS_FIELDS}, dt='ENS_READ', ref=ref)
        if http == 200 and result:
            dynamic_upsert(cur, S, 'EnsHeaders', 'declaration_number', ref, result, {'raw_json': json.dumps(result, default=str)}); counts['ens'] += 1
            con.print(f'  [green]{ref}[/green]  {sget(result,"status")}  '
                      f'port={sget(result,"arrival_port")}  {ms}ms')
        else:
            con.print(f'  [red]{ref}  HTTP {http}  {ms}ms[/red]')
    conn.commit(); conn.close(); logger.flush()

    # ── Step 3: Read + Insert Consignments ────────────────────
    con.print(); con.rule('[bold green]Step 3 -- Consignments[/bold green]'); con.print()
    all_cons = set(SEED_CONS)
    sfd_refs = set()
    conn = make_conn(); cur = conn.cursor()
    for ref in sorted(all_cons):
        http, result, raw, ms = api._get('consignments',
            {'reference':ref,'fields':CONS_FIELDS}, dt='CONS_READ', ref=ref)
        if http == 200 and result:
            dynamic_upsert(cur, S, 'Consignments', 'consignment_number', ref, result, {'raw_json': json.dumps(result, default=str)}); counts['cons'] += 1
            con.print(f'  [green]{ref}[/green]  {sget(result,"status")}  '
                      f'ENS={sget(result,"declaration_number")}  '
                      f'ctrl={sget(result,"controlled_goods")}  {ms}ms')
    conn.commit(); conn.close(); logger.flush()

    # ── Step 4: SFD Lookup + Read + Goods ─────────────────────
    con.print(); con.rule('[bold green]Step 4 -- SFDs + Goods[/bold green]'); con.print()

    # Also filter for any SFDs we haven't seen
    http, result, raw, ms = api._get('simplified_frontier_declarations',
        {'filter':'status=Arrived'}, dt='SFD_FILTER', ref='Arrived')
    if http == 200 and result:
        from_filter = [i.get('number','') for i in (result if isinstance(result, list) else [])
                       if isinstance(i, dict) and i.get('number')]
        sfd_refs.update(from_filter)

    # SFD Lookup from consignments
    for ref in sorted(all_cons):
        http, result, raw, ms = api._get('simplified_frontier_declarations',
            {'consignment_number':ref}, dt='SFD_LOOKUP', ref=ref)
        if http == 200 and result:
            sn = sget(result, 'sfd_number', '')
            if sn:
                sfd_refs.add(sn)
                con.print(f'  Cons {ref} -> SFD [cyan]{sn}[/cyan]')

    # Also check Draft/Submitted/Processing SFDs
    for status in ['Draft','Submitted','Processing','Authorised for Movement']:
        http, result, raw, ms = api._get('simplified_frontier_declarations',
            {'filter':f'status={status}'}, dt='SFD_FILTER', ref=status)
        if http == 200 and result:
            refs = [i.get('number','') for i in (result if isinstance(result, list) else [])
                    if isinstance(i, dict) and i.get('number')]
            sfd_refs.update(refs)
            if refs: con.print(f'  Filter {status}: {len(refs)} SFDs')

    # Read + insert each SFD
    conn = make_conn(); cur = conn.cursor()
    for sfd_ref in sorted(sfd_refs):
        http, result, raw, ms = api._get('simplified_frontier_declarations',
            {'reference':sfd_ref,'fields':SFD_FIELDS}, dt='SFD_READ', ref=sfd_ref)
        if http == 200 and result:
            # Goods lookup
            http2, result2, raw2, ms2 = api._get('goods',
                {'sfd_number':sfd_ref}, dt='GOODS_LOOKUP_SFD', ref=sfd_ref)
            gids = extract_goods_ids(result2) if http2==200 else []

            dynamic_upsert(cur, S, 'Sfds', 'sfd_number', sfd_ref, result, {'raw_json': json.dumps(result, default=str), 'goods_item_count': len(gids)}); counts['sfd'] += 1
            con.print(f'  [green]{sfd_ref}[/green]  {sget(result,"status")}  '
                      f'{len(gids)} goods  {ms}ms')

            # Read + insert each goods item
            for gid in gids:
                http3, result3, raw3, ms3 = api._get('goods',
                    {'reference':gid,'fields':GOODS_FIELDS}, dt='SFD_GOODS_READ', ref=gid)
                if http3 == 200 and result3:
                    dynamic_upsert(cur, S, 'SfdGoodsItems', 'goods_id', gid, result3, {'sfd_number': sfd_ref, 'raw_json': json.dumps(result3, default=str)})
                    counts['sfd_goods'] += 1
                    con.print(f'    {gid[:20]}..  {sget(result3,"commodity_code")}  '
                              f'{sget(result3,"goods_description","")[:25]}  {ms3}ms')
    conn.commit(); conn.close(); logger.flush()

    # ── Step 5: SD Lookup + Read + Goods ──────────────────────
    con.print(); con.rule('[bold green]Step 5 -- Sup Decs + Goods[/bold green]'); con.print()
    sd_refs = set()

    # SD lookup from SFDs
    for sfd_ref in sorted(sfd_refs):
        http, result, raw, ms = api._get('supplementary_declarations',
            {'sfd_number':sfd_ref}, dt='SD_LOOKUP', ref=sfd_ref)
        if http == 200 and result:
            sup = sget(result, 'sup_dec_number', '')
            for s in sup.split(','):
                s = s.strip()
                if s: sd_refs.add(s); con.print(f'  SFD {sfd_ref} -> SD [cyan]{s}[/cyan]')

    # Also filter for SDs
    for status in ['draft','closed','trader input required','submitted']:
        http, result, raw, ms = api._get('supplementary_declarations',
            {'filter':f'status={status}'}, dt='SD_FILTER', ref=status)
        if http == 200 and result:
            refs = [i.get('number','') for i in (result if isinstance(result, list) else [])
                    if isinstance(i, dict) and i.get('number')]
            sd_refs.update(refs)
            if refs: con.print(f'  Filter {status}: {len(refs)} SDs')

    if sd_refs:
        conn = make_conn(); cur = conn.cursor()
        for sd_ref in sorted(sd_refs):
            http, result, raw, ms = api._get('supplementary_declarations',
                {'reference':sd_ref,'fields':SD_FIELDS}, dt='SD_READ', ref=sd_ref)
            if http == 200 and result:
                # SD Goods lookup
                http2, result2, raw2, ms2 = api._get('goods',
                    {'sup_dec_number':sd_ref}, dt='GOODS_LOOKUP_SD', ref=sd_ref)
                gids = extract_goods_ids(result2) if http2==200 else []

                dynamic_upsert(cur, S, 'SupplementaryDeclarations', 'sup_dec_number', sd_ref, result, {'raw_json': json.dumps(result, default=str), 'goods_item_count': len(gids)}); counts['sd'] += 1
                con.print(f'  [green]{sd_ref}[/green]  {sget(result,"status")}  '
                          f'{len(gids)} goods  {ms}ms')

                for gid in gids:
                    http3, result3, raw3, ms3 = api._get('goods',
                        {'reference':gid,'fields':GOODS_FIELDS}, dt='SD_GOODS_READ', ref=gid)
                    if http3 == 200 and result3:
                        dynamic_upsert(cur, S, 'SupDecGoodsItems', 'goods_id', gid, result3, {'sup_dec_number': sd_ref, 'raw_json': json.dumps(result3, default=str)})
                        counts['sd_goods'] += 1
        conn.commit(); conn.close()
    else:
        con.print(f'  [yellow]No Sup Decs found[/yellow]')
    logger.flush()

    # ── Step 6: Verification ──────────────────────────────────
    elapsed = time.time() - t0; logger.flush()

    con.print(); con.rule('[bold cyan]Step 6 -- Verify[/bold cyan]'); con.print()
    for tbl in ['EnsHeaders','Consignments','Sfds','SfdGoodsItems',
                'SupplementaryDeclarations','SupDecGoodsItems']:
        try:
            cnt = query(f"SELECT COUNT(*) AS c FROM {S}.[{tbl}]")[0]['c']
            con.print(f'  {S}.{tbl}: [bold]{cnt}[/bold] rows')
        except: con.print(f'  [dim]{S}.{tbl}: error[/dim]')

    # Output JSON
    json_out = {
        'metadata': {'product':__product__,'module':__module__,'version':__version__,
            'generated':datetime.now(timezone.utc).isoformat(),
            'client':CLIENT_CODE,'env':ENV_CODE,
            'api_calls':api.total_calls,'elapsed':elapsed},
        'counts': counts,
        'sfd_refs': sorted(sfd_refs), 'sd_refs': sorted(sd_refs),
    }
    with open(JSON_FILE,'w',encoding='utf-8') as f:
        json.dump(json_out, f, indent=2, default=str)
    con.print(f'\n  JSON: [green]{JSON_FILE}[/green]')

    # Summary
    con.print(); con.rule('[bold yellow]Populate Complete[/bold yellow]'); con.print()
    tbl = Table(box=box.ROUNDED, title=f'[bold]{CLIENT_NAME} -- Populate v{__version__}[/bold]', border_style='green')
    tbl.add_column('Table', style='cyan', min_width=30)
    tbl.add_column('Rows Written', justify='right', style='green')
    tbl.add_row('ENS Headers', str(counts['ens']))
    tbl.add_row('Consignments', str(counts['cons']))
    tbl.add_row('SFDs', str(counts['sfd']))
    tbl.add_row('SFD Goods Items', str(counts['sfd_goods']))
    tbl.add_row('Supplementary Declarations', str(counts['sd']))
    tbl.add_row('SD Goods Items', str(counts['sd_goods']))
    tbl.add_row('')
    tbl.add_row('[bold]API Calls[/bold]', f'[bold]{api.total_calls}[/bold]')
    tbl.add_row('Runtime', f'{elapsed:.0f}s')
    con.print(tbl)
    con.print(f'\n  [dim]{__product__} v{__version__} -- {__module__} -- {CLIENT_NAME} -- Synovia Digital Ltd[/dim]\n')

if __name__ == '__main__':
    main()
