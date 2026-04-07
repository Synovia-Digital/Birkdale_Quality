"""
BKD_Status_Checker.py — TSS Declaration Status Monitor
Synovia Flow  |  Birkdale  |  v1.0.0

Calls EXC.usp_TSS_ExtractForStatusCheck to get all references,
reads each from TSS API, calls EXC.usp_TSS_RecordStatus per ref,
then closes the run with EXC.usp_TSS_CompleteRun.

Usage:
    python BKD_Status_Checker.py          # TST environment
    python BKD_Status_Checker.py --env PRD
"""
import sys, os, time, json, argparse
sys.path.insert(0, os.path.dirname(__file__))

from BKD_Shared import (
    make_conn, TssApi, con, ENV_CODE, CLIENT_CODE,
    make_run_id, sget, load_credentials
)

# ── API endpoint + read-fields map per declaration type ──

TYPE_CONFIG = {
    'ENS_HEADER': {
        'endpoint': 'headers',
        'ref_field': 'reference',
        'fields': 'status,movement_type,identity_no_of_transport,arrival_date_time,'
                  'arrival_port,place_of_loading,place_of_unloading,seal_number,'
                  'route,carrier_eori,carrier_name,haulier_eori,error_message',
        'status_key': 'status',
        'mrn_key': None,
        'error_key': 'error_message',
    },
    'CONSIGNMENT': {
        'endpoint': 'consignments',
        'ref_field': 'reference',
        'fields': 'status,declaration_number,goods_description,transport_document_number,'
                  'controlled_goods,total_packages,gross_mass_kg,'
                  'movement_reference_number,error_message',
        'status_key': 'status',
        'mrn_key': 'movement_reference_number',
        'error_key': 'error_message',
    },
    'GOODS_ITEM': {
        'endpoint': 'goods',
        'ref_field': 'reference',
        'fields': 'consignment_number,goods_description,commodity_code,'
                  'type_of_packages,number_of_packages,gross_mass_kg,net_mass_kg,'
                  'country_of_origin,procedure_code,controlled_goods',
        'status_key': None,  # goods don't have a status field; we track existence
        'mrn_key': None,
        'error_key': None,
    },
    'SFD': {
        'endpoint': 'simplified_frontier_declarations',
        'ref_field': 'reference',
        'fields': 'status,goods_description,transport_document_number,'
                  'controlled_goods,goods_domestic_status,importer_eori,'
                  'total_packages,gross_mass_kg,movement_reference_number,'
                  'eori_for_eidr,ens_consignment_reference,error_code,error_message',
        'status_key': 'status',
        'mrn_key': 'movement_reference_number',
        'error_key': 'error_message',
    },
    'SUPPLEMENTARY': {
        'endpoint': 'supplementary_declarations',
        'ref_field': 'reference',
        'fields': 'status,goods_description,transport_document_number,'
                  'total_packages,gross_mass_kg,error_message',
        'status_key': 'status',
        'mrn_key': None,
        'error_key': 'error_message',
    },
    'IMMI': {
        'endpoint': 'internal_market_movements',
        'ref_field': 'reference',
        'fields': 'status,goods_description,error_message',
        'status_key': 'status',
        'mrn_key': None,
        'error_key': 'error_message',
    },
}


def read_declaration(api, decl_type, reference):
    """Read a single declaration from TSS API.
    Returns (http_status, result_dict, raw_json, ms).
    """
    cfg = TYPE_CONFIG.get(decl_type)
    if not cfg:
        return 0, {}, '{"error":"Unknown type"}', 0

    # TssApi.get returns (http_status, result_dict, raw_text, duration_ms)
    http, result, raw_json, ms = api.get(cfg['endpoint'], params={
        cfg['ref_field']: reference,
        'fields': cfg['fields'],
    })
    return http, result, raw_json, ms


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', default='BKD')
    args = parser.parse_args()

    client = args.client

    con.print(f'\n[bold]Synovia Flow  |  BKD Status Checker  v1.0.0[/bold]')
    con.print(f'  Client: {client}  |  Env: {ENV_CODE}\n')

    # ── Connect DB ──
    conn = make_conn()
    cur = conn.cursor()

    # ── 1. Create run + extract check list ──
    con.print('[cyan]Extracting declaration references...[/cyan]')

    # Call SP — use two-step: first create the run, then get the list
    cur.execute("""
        DECLARE @rid INT;
        EXEC EXC.usp_TSS_ExtractForStatusCheck @client_code=?, @run_id=@rid OUTPUT;
        SELECT @rid AS run_id;
    """, (client,))

    # SP returns check list as first result set
    rows = cur.fetchall()
    columns = [d[0] for d in cur.description] if cur.description else []

    # If the first result set is the check list (has tracker_id column)
    if columns and columns[0] == 'tracker_id':
        check_list = [dict(zip(columns, r)) for r in rows]
        # Next result set has the run_id
        if cur.nextset():
            run_row = cur.fetchone()
            run_id = run_row[0] if run_row else 0
        else:
            run_id = 0
    elif columns and columns[0] == 'run_id':
        # Got run_id first (no check list rows)
        run_id = rows[0][0] if rows else 0
        check_list = []
    else:
        run_id = 0
        check_list = []

    conn.commit()

    if not check_list:
        con.print('  [dim]No declarations to check.[/dim]')
        return

    con.print(f'  Found {len(check_list)} declaration(s) to check  |  run_id={run_id}\n')

    # ── 2. Connect API ──
    creds = load_credentials()
    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'])
    con.print(f'  API: {api.base_url}\n')

    # ── 3. Check each ──
    checked = 0
    changed = 0
    errors = 0

    for item in check_list:
        tracker_id = item['tracker_id']
        decl_type = item['declaration_type']
        ref = item['reference']
        old_status = item.get('current_status') or ''
        label = item.get('label', '')

        cfg = TYPE_CONFIG.get(decl_type)
        if not cfg:
            con.print(f'  [yellow]SKIP[/yellow]  {decl_type} {ref} — no config')
            continue

        # API READ
        http, result, raw_json, ms = read_declaration(api, decl_type, ref)

        # Extract status, MRN, error
        new_status = result.get(cfg['status_key'], '') if cfg['status_key'] else 'EXISTS'
        mrn = result.get(cfg['mrn_key'], '') if cfg.get('mrn_key') else ''
        err = result.get(cfg['error_key'], '') if cfg.get('error_key') else ''

        if http == 0 or (isinstance(result, dict) and result.get('status') == 'error'):
            new_status = 'API_ERROR'
            err = raw_json[:500] if not err else err
            errors += 1

        # Detect change
        is_changed = (old_status or '') != (new_status or '')
        if is_changed:
            changed += 1
            marker = '[green]CHANGED[/green]'
        else:
            marker = '[dim]same[/dim]'

        tag = f'{decl_type:15s} {ref}'
        con.print(f'  {marker}  {tag}  {old_status or "?"} → {new_status}  ({ms}ms)')

        # Record to DB
        cur.execute("""
            EXEC EXC.usp_TSS_RecordStatus
                @run_id=?, @tracker_id=?, @new_status=?,
                @tss_error_message=?, @tss_mrn=?,
                @http_status=?, @response_json=?,
                @duration_ms=?, @notes=?
        """, (
            run_id, tracker_id, new_status,
            err or None, mrn or None,
            http, raw_json,
            ms, label
        ))
        conn.commit()
        checked += 1

    # ── 4. Complete run ──
    con.print(f'\n[bold]Run complete[/bold]  checked={checked}  changed={changed}  errors={errors}')

    cur.execute("""
        EXEC EXC.usp_TSS_CompleteRun @run_id=?, @status=?, @notes=?
    """, (
        run_id,
        'COMPLETED' if errors == 0 else 'COMPLETED_WITH_ERRORS',
        f'checked={checked} changed={changed} errors={errors}'
    ))
    conn.commit()

    # ── 5. Print dashboard ──
    con.print('\n[bold cyan]── Status Dashboard ──[/bold cyan]')
    cur.execute('EXEC EXC.usp_TSS_StatusDashboard @client_code=?', (client,))

    dash_rows = cur.fetchall()
    dash_cols = [d[0] for d in cur.description] if cur.description else []

    if dash_rows:
        con.print(f'\n  {"Type":<15} {"Reference":<25} {"Status":<20} {"MRN":<25} {"Checks":<6}')
        con.print(f'  {"─"*15} {"─"*25} {"─"*20} {"─"*25} {"─"*6}')
        for r in dash_rows:
            d = dict(zip(dash_cols, r))
            con.print(f'  {d.get("declaration_type",""):<15} '
                      f'{d.get("reference",""):<25} '
                      f'{d.get("current_status",""):<20} '
                      f'{(d.get("tss_mrn","") or ""):<25} '
                      f'{d.get("check_count",0):<6}')

    conn.close()
    con.print(f'\n  Synovia Flow v1.0.0 -- Birkdale -- Synovia Digital Ltd\n')


if __name__ == '__main__':
    main()
