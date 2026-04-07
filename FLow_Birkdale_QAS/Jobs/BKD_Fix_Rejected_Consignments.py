"""
================================================================================
  Synovia Flow -- BKD Fix Rejected Consignments
  Licensed Component: Synovia Digital Ltd
================================================================================

  Version:  1.0.0
  Schema:   BKD (Birkdale)
  API:      TSS Declaration API v2.9.4

  Reads BKD.SFD_FixQueue, for each rejected consignment:
    1. LOOKUP  — find SFD reference from consignment DEC reference
    2. READ    — read SFD header to get the SFD header number
    3. UPDATE  — patch SFD header (carrier) and SFD consignment (parties)
    4. SUBMIT  — resubmit the SFD to CDS
    5. POLL    — check final status

  All calls logged to BKD.ApiLog. Queue status updated per step.

  Usage:
    python BKD_Fix_Rejected_Consignments.py
    python BKD_Fix_Rejected_Consignments.py --dry-run

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

import os, sys, time, json
from datetime import datetime, timezone

from BKD_Shared import (
    con, CLIENT_CODE, CLIENT_NAME, ENV_CODE, S, OUTPUT_DIR, DRY_RUN,
    query, execute, sget, load_credentials, TssApi,
    log_api_call, JsonLogger,
    make_run_id, print_banner, print_creds,
    print_summary_table, print_run_footer, RATE_LIMIT,
)

SCRIPT_NAME = 'BKD Fix Rejected Consignments'
QUEUE_TBL   = f'{S}.SFD_FixQueue'


# ──────────────────────────────────────────────────────────────
#  DB helpers
# ──────────────────────────────────────────────────────────────

def load_queue():
    """Load PENDING / FAILED fix queue entries."""
    return query(f"""
        SELECT * FROM {QUEUE_TBL}
        WHERE status IN ('PENDING', 'FAILED', 'LOOKUP', 'UPDATING')
          AND retry_count < max_retries
        ORDER BY fix_id
    """)


def update_queue(fix_id, **fields):
    """Update a fix queue row with arbitrary fields."""
    sets = ', '.join(f'{k} = ?' for k in fields.keys())
    vals = list(fields.values()) + [fix_id]
    execute(f"UPDATE {QUEUE_TBL} SET {sets}, last_attempted_at = SYSUTCDATETIME() WHERE fix_id = ?", vals)


# ──────────────────────────────────────────────────────────────
#  API call wrapper with logging
# ──────────────────────────────────────────────────────────────

def api_call(api, method, endpoint, params_or_payload, decl_type, call_type, ref, jlog, note=''):
    """Execute API call, log to ApiLog + JSON, return (http, result, raw, ms)."""
    url = f'{api.base_url}/{endpoint}'

    if method == 'GET':
        http, result, raw, ms = api.get(endpoint, params=params_or_payload)
    else:
        http, result, raw, ms = api.post(endpoint, params_or_payload)

    msg = ''
    api_status = ''
    if isinstance(result, dict):
        msg = result.get('process_message', '')
        api_status = result.get('status', '')

    # Log to BKD.ApiLog
    log_api_call(
        declaration_type=decl_type,
        call_type=call_type,
        reference=ref,
        http_method=method,
        url=url,
        request_params=params_or_payload,
        http_status=http,
        response_status=api_status or ('OK' if http == 200 else 'FAIL'),
        response_message=msg or (raw[:500] if raw else ''),
        response_json=raw,
        duration_ms=ms,
        error_detail='' if http == 200 else (raw or '')[:4000],
        notes=note,
    )

    # Log to JSON
    jlog.log_call(decl_type, call_type, ref, endpoint, method,
                  params_or_payload, http, result, raw, ms,
                  api_status, note)

    return http, result, raw, ms


# ──────────────────────────────────────────────────────────────
#  Step 1: LOOKUP — find SFD ref from consignment ref
# ──────────────────────────────────────────────────────────────

def step_lookup(api, row, jlog):
    """Lookup SFD reference from the ENS consignment DEC reference."""
    fix_id = row['fix_id']
    cons_ref = row['cons_dec_reference']

    con.print(f'    [dim]LOOKUP[/dim]  SFD from consignment {cons_ref}')

    http, result, raw, ms = api_call(
        api, 'GET', 'simplified_frontier_declarations',
        {'consignment_reference': cons_ref},
        'SFD_LOOKUP', 'READ', cons_ref, jlog,
        note=f'fix_id={fix_id} lookup SFD from consignment'
    )

    sfd_ref = ''
    if http == 200 and isinstance(result, dict):
        sfd_ref = result.get('sfd_number', '') or result.get('reference', '')

    if sfd_ref:
        update_queue(fix_id,
            sfd_reference=sfd_ref,
            lookup_status='OK',
            lookup_response=raw[:4000] if raw else '',
            status='LOOKUP',
            step_completed='LOOKUP'
        )
        con.print(f'           → [green]{sfd_ref}[/green]  ({ms}ms)')
        return sfd_ref
    else:
        update_queue(fix_id,
            lookup_status='FAIL',
            lookup_response=raw[:4000] if raw else '',
            error_message=f'SFD lookup failed: {raw[:500]}',
            retry_count=row['retry_count'] + 1
        )
        con.print(f'           → [red]FAILED[/red]  {raw[:80]}  ({ms}ms)')
        return None


# ──────────────────────────────────────────────────────────────
#  Step 2: READ — get current SFD state + header ref
# ──────────────────────────────────────────────────────────────

def step_read_sfd(api, row, sfd_ref, jlog):
    """Read SFD to get current status and ENS header reference."""
    fix_id = row['fix_id']

    con.print(f'    [dim]READ[/dim]    SFD {sfd_ref}')

    http, result, raw, ms = api_call(
        api, 'GET', 'simplified_frontier_declarations',
        {
            'reference': sfd_ref,
            'fields': 'status,ens_consignment_reference,error_message,'
                      'error_code,movement_reference_number,eori_for_eidr,'
                      'consignor_eori,consignee_eori,importer_eori,exporter_eori,'
                      'goods_description,transport_document_number'
        },
        'SFD_READ', 'READ', sfd_ref, jlog,
        note=f'fix_id={fix_id} read SFD state'
    )

    if http == 200 and isinstance(result, dict):
        status = result.get('status', '')
        ens_cons = result.get('ens_consignment_reference', '')
        error = result.get('error_message', '')
        con.print(f'           → status=[cyan]{status}[/cyan]  ens_cons={ens_cons}  ({ms}ms)')
        if error:
            con.print(f'           → error: {error[:120]}')
        return result
    else:
        con.print(f'           → [red]READ FAILED[/red]  ({ms}ms)')
        return None


# ──────────────────────────────────────────────────────────────
#  Step 2b: READ SFD Header
# ──────────────────────────────────────────────────────────────

def step_read_sfd_header(api, row, ens_ref, jlog):
    """Read the SFD header to find the SFD header number."""
    fix_id = row['fix_id']

    con.print(f'    [dim]READ[/dim]    SFD Header for ENS {ens_ref}')

    http, result, raw, ms = api_call(
        api, 'GET', 'sfd_headers',
        {
            'reference': ens_ref,
            'fields': 'status,carrier_eori,haulier_eori,movement_type,'
                      'identity_no_of_transport,arrival_port,arrival_date_time'
        },
        'SFD_HDR_READ', 'READ', ens_ref, jlog,
        note=f'fix_id={fix_id} read SFD header'
    )

    if http == 200 and isinstance(result, dict):
        carrier = result.get('carrier_eori', '')
        status = result.get('status', '')
        con.print(f'           → status=[cyan]{status}[/cyan]  carrier={carrier}  ({ms}ms)')
        return result
    else:
        con.print(f'           → [yellow]SFD Header not accessible[/yellow]  ({ms}ms)')
        return None


# ──────────────────────────────────────────────────────────────
#  Step 3: UPDATE — patch SFD consignment with corrected EORIs
# ──────────────────────────────────────────────────────────────

def step_update_sfd(api, row, sfd_ref, jlog):
    """Update the SFD consignment with corrected party EORIs."""
    fix_id = row['fix_id']

    payload = {
        'op_type':    'update',
        'sfd_number': sfd_ref,
    }

    # Add party fixes from queue — only include non-empty values
    field_map = {
        'consignor_eori':          'fix_consignor_eori',
        'consignor_name':          'fix_consignor_name',
        'consignee_eori':          'fix_consignee_eori',
        'consignee_name':          'fix_consignee_name',
        'importer_eori':           'fix_importer_eori',
        'importer_name':           'fix_importer_name',
        'exporter_eori':           'fix_exporter_eori',
        'exporter_name':           'fix_exporter_name',
        'exporter_street_number':  'fix_exporter_street',
        'exporter_city':           'fix_exporter_city',
        'exporter_postcode':       'fix_exporter_postcode',
        'exporter_country':        'fix_exporter_country',
    }
    for api_field, queue_field in field_map.items():
        v = sget(row, queue_field)
        if v:
            payload[api_field] = v

    con.print(f'    [dim]UPDATE[/dim]  SFD Consignment {sfd_ref}')

    http, result, raw, ms = api_call(
        api, 'POST', 'simplified_frontier_declarations', payload,
        'SFD_UPDATE', 'WRITE', sfd_ref, jlog,
        note=f'fix_id={fix_id} update SFD consignment EORIs'
    )

    msg = result.get('process_message', '') if isinstance(result, dict) else ''
    api_status = result.get('status', '') if isinstance(result, dict) else ''

    if http == 200 and api_status in ('updated', 'success', ''):
        update_queue(fix_id,
            update_status='OK',
            update_response=raw[:4000] if raw else '',
            status='UPDATING',
            step_completed='UPDATE_CONS'
        )
        con.print(f'           → [green]UPDATED[/green]  {msg}  ({ms}ms)')
        return True
    else:
        update_queue(fix_id,
            update_status='FAIL',
            update_response=raw[:4000] if raw else '',
            error_message=f'SFD update failed: {msg}'
        )
        con.print(f'           → [red]FAILED[/red]  {msg[:100]}  ({ms}ms)')
        return False


# ──────────────────────────────────────────────────────────────
#  Step 3b: UPDATE SFD Header (carrier EORI)
# ──────────────────────────────────────────────────────────────

def step_update_sfd_header(api, row, ens_ref, jlog):
    """Update the SFD header with corrected carrier EORI."""
    fix_id = row['fix_id']
    carrier = sget(row, 'fix_carrier_eori')
    if not carrier:
        con.print(f'    [dim]SKIP[/dim]   SFD Header update (no carrier fix)')
        return True

    payload = {
        'op_type':     'update',
        'reference':   ens_ref,
        'carrier_eori': carrier,
    }

    con.print(f'    [dim]UPDATE[/dim]  SFD Header {ens_ref}  carrier={carrier}')

    http, result, raw, ms = api_call(
        api, 'POST', 'sfd_headers', payload,
        'SFD_HDR_UPDATE', 'WRITE', ens_ref, jlog,
        note=f'fix_id={fix_id} update SFD header carrier_eori'
    )

    msg = result.get('process_message', '') if isinstance(result, dict) else ''
    api_status = result.get('status', '') if isinstance(result, dict) else ''

    if http == 200:
        con.print(f'           → [green]UPDATED[/green]  {msg}  ({ms}ms)')
        return True
    else:
        con.print(f'           → [yellow]HEADER UPDATE FAILED[/yellow]  {msg[:100]}  ({ms}ms)')
        # Don't block — header update may not be supported, continue to submit
        return False


# ──────────────────────────────────────────────────────────────
#  Step 4: SUBMIT — resubmit the SFD
# ──────────────────────────────────────────────────────────────

def step_submit_sfd(api, row, sfd_ref, jlog):
    """Resubmit the SFD to CDS."""
    fix_id = row['fix_id']

    payload = {
        'op_type':    'submit',
        'sfd_number': sfd_ref,
    }

    con.print(f'    [dim]SUBMIT[/dim]  SFD {sfd_ref}')

    http, result, raw, ms = api_call(
        api, 'POST', 'simplified_frontier_declarations', payload,
        'SFD_SUBMIT', 'WRITE', sfd_ref, jlog,
        note=f'fix_id={fix_id} resubmit SFD'
    )

    msg = result.get('process_message', '') if isinstance(result, dict) else ''
    api_status = result.get('status', '') if isinstance(result, dict) else ''

    if http == 200:
        update_queue(fix_id,
            submit_status='OK',
            submit_response=raw[:4000] if raw else '',
            status='RESUBMITTING',
            step_completed='SUBMIT'
        )
        con.print(f'           → [green]SUBMITTED[/green]  {msg}  ({ms}ms)')
        return True
    else:
        update_queue(fix_id,
            submit_status='FAIL',
            submit_response=raw[:4000] if raw else '',
            error_message=f'SFD submit failed: {msg}',
            retry_count=row['retry_count'] + 1
        )
        con.print(f'           → [red]FAILED[/red]  {msg[:100]}  ({ms}ms)')
        return False


# ──────────────────────────────────────────────────────────────
#  Step 5: POLL — check final status after submit
# ──────────────────────────────────────────────────────────────

def step_poll_status(api, row, sfd_ref, jlog, max_polls=5, wait_sec=3):
    """Poll the SFD status until it changes from Submitted/Processing."""
    fix_id = row['fix_id']
    terminal = {'Arrived', 'Authorised for Movement', 'Authorised for movement',
                'Trader Input Required', 'Amendment Required', 'Cancelled'}

    for attempt in range(1, max_polls + 1):
        time.sleep(wait_sec)
        con.print(f'    [dim]POLL[/dim]    {sfd_ref}  (attempt {attempt}/{max_polls})')

        http, result, raw, ms = api_call(
            api, 'GET', 'simplified_frontier_declarations',
            {'reference': sfd_ref, 'fields': 'status,error_message,movement_reference_number,eori_for_eidr'},
            'SFD_POLL', 'READ', sfd_ref, jlog,
            note=f'fix_id={fix_id} poll attempt {attempt}'
        )

        if http == 200 and isinstance(result, dict):
            status = result.get('status', '')
            mrn = result.get('movement_reference_number', '')
            eidr = result.get('eori_for_eidr', '')
            error = result.get('error_message', '')

            con.print(f'           → [cyan]{status}[/cyan]  mrn={mrn or eidr or ""}  ({ms}ms)')
            if error:
                con.print(f'           → error: {error[:120]}')

            if status in terminal:
                final = 'COMPLETED' if status in ('Arrived', 'Authorised for Movement',
                                                   'Authorised for movement') else 'FAILED'
                update_queue(fix_id,
                    final_tss_status=status,
                    error_message=error[:4000] if error else None,
                    status=final,
                    step_completed='POLL',
                    completed_at='SYSUTCDATETIME()'
                )
                # Fix: can't use SQL function in parameterised update
                execute(f"UPDATE {QUEUE_TBL} SET completed_at = SYSUTCDATETIME() WHERE fix_id = ?", [fix_id])
                return status

    # Timed out — record whatever we have
    con.print(f'    [yellow]POLL TIMEOUT[/yellow]  still processing after {max_polls} attempts')
    update_queue(fix_id, step_completed='POLL_TIMEOUT')
    return None


# ──────────────────────────────────────────────────────────────
#  MAIN — orchestrate the fix pipeline
# ──────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    run_id = make_run_id('FIX')
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print_banner(SCRIPT_NAME, run_id)
    creds = load_credentials()
    print_creds(creds)

    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'])
    jlog = JsonLogger(run_id)

    con.rule(f'[bold]{SCRIPT_NAME}[/bold]')
    con.print()

    queue = load_queue()
    if not queue:
        con.print('  [dim]No entries in SFD_FixQueue to process.[/dim]')
        return

    con.print(f'  Found [bold]{len(queue)}[/bold] fix(es) to process\n')

    completed = failed = skipped = 0
    errors = []

    # Track which ENS headers we've already fixed (avoid duplicate header updates)
    fixed_headers = set()

    for i, row in enumerate(queue, 1):
        fix_id = row['fix_id']
        cons_ref = row['cons_dec_reference']
        ens_ref = row['ens_reference']
        label = row.get('label', '')

        con.print(f'  [{i}/{len(queue)}]  fix_id={fix_id}  {cons_ref}  {label}')
        con.print(f'         ENS={ens_ref}')

        update_queue(fix_id, retry_count=row['retry_count'] + 1)

        # ── Step 1: LOOKUP ──
        sfd_ref = row.get('sfd_reference') or ''
        if not sfd_ref:
            sfd_ref = step_lookup(api, row, jlog)
            if not sfd_ref:
                failed += 1
                errors.append({'fix_id': fix_id, 'step': 'LOOKUP', 'ref': cons_ref})
                con.print()
                continue

        # ── Step 2: READ current state ──
        sfd_data = step_read_sfd(api, row, sfd_ref, jlog)
        if not sfd_data:
            failed += 1
            errors.append({'fix_id': fix_id, 'step': 'READ', 'ref': sfd_ref})
            con.print()
            continue

        current_status = sfd_data.get('status', '')
        if current_status not in ('Trader Input Required', 'Draft', 'Amendment Required'):
            con.print(f'    [yellow]SKIP[/yellow]  status=[cyan]{current_status}[/cyan] — not fixable')
            skipped += 1
            update_queue(fix_id, final_tss_status=current_status, status='SKIPPED')
            con.print()
            continue

        # ── Step 2b: READ + UPDATE SFD Header (carrier) ──
        if ens_ref and ens_ref not in fixed_headers:
            step_read_sfd_header(api, row, ens_ref, jlog)
            step_update_sfd_header(api, row, ens_ref, jlog)
            fixed_headers.add(ens_ref)

        # ── Step 3: UPDATE SFD consignment ──
        ok = step_update_sfd(api, row, sfd_ref, jlog)
        if not ok:
            failed += 1
            errors.append({'fix_id': fix_id, 'step': 'UPDATE', 'ref': sfd_ref})
            con.print()
            continue

        # ── Step 4: SUBMIT ──
        ok = step_submit_sfd(api, row, sfd_ref, jlog)
        if not ok:
            failed += 1
            errors.append({'fix_id': fix_id, 'step': 'SUBMIT', 'ref': sfd_ref})
            con.print()
            continue

        # ── Step 5: POLL ──
        final = step_poll_status(api, row, sfd_ref, jlog)
        if final and final in ('Arrived', 'Authorised for Movement', 'Authorised for movement'):
            completed += 1
            con.print(f'    [bold green]✓ RESOLVED[/bold green]  → {final}')
        else:
            failed += 1
            errors.append({'fix_id': fix_id, 'step': 'POLL', 'ref': sfd_ref, 'final': final})
            con.print(f'    [bold red]✗ STILL REJECTED[/bold red]  → {final}')

        con.print()

    elapsed = time.time() - t0
    con.print()
    con.rule('[bold yellow]Complete[/bold yellow]')
    print_summary_table(f'{CLIENT_NAME} -- {run_id}', [
        ('RESOLVED', completed, 0),
        ('FAILED', 0, failed),
        ('SKIPPED', skipped, 0),
    ])

    jlog.write_summary({
        'completed': completed, 'failed': failed, 'skipped': skipped,
        'errors': errors
    })
    print_run_footer(run_id, jlog, errors, elapsed)


if __name__ == '__main__':
    main()
