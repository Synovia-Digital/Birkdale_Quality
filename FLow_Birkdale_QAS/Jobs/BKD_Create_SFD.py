"""
================================================================================
  Synovia Flow -- BKD Create SFD
  Licensed Component: Synovia Digital Ltd
================================================================================

  Version:  1.0.0
  Schema:   BKD (Birkdale)
  API:      TSS Declaration API v2.9.4

  Reads PENDING SFDs from BKD.StagingSfds where the parent
  Consignment is CREATED, submits to the TSS /sfds endpoint,
  updates status, logs everything to BKD.ApiLog and JSON.

  SFDs can be read without actAs, but creation may require it
  depending on the trader configuration.

  Usage:
    python BKD_Create_SFD.py              # normal run
    python BKD_Create_SFD.py --dry-run    # validate without submitting

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

import os, sys, time
from datetime import datetime, timezone

from BKD_Shared import (
    con, CLIENT_CODE, CLIENT_NAME, ENV_CODE, S, OUTPUT_DIR, DRY_RUN,
    ACT_AS_EORI, ACT_AS_CUSTOMER,
    query, execute, sget, load_credentials, TssApi,
    log_api_call, JsonLogger,
    is_retryable, sc, make_run_id, print_banner, print_creds,
    print_summary_table, print_run_footer,
)

SCRIPT_NAME = 'BKD Create SFD'
DECL_TYPE   = 'SFD'
ENDPOINT    = 'sfds'
STAGING_TBL = f'{S}.StagingSfds'


def load_pending():
    """Load PENDING, FAILED, or INVALID SFDs whose parent consignment is CREATED."""
    return query(f"""
        SELECT s.*, c.dec_reference AS parent_dec_ref
        FROM {S}.StagingSfds s
        JOIN {S}.StagingConsignments c ON c.staging_id = s.staging_cons_id
        WHERE s.status IN ('PENDING', 'FAILED', 'INVALID')
          AND s.retry_count < s.max_retries
          AND c.status = 'CREATED'
          AND c.dec_reference IS NOT NULL
        ORDER BY s.staging_id
    """)


def build_payload(row):
    """Build the API request payload from a staging row."""
    payload = {
        'op_type':                  'create',
        'consignment_number':       row['parent_dec_ref'],
        'sfd_number':               '',
        'goods_description':        row['goods_description'],
        'commodity_code':           sget(row, 'commodity_code'),
        'country_of_origin':        sget(row, 'country_of_origin'),
        'gross_mass_kg':            str(row['gross_mass_kg']),
        'net_mass_kg':              str(sget(row, 'net_mass_kg')),
        'type_of_packages':         row['type_of_packages'],
        'number_of_packages':       str(row['number_of_packages']),
        'item_invoice_amount':      str(sget(row, 'item_invoice_amount')),
        'item_invoice_currency':    sget(row, 'item_invoice_currency', 'GBP'),
        'procedure_code':           sget(row, 'procedure_code'),
        'additional_procedure_code': sget(row, 'additional_procedure_code'),
        'duty_preference':          sget(row, 'duty_preference'),
        'customs_value':            str(sget(row, 'customs_value')),
        'valuation_method':         sget(row, 'valuation_method', '1'),
    }
    # Optional fields
    for f in ['supplementary_units', 'quota_order_number', 'document_type',
              'document_reference', 'document_status', 'previous_document_type',
              'previous_document_reference', 'previous_document_category']:
        v = row.get(f)
        if v:
            payload[f] = str(v)
    return payload


def submit(api, run_id, jlog):
    """Submit all pending SFDs. Returns (created, failed, errors)."""
    rows = load_pending()
    if not rows:
        con.print('  [dim]No PENDING SFDs (or parent Consignment not CREATED)[/dim]')
        return 0, 0, []

    con.print(f'  Found [bold]{len(rows)}[/bold] SFD(s) to submit\n')
    created = failed = 0
    errors = []

    for i, row in enumerate(rows, 1):
        sid = row['staging_id']
        dec_ref = row['parent_dec_ref']
        payload = build_payload(row)
        desc = (row['goods_description'] or '')[:30]

        con.print(
            f'  {i:>3}/{len(rows)}  [dim]#{sid}[/dim]  '
            f'<- [cyan]{dec_ref}[/cyan]  '
            f'{desc}  CC={sget(row, "commodity_code", "?")}  '
            f'PC={sget(row, "procedure_code", "?")}'
        )

        execute(f"""
            UPDATE {STAGING_TBL}
            SET status = 'SUBMITTED',
                submitted_at = SYSUTCDATETIME(),
                last_attempted_at = SYSUTCDATETIME(),
                retry_count = retry_count + 1
            WHERE staging_id = ?
        """, [sid])

        if DRY_RUN:
            con.print('       [dim]DRY RUN -- payload validated, not sent[/dim]')
            execute(f"UPDATE {STAGING_TBL} SET status='PENDING', retry_count=retry_count-1 WHERE staging_id=?", [sid])
            jlog.log_call(DECL_TYPE, 'DRY_RUN', dec_ref, ENDPOINT, 'POST', payload, 0, {}, '', 0, 'dry_run', f'#{sid}')
            con.print()
            continue

        # ── API call ──
        http, result, raw, ms = api.post(ENDPOINT, payload)
        ref = result.get('reference', '')
        msg = result.get('process_message', '')
        api_status = result.get('status', '')
        url = f'{api.base_url}/{ENDPOINT}'

        # ── Log to BKD.ApiLog ──
        log_api_call(
            declaration_type=DECL_TYPE,
            call_type='CREATE',
            reference=ref or f'staging:{sid}',
            http_method='POST',
            url=url,
            request_params=payload,
            http_status=http,
            response_status=api_status,
            response_message=msg,
            response_json=raw,
            duration_ms=ms,
            error_detail='' if http == 200 else (raw or '')[:4000],
            notes=f'staging_id={sid} dec_ref={dec_ref} cc={sget(row, "commodity_code")} pc={sget(row, "procedure_code")}',
            act_as=ACT_AS_EORI,
            act_as_customer=ACT_AS_CUSTOMER,
        )

        # ── Log to JSON ──
        jlog.log_call(DECL_TYPE, 'CREATE', ref, ENDPOINT, 'POST', payload,
                       http, result, raw, ms, api_status, f'#{sid} <- {dec_ref}')

        # ── Update staging ──
        if http == 200 and api_status == 'created':
            execute(f"""
                UPDATE {STAGING_TBL}
                SET status = 'CREATED',
                    sfd_reference = ?,
                    api_status = ?,
                    api_message = ?,
                    http_status = ?,
                    completed_at = SYSUTCDATETIME()
                WHERE staging_id = ?
            """, [ref, api_status, msg[:500], http, sid])
            con.print(f'       [green]CREATED  {ref}[/green]  {ms}ms')
            created += 1
        else:
            new_status = 'FAILED' if is_retryable(msg) else 'INVALID'
            execute(f"""
                UPDATE {STAGING_TBL}
                SET status = ?,
                    api_status = ?,
                    api_message = ?,
                    http_status = ?,
                    error_message = ?
                WHERE staging_id = ?
            """, [new_status, api_status, msg[:500], http, (raw or '')[:4000], sid])
            con.print(f'       [{sc(new_status)}]{new_status}  HTTP {http}  {msg[:60]}[/{sc(new_status)}]')
            errors.append({
                'type': DECL_TYPE, 'staging_id': sid, 'http': http, 'message': msg,
                'dec_ref': dec_ref, 'commodity_code': sget(row, 'commodity_code'),
                'raw': raw[:2000] if raw else '',
            })
            failed += 1

        con.print()

    return created, failed, errors


# ──────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    run_id = make_run_id('SFD')
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print_banner(SCRIPT_NAME, run_id)
    creds = load_credentials()
    print_creds(creds)

    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'])
    jlog = JsonLogger(run_id)

    con.rule(f'[bold magenta]{SCRIPT_NAME}[/bold magenta]')
    con.print()

    ok, fail, errs = submit(api, run_id, jlog)

    elapsed = time.time() - t0
    con.print()
    con.rule('[bold yellow]Complete[/bold yellow]')
    print_summary_table(f'{CLIENT_NAME} -- {run_id}', [(DECL_TYPE, ok, fail)])
    jlog.write_summary({'created': ok, 'failed': fail, 'errors': errs})
    print_run_footer(run_id, jlog, errs, elapsed)


if __name__ == '__main__':
    main()
