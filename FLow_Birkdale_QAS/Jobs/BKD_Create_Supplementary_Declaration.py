"""
================================================================================
  Synovia Flow -- BKD Create Supplementary Declaration
  Licensed Component: Synovia Digital Ltd
================================================================================

  Version:  1.0.0
  Schema:   BKD (Birkdale)
  API:      TSS Declaration API v2.9.4

  Reads PENDING Supplementary Declarations from
  BKD.StagingSupplementaryDeclarations, submits to the TSS
  /supplementary_declarations endpoint, updates status, logs
  everything to BKD.ApiLog and JSON.

  NOTE: Supplementary Declarations REQUIRE the actAs parameter.

  Usage:
    python BKD_Create_Supplementary_Declaration.py
    python BKD_Create_Supplementary_Declaration.py --dry-run

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

SCRIPT_NAME = 'BKD Create Supplementary Declaration'
DECL_TYPE   = 'SUPPLEMENTARY'
ENDPOINT    = 'supplementary_declarations'
STAGING_TBL = f'{S}.StagingSupplementaryDeclarations'


def load_pending():
    """Load PENDING, FAILED, or INVALID Supplementary Declarations."""
    return query(f"""
        SELECT sd.*, c.dec_reference AS parent_dec_ref
        FROM {S}.StagingSupplementaryDeclarations sd
        JOIN {S}.StagingConsignments c ON c.staging_id = sd.staging_cons_id
        WHERE sd.status IN ('PENDING', 'FAILED', 'INVALID')
          AND sd.retry_count < sd.max_retries
          AND c.status = 'CREATED'
          AND c.dec_reference IS NOT NULL
        ORDER BY sd.staging_id
    """)


def build_payload(row):
    """Build the API request payload for a Supplementary Declaration.

    NOTE: This is a HEADER-level resource. Goods items are added
    separately via the goods endpoint using the SD reference.

    Required fields per TSS API Data Model:
      op_type, declaration_choice, authorisation_type,
      arrival_date_time, representation_type, controlled_goods,
      additional_procedure, goods_domestic_status
    """
    payload = {
        'op_type':                   sget(row, 'op_type', 'update'),
        'sup_dec_number':            sget(row, 'supp_dec_reference', ''),
        'declaration_choice':        sget(row, 'declaration_choice', 'H1'),
        'authorisation_type':        sget(row, 'authorisation_type', 'SDE'),
        'arrival_date_time':         sget(row, 'arrival_date_time', ''),
        'representation_type':       sget(row, 'representation_type', '2'),
        'controlled_goods':          sget(row, 'controlled_goods', 'no'),
        'additional_procedure':      sget(row, 'additional_procedure', 'no'),
        'goods_domestic_status':     sget(row, 'goods_domestic_status', 'D'),
    }
    # Optional fields — only include when populated
    optional = {
        'supervising_customs_office':   sget(row, 'supervising_customs_office'),
        'customs_warehouse_identifier': sget(row, 'customs_warehouse_identifier'),
        'importer_name':                sget(row, 'importer_name'),
        'importer_street_number':       sget(row, 'importer_street_number'),
        'importer_city':                sget(row, 'importer_city'),
        'importer_postcode':            sget(row, 'importer_postcode'),
        'importer_country':             sget(row, 'importer_country'),
    }
    for k, v in optional.items():
        if v:
            payload[k] = v
    return payload


def submit(api, run_id, jlog):
    """Submit all pending Supplementary Declarations. Returns (created, failed, errors)."""
    rows = load_pending()
    if not rows:
        con.print('  [dim]No PENDING Supplementary Declarations (or parent not CREATED)[/dim]')
        return 0, 0, []

    con.print(f'  Found [bold]{len(rows)}[/bold] Supplementary Declaration(s) to submit\n')
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
            f'PC={sget(row, "procedure_code", "?")}  '
            f'type={sget(row, "declaration_type", "?")}'
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
                    supp_dec_reference = ?,
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
    run_id = make_run_id('SUPP')
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print_banner(SCRIPT_NAME, run_id)
    creds = load_credentials()
    print_creds(creds)

    if not ACT_AS_EORI:
        con.print('  [bold red]WARNING: actAs EORI not configured.[/bold red]')
        con.print('  [red]Supplementary Declarations require actAs. Check CFG.Credentials.[/red]')
        con.print()

    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'])
    jlog = JsonLogger(run_id)

    con.rule(f'[bold red]{SCRIPT_NAME}[/bold red]')
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
