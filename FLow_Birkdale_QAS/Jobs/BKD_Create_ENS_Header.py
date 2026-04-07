"""
================================================================================
  Synovia Flow -- BKD Create ENS Header
  Licensed Component: Synovia Digital Ltd
================================================================================

  Version:  1.0.0
  Schema:   BKD (Birkdale)
  API:      TSS Declaration API v2.9.4

  Reads PENDING ENS Headers from BKD.StagingEnsHeaders, submits to
  the TSS /headers endpoint, updates status, logs everything to
  BKD.ApiLog and JSON files.

  Usage:
    python BKD_Create_ENS_Header.py              # normal run
    python BKD_Create_ENS_Header.py --dry-run    # validate without submitting

  ENS Headers do NOT require actAs parameter.

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

import os, sys, time
from datetime import datetime, timezone

from BKD_Shared import (
    con, CLIENT_CODE, CLIENT_NAME, ENV_CODE, S, OUTPUT_DIR, DRY_RUN,
    query, execute, sget, load_credentials, TssApi,
    log_api_call, JsonLogger,
    is_retryable, sc, make_run_id, print_banner, print_creds,
    print_summary_table, print_run_footer,
)

SCRIPT_NAME = 'BKD Create ENS Header'
DECL_TYPE   = 'ENS_HEADER'
ENDPOINT    = 'headers'
STAGING_TBL = f'{S}.StagingEnsHeaders'


def load_pending():
    """Load PENDING, FAILED, or INVALID ENS Headers."""
    return query(f"""
        SELECT *
        FROM {STAGING_TBL}
        WHERE status IN ('PENDING', 'FAILED', 'INVALID')
          AND retry_count < max_retries
        ORDER BY staging_id
    """)


def build_payload(row):
    """Build the API request payload from a staging row."""
    return {
        'op_type':                    'create',
        'declaration_number':         '',
        'movement_type':              row['movement_type'],
        'identity_no_of_transport':   row['identity_no_of_transport'],
        'nationality_of_transport':   row['nationality_of_transport'],
        'arrival_date_time':          row['arrival_date_time'],
        'arrival_port':               row['arrival_port'],
        'place_of_loading':           row['place_of_loading'],
        'place_of_unloading':         row['place_of_unloading'],
        'seal_number':                sget(row, 'seal_number'),
        'transport_charges':          row['transport_charges'],
        'carrier_eori':               row['carrier_eori'],
        'carrier_name':               sget(row, 'carrier_name'),
        'carrier_street_number':      sget(row, 'carrier_street_number'),
        'carrier_city':               sget(row, 'carrier_city'),
        'carrier_postcode':           sget(row, 'carrier_postcode'),
        'carrier_country':            sget(row, 'carrier_country'),
        'haulier_eori':               sget(row, 'haulier_eori'),
    }


def submit(api, run_id, jlog):
    """Submit all pending ENS Headers. Returns (created, failed, errors)."""
    rows = load_pending()
    if not rows:
        con.print('  [dim]No PENDING ENS Headers[/dim]')
        return 0, 0, []

    con.print(f'  Found [bold]{len(rows)}[/bold] ENS Header(s) to submit\n')
    created = failed = 0
    errors = []

    for i, row in enumerate(rows, 1):
        sid = row['staging_id']
        payload = build_payload(row)

        # Console preview
        con.print(
            f'  {i:>3}/{len(rows)}  [dim]#{sid}[/dim]  '
            f'mt=[bold]{row["movement_type"]}[/bold]  '
            f'id=[bold]{row["identity_no_of_transport"]}[/bold]  '
            f'port={row["arrival_port"]}  '
            f'[yellow]{row["arrival_date_time"]}[/yellow]'
        )

        # Mark as SUBMITTED
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
            jlog.log_call(DECL_TYPE, 'DRY_RUN', '', ENDPOINT, 'POST', payload, 0, {}, '', 0, 'dry_run',
                          f'#{sid} mt={row["movement_type"]} id={row["identity_no_of_transport"]}')
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
            notes=f'staging_id={sid} mt={row["movement_type"]} id={row["identity_no_of_transport"]} port={row["arrival_port"]}',
            act_as=None,            # ENS Headers: no actAs required
            act_as_customer=None,
        )

        # ── Log to JSON ──
        jlog.log_call(DECL_TYPE, 'CREATE', ref, ENDPOINT, 'POST', payload,
                       http, result, raw, ms, api_status,
                       f'#{sid} mt={row["movement_type"]} id={row["identity_no_of_transport"]}')

        # ── Update staging ──
        if http == 200 and api_status == 'created':
            execute(f"""
                UPDATE {STAGING_TBL}
                SET status = 'CREATED',
                    ens_reference = ?,
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
            con.print(f'       [{sc(new_status)}]{new_status}  HTTP {http}  {msg[:80]}[/{sc(new_status)}]')
            errors.append({
                'type': DECL_TYPE, 'staging_id': sid, 'http': http, 'message': msg,
                'movement_type': row['movement_type'],
                'identity': row['identity_no_of_transport'],
                'port': row['arrival_port'],
                'raw': raw[:2000] if raw else '',
            })
            failed += 1

        con.print()

    return created, failed, errors


# ──────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    run_id = make_run_id('ENS')
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print_banner(SCRIPT_NAME, run_id)
    creds = load_credentials()
    print_creds(creds)

    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'])
    jlog = JsonLogger(run_id)

    con.rule(f'[bold cyan]{SCRIPT_NAME}[/bold cyan]')
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
