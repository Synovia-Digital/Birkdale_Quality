"""
================================================================================
  Synovia Flow -- BKD Create Consignment
  Licensed Component: Synovia Digital Ltd
================================================================================

  Version:  1.0.0
  Schema:   BKD (Birkdale)
  API:      TSS Declaration API v2.9.4

  Reads PENDING Consignments from BKD.StagingConsignments where the
  parent ENS Header is CREATED, submits to the TSS /consignments
  endpoint, updates status, logs everything to BKD.ApiLog and JSON.

  NOTE: Consignments REQUIRE the actAs parameter.

  Usage:
    python BKD_Create_Consignment.py              # normal run
    python BKD_Create_Consignment.py --dry-run    # validate without submitting

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

SCRIPT_NAME = 'BKD Create Consignment'
DECL_TYPE   = 'CONSIGNMENT'
ENDPOINT    = 'consignments'
STAGING_TBL = f'{S}.StagingConsignments'


def propagate_ens_refs():
    """Push ENS references down from CREATED headers to child consignments."""
    n = execute(f"""
        UPDATE c
        SET c.ens_reference = e.ens_reference
        FROM {S}.StagingConsignments c
        JOIN {S}.StagingEnsHeaders e ON e.staging_id = c.staging_ens_id
        WHERE e.status = 'CREATED'
          AND e.ens_reference IS NOT NULL
          AND c.ens_reference IS NULL
    """)
    if n:
        con.print(f'  Propagated ENS refs to [bold]{n}[/bold] consignment(s)')
    return n


def load_pending():
    """Load PENDING, FAILED, or INVALID Consignments whose parent ENS is CREATED."""
    return query(f"""
        SELECT c.*, e.ens_reference AS parent_ens_ref
        FROM {S}.StagingConsignments c
        JOIN {S}.StagingEnsHeaders e ON e.staging_id = c.staging_ens_id
        WHERE c.status IN ('PENDING', 'FAILED', 'INVALID')
          AND c.retry_count < c.max_retries
          AND e.status = 'CREATED'
          AND e.ens_reference IS NOT NULL
        ORDER BY c.staging_id
    """)


def build_payload(row):
    """Build the API request payload from a staging row.
    Optional fields are OMITTED when empty — the TSS API rejects
    empty strings on mandatory fields like no_sfd_reason.
    """
    payload = {
        'op_type':                   'create',
        'declaration_number':        row['parent_ens_ref'],
        'consignment_number':        '',
        'goods_description':         row['goods_description'],
        'transport_document_number': row['transport_document_number'],
        'controlled_goods':          sget(row, 'controlled_goods', 'no'),
        'goods_domestic_status':     sget(row, 'goods_domestic_status', 'D'),
        'destination_country':       sget(row, 'destination_country', 'GB'),
        'container_indicator':       sget(row, 'container_indicator', '1'),
        'no_sfd_reason':             sget(row, 'no_sfd_reason'),
        'buyer_same_as_importer':    sget(row, 'buyer_same_as_importer', 'yes'),
        'seller_same_as_exporter':   sget(row, 'seller_same_as_exporter', 'yes'),
    }
    # Conditionally add fields -- only include when populated
    optional = {
        'consignor_eori':          sget(row, 'consignor_eori'),
        'consignor_name':          sget(row, 'consignor_name'),
        'consignor_street_number': sget(row, 'consignor_street_number'),
        'consignor_city':          sget(row, 'consignor_city'),
        'consignor_postcode':      sget(row, 'consignor_postcode'),
        'consignor_country':       sget(row, 'consignor_country'),
        'consignee_eori':          sget(row, 'consignee_eori'),
        'consignee_name':          sget(row, 'consignee_name'),
        'consignee_street_number': sget(row, 'consignee_street_number'),
        'consignee_city':          sget(row, 'consignee_city'),
        'consignee_postcode':      sget(row, 'consignee_postcode'),
        'consignee_country':       sget(row, 'consignee_country'),
        'importer_eori':           sget(row, 'importer_eori'),
        'importer_name':           sget(row, 'importer_name'),
        'importer_street_number':  sget(row, 'importer_street_number'),
        'importer_city':           sget(row, 'importer_city'),
        'importer_postcode':       sget(row, 'importer_postcode'),
        'importer_country':        sget(row, 'importer_country'),
        'exporter_eori':           sget(row, 'exporter_eori'),
        'exporter_name':           sget(row, 'exporter_name'),
        'exporter_street_number':  sget(row, 'exporter_street_number'),
        'exporter_city':           sget(row, 'exporter_city'),
        'exporter_postcode':       sget(row, 'exporter_postcode'),
        'exporter_country':        sget(row, 'exporter_country'),
    }
    for k, v in optional.items():
        if v:  # only include non-empty values
            payload[k] = v
    return payload


def submit(api, run_id, jlog):
    """Submit all pending Consignments. Returns (created, failed, errors)."""
    rows = load_pending()
    if not rows:
        con.print('  [dim]No PENDING Consignments (or parent ENS not CREATED)[/dim]')
        return 0, 0, []

    con.print(f'  Found [bold]{len(rows)}[/bold] Consignment(s) to submit\n')
    created = failed = 0
    errors = []

    for i, row in enumerate(rows, 1):
        sid = row['staging_id']
        ens_ref = row['parent_ens_ref']
        payload = build_payload(row)

        # Console preview
        label = sget(row, 'label') or (row['goods_description'] or '')[:35]
        con.print(
            f'  {i:>3}/{len(rows)}  [dim]#{sid}[/dim]  '
            f'<- [cyan]{ens_ref}[/cyan]  '
            f'{label}  '
            f'no_sfd_reason={sget(row, "no_sfd_reason", "?")}'
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
            jlog.log_call(DECL_TYPE, 'DRY_RUN', ens_ref, ENDPOINT, 'POST', payload, 0, {}, '', 0, 'dry_run', f'#{sid}')
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
            notes=f'staging_id={sid} ens_ref={ens_ref} no_sfd={sget(row, "no_sfd_reason")}',
            act_as=ACT_AS_EORI,
            act_as_customer=ACT_AS_CUSTOMER,
        )

        # ── Log to JSON ──
        jlog.log_call(DECL_TYPE, 'CREATE', ref, ENDPOINT, 'POST', payload,
                       http, result, raw, ms, api_status, f'#{sid} <- {ens_ref}')

        # ── Update staging ──
        if http == 200 and api_status == 'created':
            execute(f"""
                UPDATE {STAGING_TBL}
                SET status = 'CREATED',
                    dec_reference = ?,
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
                'ens_ref': ens_ref, 'no_sfd_reason': sget(row, 'no_sfd_reason'),
                'raw': raw[:2000] if raw else '',
            })
            failed += 1

        con.print()

    return created, failed, errors


# ──────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    run_id = make_run_id('CONS')
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print_banner(SCRIPT_NAME, run_id)
    creds = load_credentials()
    print_creds(creds)

    api = TssApi(creds['base_url'], creds['tss_username'], creds['tss_password'])
    jlog = JsonLogger(run_id)

    # Propagate ENS refs before submitting
    con.rule('[bold]Propagate ENS References[/bold]')
    propagate_ens_refs()
    con.print()

    con.rule(f'[bold green]{SCRIPT_NAME}[/bold green]')
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
