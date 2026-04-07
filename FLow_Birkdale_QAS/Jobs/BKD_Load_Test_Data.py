"""
================================================================================
  Synovia Flow -- BKD Load Test Data
  Licensed Component: Synovia Digital Ltd
================================================================================

  Version:  1.0.0
  Schema:   BKD (Birkdale)
  API:      TSS Declaration API v2.9.4

  Reads BKD_Test_Data.xlsx and inserts rows into BKD staging tables.
  Handles FK mapping: staging_ens_id / staging_cons_id in the spreadsheet
  are 1-based row numbers; the loader maps them to actual IDENTITY values
  after INSERT.

  Usage:
    python BKD_Load_Test_Data.py                         # default file
    python BKD_Load_Test_Data.py BKD_Test_Data.xlsx      # explicit path
    python BKD_Load_Test_Data.py --clear                 # truncate + reload

  Copyright (c) 2026 Synovia Digital Ltd. All rights reserved.
================================================================================
"""

__version__ = '1.0.0'

import os, sys
import pandas as pd
from BKD_Shared import (
    con, CLIENT_CODE, CLIENT_NAME, ENV_CODE, S, DB_NAME,
    query, execute, make_conn, sc,
)
from rich.table import Table
from rich import box

DEFAULT_FILE = r'D:\Birkdale_Scaffold\Config\Test_data\BKD_Test_Data.xlsx'
CLEAR_MODE = '--clear' in sys.argv


def get_file_path():
    """Resolve the Excel file path from args or default."""
    for arg in sys.argv[1:]:
        if not arg.startswith('--') and arg.endswith('.xlsx'):
            return arg
    return DEFAULT_FILE


def truncate_staging():
    """Truncate all staging tables (order matters for FKs if enforced)."""
    tables = [
        'StagingImmis', 'StagingSupplementaryDeclarations', 'StagingSfds',
        'StagingGoodsItems', 'StagingConsignments', 'StagingEnsHeaders',
    ]
    for t in tables:
        n = execute(f"DELETE FROM {S}.{t}")
        con.print(f'  Cleared {S}.{t}  ({n} rows)')
    # Reseed identities
    for t in tables:
        try:
            execute(f"DBCC CHECKIDENT ('{S}.{t}', RESEED, 0)")
        except Exception:
            pass  # CHECKIDENT may not be available on all configs
    con.print()


def insert_ens_headers(df):
    """Insert ENS Headers and return mapping of spreadsheet row → staging_id."""
    con.print('  [bold cyan]ENS Headers[/bold cyan]')
    mapping = {}
    cols = [c for c in df.columns if c not in ('staging_id',)]
    for idx, row in df.iterrows():
        spreadsheet_row = idx + 1  # 1-based
        placeholders = ', '.join(['?'] * len(cols))
        col_list = ', '.join(cols)
        vals = [None if pd.isna(row[c]) else row[c] for c in cols]

        conn = make_conn()
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO {S}.StagingEnsHeaders ({col_list})
            OUTPUT INSERTED.staging_id
            VALUES ({placeholders})
        """, vals)
        new_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

        mapping[spreadsheet_row] = new_id
        con.print(f'    Row {spreadsheet_row} → staging_id={new_id}  {row.get("label", "")}')
    con.print(f'  Inserted [bold]{len(mapping)}[/bold] ENS Header(s)\n')
    return mapping


def insert_consignments(df, ens_map):
    """Insert Consignments with FK mapping and return cons mapping."""
    con.print('  [bold green]Consignments[/bold green]')
    mapping = {}
    cols = [c for c in df.columns if c not in ('staging_id',)]
    for idx, row in df.iterrows():
        spreadsheet_row = idx + 1
        vals = []
        for c in cols:
            v = row[c]
            if pd.isna(v):
                vals.append(None)
            elif c == 'staging_ens_id':
                mapped = ens_map.get(int(v))
                if not mapped:
                    con.print(f'    [red]Row {spreadsheet_row}: staging_ens_id={v} not found in ENS map![/red]')
                    continue
                vals.append(mapped)
            else:
                vals.append(v)

        placeholders = ', '.join(['?'] * len(cols))
        col_list = ', '.join(cols)

        conn = make_conn()
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO {S}.StagingConsignments ({col_list})
            OUTPUT INSERTED.staging_id
            VALUES ({placeholders})
        """, vals)
        new_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

        mapping[spreadsheet_row] = new_id
        con.print(f'    Row {spreadsheet_row} → staging_id={new_id}  ens_id={vals[0]}  {row.get("label", "")}')
    con.print(f'  Inserted [bold]{len(mapping)}[/bold] Consignment(s)\n')
    return mapping


def insert_child_table(table_name, display_name, df, parent_map, fk_col, ref_col=None):
    """Generic insert for child tables (goods, sfds, supp decs, immis)."""
    con.print(f'  [bold blue]{display_name}[/bold blue]')
    cols = [c for c in df.columns if c not in ('staging_id',)]
    count = 0
    for idx, row in df.iterrows():
        spreadsheet_row = idx + 1
        vals = []
        for c in cols:
            v = row[c]
            if pd.isna(v):
                vals.append(None)
            elif c == fk_col:
                mapped = parent_map.get(int(v))
                if not mapped:
                    con.print(f'    [red]Row {spreadsheet_row}: {fk_col}={v} not found in parent map![/red]')
                    continue
                vals.append(mapped)
            elif isinstance(v, float) and v == int(v) and c not in ('gross_mass_kg', 'net_mass_kg', 'customs_value', 'item_invoice_amount', 'statistical_value'):
                vals.append(int(v))
            else:
                vals.append(v)

        placeholders = ', '.join(['?'] * len(cols))
        col_list = ', '.join(cols)

        conn = make_conn()
        cur = conn.cursor()
        cur.execute(f"""
            INSERT INTO {S}.{table_name} ({col_list})
            OUTPUT INSERTED.staging_id
            VALUES ({placeholders})
        """, vals)
        new_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

        parent_val = vals[cols.index(fk_col)] if fk_col in cols else '?'
        con.print(f'    Row {spreadsheet_row} → staging_id={new_id}  {fk_col}={parent_val}  {row.get("label", "")}')
        count += 1
    con.print(f'  Inserted [bold]{count}[/bold] {display_name}\n')
    return count


# ══════════════════════════════════════════════════════════════
def main():
    filepath = get_file_path()
    con.print()
    con.rule(f'[bold yellow]BKD Load Test Data  v{__version__}[/bold yellow]')
    con.print()
    con.print(f'  File:     [green]{filepath}[/green]')
    con.print(f'  Client:   [cyan]{CLIENT_NAME}[/cyan]  ({CLIENT_CODE})')
    con.print(f'  Env:      [bold yellow]{ENV_CODE}[/bold yellow]')
    con.print(f'  Database: {DB_NAME}')
    con.print(f'  Schema:   {S}')
    con.print()

    if not os.path.exists(filepath):
        con.print(f'  [red]File not found: {filepath}[/red]')
        sys.exit(1)

    # Read all sheets
    sheets = pd.read_excel(filepath, sheet_name=None, dtype=str)
    available = list(sheets.keys())
    con.print(f'  Sheets found: {", ".join(available)}')
    con.print()

    if CLEAR_MODE:
        con.rule('[bold red]Clearing existing data[/bold red]')
        truncate_staging()

    # Check current counts
    con.rule('[bold]Current Staging Counts[/bold]')
    for t in ['StagingEnsHeaders', 'StagingConsignments', 'StagingGoodsItems',
              'StagingSfds', 'StagingSupplementaryDeclarations', 'StagingImmis']:
        rows = query(f"SELECT COUNT(*) AS cnt FROM {S}.{t}")
        con.print(f'  {S}.{t:40s}  {rows[0]["cnt"]} rows')
    con.print()

    con.rule('[bold green]Loading Test Data[/bold green]')
    con.print()

    # Convert numeric columns back from string
    def prep(df, int_cols=None, float_cols=None):
        for c in (int_cols or []):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
        for c in (float_cols or []):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        return df

    # 1. ENS Headers
    if 'StagingEnsHeaders' in sheets:
        df = prep(sheets['StagingEnsHeaders'])
        ens_map = insert_ens_headers(df)
    else:
        con.print('  [red]StagingEnsHeaders sheet not found![/red]')
        return

    # 2. Consignments
    if 'StagingConsignments' in sheets:
        df = prep(sheets['StagingConsignments'], int_cols=['staging_ens_id'])
        cons_map = insert_consignments(df, ens_map)
    else:
        con.print('  [red]StagingConsignments sheet not found![/red]')
        return

    # 3. Goods Items
    if 'StagingGoodsItems' in sheets:
        df = prep(sheets['StagingGoodsItems'],
                  int_cols=['staging_cons_id', 'number_of_packages'],
                  float_cols=['gross_mass_kg', 'net_mass_kg', 'item_invoice_amount'])
        insert_child_table('StagingGoodsItems', 'Goods Items', df, cons_map, 'staging_cons_id')

    # 4. SFDs
    if 'StagingSfds' in sheets:
        df = prep(sheets['StagingSfds'],
                  int_cols=['staging_cons_id', 'number_of_packages'],
                  float_cols=['gross_mass_kg', 'net_mass_kg', 'item_invoice_amount', 'customs_value'])
        insert_child_table('StagingSfds', 'SFDs', df, cons_map, 'staging_cons_id')

    # 5. Supplementary Declarations
    if 'StagingSuppDecs' in sheets:
        df = prep(sheets['StagingSuppDecs'],
                  int_cols=['staging_cons_id', 'number_of_packages'],
                  float_cols=['gross_mass_kg', 'net_mass_kg', 'item_invoice_amount', 'customs_value', 'statistical_value'])
        insert_child_table('StagingSupplementaryDeclarations', 'Supplementary Declarations',
                           df, cons_map, 'staging_cons_id')

    # 6. IMMIs
    if 'StagingImmis' in sheets:
        df = prep(sheets['StagingImmis'], int_cols=['staging_ens_id'])
        insert_child_table('StagingImmis', 'IMMIs', df, ens_map, 'staging_ens_id')

    # Final counts
    con.print()
    con.rule('[bold]Final Staging Counts[/bold]')
    tbl = Table(box=box.ROUNDED, border_style='green', title=f'[bold]{CLIENT_NAME} Test Data[/bold]')
    tbl.add_column('Table', style='cyan', min_width=40)
    tbl.add_column('Total', justify='right', style='green')
    tbl.add_column('Pending', justify='right', style='yellow')
    for t in ['StagingEnsHeaders', 'StagingConsignments', 'StagingGoodsItems',
              'StagingSfds', 'StagingSupplementaryDeclarations', 'StagingImmis']:
        rows = query(f"SELECT COUNT(*) AS cnt, SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) AS pending FROM {S}.{t}")
        tbl.add_row(f'{S}.{t}', str(rows[0]['cnt']), str(rows[0]['pending']))
    con.print(tbl)

    # Pipeline status
    con.print()
    ps = query(f"SELECT * FROM {S}.vw_PipelineStatus ORDER BY declaration_type, status")
    if ps:
        tbl2 = Table(box=box.SIMPLE, border_style='dim', title='[bold]Pipeline Status[/bold]')
        tbl2.add_column('Type', style='cyan')
        tbl2.add_column('Status')
        tbl2.add_column('Count', justify='right', style='green')
        for r in ps:
            tbl2.add_row(r['declaration_type'], f'[{sc(r["status"])}]{r["status"]}[/{sc(r["status"])}]', str(r['item_count']))
        con.print(tbl2)

    con.print()
    con.print('  [bold green]Test data loaded successfully.[/bold green]')
    con.print()
    con.print('  [dim]Next steps:[/dim]')
    con.print('  [dim]  1. python BKD_Create_ENS_Header.py          # submit ENS headers[/dim]')
    con.print('  [dim]  2. python BKD_Create_Consignment.py         # submit consignments[/dim]')
    con.print('  [dim]  3. python BKD_Create_Goods_Item.py          # submit goods items[/dim]')
    con.print('  [dim]  4. python BKD_Create_SFD.py                 # submit SFDs[/dim]')
    con.print('  [dim]  5. python BKD_Create_Supplementary_Declaration.py[/dim]')
    con.print('  [dim]  6. python BKD_Create_IMMI.py                # submit IMMIs[/dim]')
    con.print()
    con.print('  [dim]Or use --dry-run on any script to validate payloads without API calls.[/dim]')
    con.print()


if __name__ == '__main__':
    main()
