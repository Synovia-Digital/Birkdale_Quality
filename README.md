# Synovia Fusion — Birkdale QAS Repository

## Overview

This repository contains the authoritative local development version of the **Birkdale QAS** project within the Synovia Fusion release structure.

Birkdale QAS is a **TSS Test Solution** built on the Synovia Flow platform, providing:
- A **live web dashboard** (Flask on Render) for managing TSS declarations
- **Background jobs** (APScheduler) for polling/downloading declarations and syncing choice values
- **Individual upload scripts** for each declaration type via the TSS Declaration API v2.9.4
- **PowerShell configuration tooling** for build verification, deployment logging, and script backup

This repository is intended to be the **single source of truth** for development and Git version control.

---

## Repository Root

`D:\Applications\Fusion_Release_4\Fusion_TSS`

### Repository-Level Files
- `.gitignore`
- `README.md`
- `LICENSE`

The repository root should only contain repository-level metadata and control files.

---

## Authoritative Project Root

`D:\Applications\Fusion_Release_4\Fusion_TSS\FLow_Birkdale_QAS`

This is the **only valid live project folder** for Birkdale QAS.

---

## Primary Solution and Project Files

### Solution File
`D:\Applications\Fusion_Release_4\Fusion_TSS\FLow_Birkdale_QAS\FLow_Birkdale_QAS.slnx`

### Project File
`D:\Applications\Fusion_Release_4\Fusion_TSS\FLow_Birkdale_QAS\FLow_Birkdale_QAS.pyproj`

### Entry Script
`D:\Applications\Fusion_Release_4\Fusion_TSS\FLow_Birkdale_QAS\FLow_Birkdale_QAS.py`

---

## Approved Folder Structure

    Fusion_TSS/
    │   .gitignore
    │   README.md
    │   LICENSE
    │
    └───FLow_Birkdale_QAS/
        │   FLow_Birkdale_QAS.slnx
        │   FLow_Birkdale_QAS.pyproj
        │   FLow_Birkdale_QAS.py
        │   Check_Config.ps1
        │
        ├───Jobs/
        │   ├───Job_Download_Declarations.py
        │   ├───Job_Upload_ENS_Header.py
        │   ├───Job_Upload_Consignment.py
        │   ├───Job_Upload_Goods_Item.py
        │   ├───Job_Upload_Sup_Dec.py
        │   ├───Job_Upload_FFD.py
        │   ├───Job_Upload_IMMI.py
        │   └───Job_Upload_GVMS_GMR.py
        │
        ├───Render/
        │   ├───app.py
        │   ├───scheduler.py
        │   ├───render.yaml
        │   ├───Procfile
        │   ├───build.sh
        │   ├───requirements.txt
        │   ├───templates/
        │   │   ├───dashboard.html
        │   │   ├───declarations_list.html
        │   │   ├───declarations_create.html
        │   │   └───declaration_detail.html
        │   └───static/
        │       └───css/
        │           └───style.css
        │
        └───Utilities/
            ├───tss_client.py
            └───.env.template

---

## Folder Responsibilities

### Jobs
Contains core processing logic, operational jobs, and execution workflows. Each job script targets a specific TSS Declaration API resource and supports `--payload`, `--interactive`, and `--dry-run` modes.

| Script | TSS Resource | Operations |
|--------|-------------|------------|
| `Job_Download_Declarations.py` | All readable resources | Poll/download by status |
| `Job_Upload_ENS_Header.py` | `headers` | Create/Update ENS Declaration Headers |
| `Job_Upload_Consignment.py` | `consignments` | Create/Update Consignments |
| `Job_Upload_Goods_Item.py` | `goods` | Create/Update/Delete Goods Items |
| `Job_Upload_Sup_Dec.py` | `supplementary_declarations` | Create/Update Standalone Sup Decs |
| `Job_Upload_FFD.py` | `full_frontier_declarations` | Create/Update FFDs (H1–H4) |
| `Job_Upload_IMMI.py` | `internal_market_movements` | Create/Update IMMIs (IMD/IMA/IMZ) |
| `Job_Upload_GVMS_GMR.py` | `gvms` | Create/Update/Submit/Cancel GMRs |

### Render
Contains the live web application deployed to Render, consisting of a Flask dashboard (`app.py`) for browsing and submitting declarations, and a background scheduler (`scheduler.py`) running periodic jobs via APScheduler. The `render.yaml` blueprint defines both a web service and a worker service.

**Dashboard routes:**

| Route | Purpose |
|-------|---------|
| `/` | Dashboard — environment status, API health, job overview |
| `/declarations` | List/filter declarations by resource and status |
| `/declarations/create` | Multi-resource creation wizard with payload templates |
| `/declarations/<resource>/<ref>` | Read a specific declaration |
| `/api/health` | Health check (used by Render) |
| `/api/jobs/status` | Background job statuses |
| `/api/declarations/<resource>` | REST proxy to TSS API |

**Scheduled jobs:**

| Job | Schedule | Description |
|-----|----------|-------------|
| `download_declarations` | Every 15 min | Polls TSS for declarations by status, saves JSON snapshots |
| `sync_choice_values` | Daily 02:00 UTC | Downloads choice value reference data for all CV fields |
| `health_ping` | Every 5 min | Connectivity check against TSS API |

### Utilities
Contains shared helper functions and reusable support code, including the centralised `TSSAPIClient` class (`tss_client.py`) and the `.env.template` for environment variable configuration.

---

## TSS Declaration API v2.9.4

The project targets the TSS Declaration API across two environments:

| Environment | Base URL |
|-------------|----------|
| TEST | `https://api.tsstestenv.co.uk` |
| PROD | `https://api.tradersupportservice.co.uk` |

**API path:** `/api/x_fhmrc_tss_api/v1/tss_api/<resource>`

### Resources (12)

| Resource | Label | Operations |
|----------|-------|------------|
| `headers` | Declaration Header | Create/Update, Cancel, Read |
| `consignments` | Consignment | Create/Update, Submit/Cancel, Read |
| `goods` | Goods Item | Create/Update, Delete, Lookup, Read |
| `sfd_headers` | SFD Header | Create/Update, Cancel, Read |
| `simplified_frontier_declarations` | SFD | Lookup, Read/Filter, Submit |
| `supplementary_declarations` | Supplementary Declaration | Lookup, Read/Filter, Standalone Create, Submit, Recall, Duty Read |
| `full_frontier_declarations` | Full Frontier Declaration | Create/Update, Read, Submit |
| `inventory_claims` | Maritime Inventory Claim | Create/Update, Submit/Cancel, Read |
| `agents` | Agent | Relationship, ActAs |
| `gvms` | GVMS GMR | Create, Update, Submit, Read, Cancel |
| `internal_market_movements` | Internal Market Movement | Create/Update, Submit/Cancel, Read, Reclassify H1→H8, H8→H1 |
| `permission_grant` | Permission Grant | Read |

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TSS_API_BASE_URL` | TSS API base URL | `https://api.tsstestenv.co.uk` |
| `TSS_API_USERNAME` | Basic auth username | _(none)_ |
| `TSS_API_PASSWORD` | Basic auth password | _(none)_ |
| `TSS_ENV` | Environment label (`test` or `prod`) | `test` |
| `SECRET_KEY` | Flask secret key | dev default |
| `PORT` | Web server port | `5000` |

Copy `Utilities/.env.template` to `.env` and fill in credentials. Never commit `.env` to source control.

---

## Entry Script Usage

The main entry point (`FLow_Birkdale_QAS.py`) provides a launcher for local development:

    python FLow_Birkdale_QAS.py info         # Print project info
    python FLow_Birkdale_QAS.py web          # Start Flask dev server
    python FLow_Birkdale_QAS.py scheduler    # Start background job scheduler
    python FLow_Birkdale_QAS.py job download # Run download job (--all)
    python FLow_Birkdale_QAS.py job ens      # Interactive ENS header builder
    python FLow_Birkdale_QAS.py job ffd      # Interactive FFD builder

---

## Check_Config.ps1

PowerShell configuration script residing at the project root. Handles build verification, deployment logging, and versioned script backups to a `config\` subfolder. Run with `-DryRun` to preview actions without modifying files.

The script maintains a `Build_Log.yaml` with a timestamped entry for each deployment, referencing the script name, version, source, and target paths.

---

## Render Deployment

1. Push the `Render/` folder contents to a Git repo (or use the project repo with root directory set to `FLow_Birkdale_QAS/Render`)
2. Connect the repo to Render — it auto-detects `render.yaml`
3. Set `TSS_API_USERNAME` and `TSS_API_PASSWORD` in the Render dashboard
4. Render creates two services: **birkdale-qas-web** (Flask dashboard) and **birkdale-qas-jobs** (APScheduler worker)

---

## Development Rules

1. Always open the solution from:
   `D:\Applications\Fusion_Release_4\Fusion_TSS\FLow_Birkdale_QAS\FLow_Birkdale_QAS.slnx`

2. Perform Git operations from:
   `D:\Applications\Fusion_Release_4\Fusion_TSS`

3. Do **not** develop from legacy network UNC paths.

4. Do **not** create parallel project folders in the repository.

5. Keep the repository clean and free from cache, backup, or temporary files.

---

## Excluded Content

The following must not be treated as active project content:

### Cache / Temporary
- `.vs/`
- `__pycache__/`
- `*.pyc`

### Old / Archive Folders
- `*_OLD_*`
- `Git_Local_OLD_*`
- `Birkdale_Quality_OLD_*`

### Git Internal Data
- `.git/`

---

## Deployment Rule

This repository is the **development source**.

Deployment locations, network paths, and runtime copies are **not** the source of truth and must be treated as output targets only.

In plain English: code lives here first, then gets copied outward. Not the other way around.

---

## Git Governance

### Branch
`main`

### Expected Healthy State
- working tree clean
- no merge conflicts
- no junk files staged
- no duplicate project roots

---

## Final Principle

There is exactly **one** active project in this repository:

`FLow_Birkdale_QAS`

Everything else is metadata, tooling, or historical noise.
