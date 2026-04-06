# ============================================================
# Synovia / Fusion_TSS Cleanup Script
# No CLI arguments required
# Set variables below, then run script
# ============================================================

$ErrorActionPreference = "Stop"

# -----------------------------
# Configuration
# -----------------------------
$RepoRoot = "D:\Applications\Fusion_Release_4\Fusion_TSS"
$ProjectFolderName = "FLow_Birkdale_QAS"

# Preview only = $true
# Actually delete = $false
$PreviewOnly = $false

# Optional cleanup switches
$DeleteOldFolders = $true
$DeleteVSFolders = $true
$DeletePyCacheFolders = $true
$DeletePycFiles = $true
$DeleteRootDuplicateCheckConfig = $true
$DeleteProjectDuplicateReadme = $true
$DeleteProjectDuplicateLicense = $true

# Safety log
$LogFolder = "D:\Applications\Fusion_Release_4\Fusion_TSS\Catalogues"

# -----------------------------
# Helper functions
# -----------------------------
function Write-Info($msg) {
    Write-Host "[INFO] $msg" -ForegroundColor Cyan
}

function Write-Warn($msg) {
    Write-Host "[WARN] $msg" -ForegroundColor Yellow
}

function Write-Ok($msg) {
    Write-Host "[ OK ] $msg" -ForegroundColor Green
}

function Write-Preview($msg) {
    Write-Host "[PREVIEW] $msg" -ForegroundColor Magenta
}

function Remove-Target {
    param(
        [string]$Path,
        [string]$Reason
    )

    if (-not (Test-Path $Path)) {
        return
    }

    if ($script:PreviewOnly) {
        Write-Preview "$Path  <-- $Reason"
        Add-Content -Path $script:LogFile -Value "[PREVIEW] $Path <-- $Reason"
    }
    else {
        Write-Warn "Deleting: $Path"
        Remove-Item -Path $Path -Recurse -Force -ErrorAction Stop
        Write-Ok "Deleted: $Path"
        Add-Content -Path $script:LogFile -Value "[DELETED] $Path <-- $Reason"
    }
}

# -----------------------------
# Setup
# -----------------------------
if (-not (Test-Path $RepoRoot)) {
    throw "Repo root not found: $RepoRoot"
}

$ProjectRoot = Join-Path $RepoRoot $ProjectFolderName

if (-not (Test-Path $ProjectRoot)) {
    throw "Project root not found: $ProjectRoot"
}

if (-not (Test-Path $LogFolder)) {
    New-Item -ItemType Directory -Path $LogFolder -Force | Out-Null
}

$TimeStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = Join-Path $LogFolder "Cleanup_FusionTSS_$TimeStamp.log"
$script:LogFile = $LogFile
$script:PreviewOnly = $PreviewOnly

Add-Content -Path $LogFile -Value "Cleanup started: $(Get-Date)"
Add-Content -Path $LogFile -Value "RepoRoot: $RepoRoot"
Add-Content -Path $LogFile -Value "ProjectRoot: $ProjectRoot"
Add-Content -Path $LogFile -Value "PreviewOnly: $PreviewOnly"
Add-Content -Path $LogFile -Value ""

Write-Info "Repo root   : $RepoRoot"
Write-Info "Project root: $ProjectRoot"
Write-Info "Preview only: $PreviewOnly"
Write-Host ""

# -----------------------------
# 1. Delete _OLD_ folders
# -----------------------------
if ($DeleteOldFolders) {
    Write-Info "Scanning for _OLD_ folders..."
    Get-ChildItem -Path $RepoRoot -Directory -Force | Where-Object {
        $_.Name -like "*_OLD_*"
    } | ForEach-Object {
        Remove-Target -Path $_.FullName -Reason "Old archived junk folder inside repo"
    }
    Write-Host ""
}

# -----------------------------
# 2. Delete .vs folders
# -----------------------------
if ($DeleteVSFolders) {
    Write-Info "Scanning for .vs folders..."
    Get-ChildItem -Path $RepoRoot -Directory -Recurse -Force | Where-Object {
        $_.Name -eq ".vs"
    } | ForEach-Object {
        Remove-Target -Path $_.FullName -Reason "Visual Studio cache folder"
    }
    Write-Host ""
}

# -----------------------------
# 3. Delete __pycache__ folders
# -----------------------------
if ($DeletePyCacheFolders) {
    Write-Info "Scanning for __pycache__ folders..."
    Get-ChildItem -Path $RepoRoot -Directory -Recurse -Force | Where-Object {
        $_.Name -eq "__pycache__"
    } | ForEach-Object {
        Remove-Target -Path $_.FullName -Reason "Python cache folder"
    }
    Write-Host ""
}

# -----------------------------
# 4. Delete *.pyc files
# -----------------------------
if ($DeletePycFiles) {
    Write-Info "Scanning for *.pyc files..."
    Get-ChildItem -Path $RepoRoot -File -Recurse -Force | Where-Object {
        $_.Extension -eq ".pyc"
    } | ForEach-Object {
        Remove-Target -Path $_.FullName -Reason "Compiled Python cache file"
    }
    Write-Host ""
}

# -----------------------------
# 5. Remove duplicate root Check_Config.ps1
# -----------------------------
if ($DeleteRootDuplicateCheckConfig) {
    Write-Info "Checking duplicate Check_Config.ps1..."
    $RootCheckConfig = Join-Path $RepoRoot "Check_Config.ps1"
    $ProjectCheckConfig = Join-Path $ProjectRoot "Check_Config.ps1"

    if ((Test-Path $RootCheckConfig) -and (Test-Path $ProjectCheckConfig)) {
        Remove-Target -Path $RootCheckConfig -Reason "Duplicate support script at repo root; keep project-local copy"
    }
    else {
        Write-Info "No duplicate Check_Config.ps1 cleanup needed."
    }
    Write-Host ""
}

# -----------------------------
# 6. Remove duplicate project README.md
# -----------------------------
if ($DeleteProjectDuplicateReadme) {
    Write-Info "Checking duplicate README.md..."
    $RootReadme = Join-Path $RepoRoot "README.md"
    $ProjectReadme = Join-Path $ProjectRoot "README.md"

    if ((Test-Path $RootReadme) -and (Test-Path $ProjectReadme)) {
        Remove-Target -Path $ProjectReadme -Reason "Duplicate README inside project; keep repo-level copy"
    }
    else {
        Write-Info "No duplicate README cleanup needed."
    }
    Write-Host ""
}

# -----------------------------
# 7. Remove duplicate project LICENSE
# -----------------------------
if ($DeleteProjectDuplicateLicense) {
    Write-Info "Checking duplicate LICENSE..."
    $RootLicense = Join-Path $RepoRoot "LICENSE"
    $ProjectLicense = Join-Path $ProjectRoot "LICENSE"

    if ((Test-Path $RootLicense) -and (Test-Path $ProjectLicense)) {
        Remove-Target -Path $ProjectLicense -Reason "Duplicate LICENSE inside project; keep repo-level copy"
    }
    else {
        Write-Info "No duplicate LICENSE cleanup needed."
    }
    Write-Host ""
}

# -----------------------------
# Completion
# -----------------------------
Add-Content -Path $LogFile -Value ""
Add-Content -Path $LogFile -Value "Cleanup finished: $(Get-Date)"

if ($PreviewOnly) {
    Write-Warn "Preview only. Nothing was deleted."
}
else {
    Write-Ok "Cleanup complete."
}

Write-Host ""
Write-Host "Log file: $LogFile" -ForegroundColor White
Write-Host ""

Write-Host "Next steps:" -ForegroundColor White
Write-Host "1. Review the log"
Write-Host "2. If preview looks correct, set `$PreviewOnly = `$false"
Write-Host "3. Run the script again"
Write-Host "4. Then run:"
Write-Host '   git -C "D:\Applications\Fusion_Release_4\Fusion_TSS" status'
Write-Host '   git -C "D:\Applications\Fusion_Release_4\Fusion_TSS" add -A'
Write-Host '   git -C "D:\Applications\Fusion_Release_4\Fusion_TSS" commit -m "Cleanup repo structure and remove junk"'