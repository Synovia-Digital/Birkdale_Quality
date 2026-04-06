param(
    [string]$ProjectFilePath = "\\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4\Synoiva_TSS\TSS_BRK_QAS\FLow_Birkdale_QAS\FLow_Birkdale_QAS.pyproj",
    [string]$LocalRepoPath   = "D:\Applications\Fusion_Release_4\Fusion_TSS",
    [string]$ExpectedRemote  = "https://github.com/Synovia-Digital/Birkdale_Quality.git",
    [string]$LogFolder       = "D:\Applications\Fusion_Release_4\Logs"
)

# -----------------------------
# Setup
# -----------------------------
$ErrorActionPreference = "Continue"

if (-not (Test-Path $LogFolder)) {
    New-Item -ItemType Directory -Path $LogFolder -Force | Out-Null
}

$TimeStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = Join-Path $LogFolder "GitProjectCheck_$TimeStamp.log"

function Write-Log {
    param([string]$Message)

    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
}

function Write-Section {
    param([string]$Title)

    $sep = "=" * 80
    Write-Log $sep
    Write-Log $Title
    Write-Log $sep
}

function Run-Git {
    param(
        [string]$RepoPath,
        [string]$Arguments
    )

    Write-Log "Running: git $Arguments"
    try {
        $output = & git -C $RepoPath $Arguments 2>&1
        if ($null -eq $output -or $output.Count -eq 0) {
            Write-Log "(no output)"
        }
        else {
            $output | ForEach-Object { Write-Log $_ }
        }
    }
    catch {
        Write-Log "ERROR running git $Arguments : $($_.Exception.Message)"
    }
}

function Safe-ResolvePath {
    param([string]$PathToResolve)

    try {
        if (Test-Path $PathToResolve) {
            return (Resolve-Path $PathToResolve).Path
        }
    }
    catch { }

    return $null
}

# -----------------------------
# Start
# -----------------------------
Write-Section "STARTING GIT / PROJECT DIAGNOSTIC"
Write-Log "ProjectFilePath : $ProjectFilePath"
Write-Log "LocalRepoPath   : $LocalRepoPath"
Write-Log "ExpectedRemote  : $ExpectedRemote"
Write-Log "LogFile         : $LogFile"

# -----------------------------
# Basic path checks
# -----------------------------
Write-Section "PATH CHECKS"

$ProjectExists = Test-Path $ProjectFilePath
$RepoExists    = Test-Path $LocalRepoPath

Write-Log "Project file exists? $ProjectExists"
Write-Log "Local repo path exists? $RepoExists"

if ($ProjectExists) {
    $ResolvedProjectFile = Safe-ResolvePath $ProjectFilePath
    $ProjectFolder = Split-Path $ResolvedProjectFile -Parent
    Write-Log "Resolved project file : $ResolvedProjectFile"
    Write-Log "Project folder        : $ProjectFolder"
}
else {
    Write-Log "WARNING: Project file not found."
    $ProjectFolder = $null
}

if ($RepoExists) {
    $ResolvedRepoPath = Safe-ResolvePath $LocalRepoPath
    Write-Log "Resolved repo path    : $ResolvedRepoPath"
}
else {
    Write-Log "WARNING: Local repo path not found."
    $ResolvedRepoPath = $null
}

# -----------------------------
# Git installed?
# -----------------------------
Write-Section "GIT AVAILABILITY CHECK"

try {
    $gitVersion = git --version 2>&1
    Write-Log $gitVersion
}
catch {
    Write-Log "ERROR: Git does not appear to be installed or is not in PATH."
}

# -----------------------------
# Is repo really a git repo?
# -----------------------------
Write-Section "LOCAL REPO CHECK"

if ($RepoExists) {
    $GitFolder = Join-Path $LocalRepoPath ".git"
    $GitFolderExists = Test-Path $GitFolder
    Write-Log ".git folder exists? $GitFolderExists"

    Run-Git -RepoPath $LocalRepoPath -Arguments "rev-parse --is-inside-work-tree"
    Run-Git -RepoPath $LocalRepoPath -Arguments "rev-parse --show-toplevel"
    Run-Git -RepoPath $LocalRepoPath -Arguments "remote -v"
    Run-Git -RepoPath $LocalRepoPath -Arguments "branch --show-current"
    Run-Git -RepoPath $LocalRepoPath -Arguments "status --short"
    Run-Git -RepoPath $LocalRepoPath -Arguments "status"
}
else {
    Write-Log "Skipping git checks because repo path does not exist."
}

# -----------------------------
# Remote validation
# -----------------------------
Write-Section "REMOTE VALIDATION"

if ($RepoExists) {
    try {
        $remoteUrl = (& git -C $LocalRepoPath remote get-url origin 2>$null)
        if ($LASTEXITCODE -eq 0 -and $remoteUrl) {
            Write-Log "Origin remote URL: $remoteUrl"

            if ($remoteUrl.Trim().ToLower() -eq $ExpectedRemote.Trim().ToLower()) {
                Write-Log "OK: origin matches expected remote."
            }
            else {
                Write-Log "WARNING: origin does NOT match expected remote."
            }
        }
        else {
            Write-Log "WARNING: No origin remote found."
        }
    }
    catch {
        Write-Log "ERROR checking origin remote: $($_.Exception.Message)"
    }
}

# -----------------------------
# Project file relation to repo
# -----------------------------
Write-Section "PROJECT VS REPO RELATIONSHIP"

if ($ProjectFolder -and $ResolvedRepoPath) {
    $ProjectFolderNormalized = [System.IO.Path]::GetFullPath($ProjectFolder)
    $RepoPathNormalized      = [System.IO.Path]::GetFullPath($ResolvedRepoPath)

    Write-Log "Normalized project folder : $ProjectFolderNormalized"
    Write-Log "Normalized repo path      : $RepoPathNormalized"

    if ($ProjectFolderNormalized.StartsWith($RepoPathNormalized, [System.StringComparison]::OrdinalIgnoreCase)) {
        Write-Log "OK: Project folder is INSIDE the Git repo."
    }
    elseif ($RepoPathNormalized.StartsWith($ProjectFolderNormalized, [System.StringComparison]::OrdinalIgnoreCase)) {
        Write-Log "INFO: Repo folder is inside project folder."
    }
    else {
        Write-Log "WARNING: Project folder and repo folder are SEPARATE."
        Write-Log "This is likely the main problem."
    }
}
else {
    Write-Log "Could not compare project folder to repo path."
}

# -----------------------------
# File inventory
# -----------------------------
Write-Section "FILE INVENTORY"

if ($ProjectFolder -and (Test-Path $ProjectFolder)) {
    Write-Log "Project folder file summary:"
    Get-ChildItem -Path $ProjectFolder -Recurse -File -ErrorAction SilentlyContinue |
        Select-Object FullName, Length, LastWriteTime |
        Sort-Object FullName |
        ForEach-Object {
            Write-Log ("PROJECT FILE | {0} | {1} bytes | {2}" -f $_.FullName, $_.Length, $_.LastWriteTime)
        }
}

if ($ResolvedRepoPath -and (Test-Path $ResolvedRepoPath)) {
    Write-Log "Repo folder file summary:"
    Get-ChildItem -Path $ResolvedRepoPath -Recurse -File -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -notmatch '\\.git\\' } |
        Select-Object FullName, Length, LastWriteTime |
        Sort-Object FullName |
        ForEach-Object {
            Write-Log ("REPO FILE    | {0} | {1} bytes | {2}" -f $_.FullName, $_.Length, $_.LastWriteTime)
        }
}

# -----------------------------
# Top-level comparison
# -----------------------------
Write-Section "TOP-LEVEL COMPARISON"

if ($ProjectFolder -and $ResolvedRepoPath -and (Test-Path $ProjectFolder) -and (Test-Path $ResolvedRepoPath)) {
    $projectTop = Get-ChildItem -Path $ProjectFolder -Force -ErrorAction SilentlyContinue |
                  Select-Object Name, FullName, PSIsContainer

    $repoTop = Get-ChildItem -Path $ResolvedRepoPath -Force -ErrorAction SilentlyContinue |
               Where-Object { $_.Name -ne ".git" } |
               Select-Object Name, FullName, PSIsContainer

    Write-Log "Top-level items in project folder:"
    $projectTop | Sort-Object Name | ForEach-Object {
        Write-Log ("PROJECT TOP | {0} | Folder={1}" -f $_.Name, $_.PSIsContainer)
    }

    Write-Log "Top-level items in repo folder:"
    $repoTop | Sort-Object Name | ForEach-Object {
        Write-Log ("REPO TOP    | {0} | Folder={1}" -f $_.Name, $_.PSIsContainer)
    }
}

# -----------------------------
# .gitignore check
# -----------------------------
Write-Section ".GITIGNORE CHECK"

if ($ResolvedRepoPath) {
    $gitIgnorePath = Join-Path $ResolvedRepoPath ".gitignore"
    if (Test-Path $gitIgnorePath) {
        Write-Log ".gitignore found at $gitIgnorePath"
        Get-Content $gitIgnorePath | ForEach-Object { Write-Log ".gitignore | $_" }
    }
    else {
        Write-Log "WARNING: No .gitignore found."
    }
}

# -----------------------------
# Recent commit history
# -----------------------------
Write-Section "RECENT COMMITS"

if ($RepoExists) {
    Run-Git -RepoPath $LocalRepoPath -Arguments "log --oneline --decorate -n 10"
}

# -----------------------------
# Final summary
# -----------------------------
Write-Section "FINAL SUMMARY"

if (-not $ProjectExists) {
    Write-Log "Problem: Project file does not exist at the supplied path."
}

if (-not $RepoExists) {
    Write-Log "Problem: Local repo path does not exist."
}

if ($RepoExists) {
    $isGitRepo = $false
    try {
        $result = & git -C $LocalRepoPath rev-parse --is-inside-work-tree 2>$null
        if ($result -eq "true") { $isGitRepo = $true }
    }
    catch { }

    if (-not $isGitRepo) {
        Write-Log "Problem: Local folder is not a valid Git working tree."
    }
}

if ($ProjectFolder -and $ResolvedRepoPath) {
    $projectInsideRepo = $false
    try {
        $projectInsideRepo = [System.IO.Path]::GetFullPath($ProjectFolder).StartsWith(
            [System.IO.Path]::GetFullPath($ResolvedRepoPath),
            [System.StringComparison]::OrdinalIgnoreCase
        )
    }
    catch { }

    if (-not $projectInsideRepo) {
        Write-Log "Problem: The Visual Studio project is not inside the Git repo folder."
        Write-Log "Likely fix: move/copy the full project into the repo folder, then open THAT copy in Visual Studio."
    }
}

Write-Log "Diagnostic completed."
Write-Log "Review log file: $LogFile"#