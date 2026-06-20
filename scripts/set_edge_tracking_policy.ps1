<#
set_edge_tracking_policy.ps1

PowerShell helper to create or update a Microsoft Edge user-data profile that disables Tracking Prevention for that profile only.

USAGE (Run as the same user that runs Edge):
  - To create/apply for a profile:
      .\set_edge_tracking_policy.ps1 -UserDataDir "$env:LOCALAPPDATA\Microsoft\Edge\User Data" -ProfileName "BotProfile" -Action apply

  - To remove (restore previous backup):
      .\set_edge_tracking_policy.ps1 -UserDataDir "$env:LOCALAPPDATA\Microsoft\Edge\User Data" -ProfileName "BotProfile" -Action remove

NOTES:
- This script does NOT change machine-level Group Policy. It only creates/updates the Preferences file under the specified profile directory.
- The script will back up the existing Preferences file before modifying it.
- After applying, launch Edge for the bot using the same UserDataDir and ProfileName. Example:
    "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" --user-data-dir="C:\Users\<you>\AppData\Local\Microsoft\Edge\User Data" --profile-directory="BotProfile"
- Run this script interactively (no admin required) as the user who owns the profile directory.
- If Edge is running using the same profile, prefer to close Edge before applying to ensure Preferences are persisted correctly.
#>
param(
    [string]$UserDataDir = "$env:LOCALAPPDATA\Microsoft\Edge\User Data",
    [string]$ProfileName = "BotProfile",
    [ValidateSet("apply","remove")][string]$Action = "apply"
)

function Write-Log {
    param([string]$msg)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$ts] $msg"
}

try {
    $profilePath = Join-Path -Path $UserDataDir -ChildPath $ProfileName
    if (-not (Test-Path $profilePath)) {
        Write-Log "Profile path does not exist, creating: $profilePath"
        New-Item -ItemType Directory -Path $profilePath -Force | Out-Null
    }

    $prefsPath = Join-Path -Path $profilePath -ChildPath "Preferences"
    if ($Action -eq 'remove') {
        # restore from backup if present
        $backup = "${prefsPath}.backup"
        if (Test-Path $backup) {
            Write-Log "Restoring Preferences from backup: $backup -> $prefsPath"
            Copy-Item -Path $backup -Destination $prefsPath -Force
            Write-Log "Restore complete. You may restart Edge (if running)."
        } else {
            Write-Log "No backup found at $backup. Nothing to restore."
        }
        return
    }

    # ensure parent exists
    $prefsExists = Test-Path $prefsPath
    if ($prefsExists) {
        $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $backupPath = "${prefsPath}.backup.${timestamp}"
        Write-Log "Backing up existing Preferences to: $backupPath"
        Copy-Item -Path $prefsPath -Destination $backupPath -Force
    }

    # build desired settings object (safe: only add keys we need)
    $desired = @{
        "privacy" = @{
            # best-effort flags: some keys may be ignored by Edge; these are non-destructive to other settings
            "tracking_protection" = @{
                "level" = "off";
                "enabled" = $false
            };
            # compatibility key
            "tracking_protection_level" = 0
        }
        # marker so we can detect that this profile was modified by the script
        "bot_helper" = @{
            "modified_by" = "set_edge_tracking_policy.ps1";
            "modified_at" = (Get-Date).ToString("o")
        }
    }

    $obj = $null
    if ($prefsExists) {
        try {
            $raw = Get-Content -Path $prefsPath -Raw -ErrorAction Stop
            if ($raw -and $raw.Trim().Length -gt 0) {
                $obj = $raw | ConvertFrom-Json -ErrorAction Stop
            }
        } catch {
            Write-Log "Warning: cannot parse existing Preferences JSON, continuing with a new Preferences object. Error: $_"
            $obj = $null
        }
    }

    if (-not $obj) {
        $obj = @{}
    }

    # merge desired into obj (non-destructive)
    if (-not $obj.ContainsKey('privacy')) { $obj['privacy'] = @{} }
    $obj['privacy']['tracking_protection'] = $desired['privacy']['tracking_protection']
    $obj['privacy']['tracking_protection_level'] = $desired['privacy']['tracking_protection_level']

    # attach marker
    $obj['bot_helper'] = $desired['bot_helper']

    # convert back to JSON with indentation
    $json = $obj | ConvertTo-Json -Depth 10

    # write Preferences atomically
    $tmp = "${prefsPath}.tmp"
    $json | Out-File -FilePath $tmp -Encoding UTF8 -Force
    Move-Item -Path $tmp -Destination $prefsPath -Force

    Write-Log "Applied tracking-prevention=off settings to profile: $profilePath"
    Write-Log "IMPORTANT: Start Edge with the same user-data-dir and profile-directory to use this profile. Example:"
    Write-Log "  \"C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe\" --user-data-dir=\"$UserDataDir\" --profile-directory=\"$ProfileName\""
    Write-Log "If Edge is running with the same profile, please restart Edge to ensure preferences are loaded."

} catch {
    Write-Log "ERROR: $_"
    exit 1
}
