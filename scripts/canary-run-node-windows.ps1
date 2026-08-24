<#
.SYNOPSIS
  Windows half of the auto-update canary's node runner (#5341).

.DESCRIPTION
  Starts the freenet binary under test, watches its log directory until the
  startup update check has produced an outcome (or the budget expires), stops
  it, and records what happened. It reaches NO verdict: `auto-update-canary.sh`
  reads the files this produces and decides. See the "PLATFORM SEAM" block in
  that file's `run_node_until_check` for the contract.

  WHY THIS EXISTS AT ALL. Gate A gates every release on the updater working,
  and until now it only ever ran the Linux musl x86_64 asset -- so a green gate
  said nothing about Windows, which is where a real updater break has already
  shipped (#3933/#3934, the FreeConsole()/invalid-handle class) and where the
  peers stranded by the 0.2.120 break actually are (~80% of that cohort).

  WHY IT IS NOT A SECOND CANARY. Every string it matches on is passed in from
  auto-update-canary.sh via the environment, and every assertion stays there.
  A Windows copy of the markers would rot silently the first time one of them
  is reworded -- the failure mode .claude/rules/bug-prevention-patterns.md
  describes for this canary's pins -- and a Windows copy of the assertions
  would drift from the ones the fixture tests drive. This file has neither.

  WHAT IT DOES NOT DO, stated because a reader will otherwise assume it:
  it does not isolate the node's update state directory. `state_dir()`
  (crates/core/src/bin/commands/auto_update.rs) is `dirs::home_dir()/.local/
  state/freenet`, and on Windows `dirs` 6 resolves the home directory through
  SHGetKnownFolderPath, which NO environment variable overrides -- so the
  Linux runner's isolated-HOME trick has no Windows equivalent. On a hosted
  runner that is harmless (fresh VM, nothing else has written there). Run it on
  a developer's own Windows box and the canary node shares the poll bucket and
  backoff state of the real node on that machine, so it is warned about below
  rather than silently done.

.PARAMETER Binary
  Windows path to the freenet.exe under test.

.PARAMETER WorkDir
  Windows path to the canary workdir. `logs`, `cfg`, `data` and `tmp` already
  exist inside it (auto-update-canary.sh creates them).
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Binary,
    [Parameter(Mandatory = $true)][string]$WorkDir
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Fail-Harness([string]$Message) {
    # `::error::` so it lands in the GitHub run summary rather than only in the
    # log. A harness failure blocks a release, so it must be findable.
    Write-Host "::error::canary-run-node-windows: $Message"
    exit 1
}

# --- inputs from auto-update-canary.sh --------------------------------------
# Required, and empty is refused rather than defaulted. A default here would be
# a second copy of the marker with extra steps: the moment the canary's own
# constant were reworded, this file would keep matching the old text and the
# poll loop would silently run to the full timeout on every healthy node --
# slow, and eventually "fixed" by shortening the wait, which is exactly the
# #5236 vacuous pass.
function Get-RequiredEnv([string]$Name) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        Fail-Harness "$Name is not set. It is exported by auto-update-canary.sh's run_node_until_check; this script must not supply its own default, or it would silently keep matching a marker the canary has since reworded."
    }
    return $value
}

function Get-RequiredIntEnv([string]$Name) {
    $raw = Get-RequiredEnv $Name
    [int]$parsed = 0
    if (-not [int]::TryParse($raw, [ref]$parsed) -or $parsed -lt 1) {
        Fail-Harness "$Name is '$raw', which is not a positive integer."
    }
    return $parsed
}

$MarkerCheckRan      = Get-RequiredEnv 'CANARY_MARKER_CHECK_RAN'
$MarkerCheckComplete = Get-RequiredEnv 'CANARY_MARKER_CHECK_COMPLETE'
$MarkerTriggeredRe   = Get-RequiredEnv 'CANARY_MARKER_TRIGGERED_RE'
$MarkerNotTriggered  = Get-RequiredEnv 'CANARY_MARKER_NOT_TRIGGERED'
$TimeoutSecs         = Get-RequiredIntEnv 'CANARY_TIMEOUT_SECS'
$OutcomeWaitSecs     = Get-RequiredIntEnv 'CANARY_OUTCOME_WAIT_SECS'
$NetworkPort         = Get-RequiredIntEnv 'CANARY_NETWORK_PORT'
$WsPort              = Get-RequiredIntEnv 'CANARY_WS_PORT'

if (-not (Test-Path -LiteralPath $Binary -PathType Leaf)) {
    Fail-Harness "the binary under test does not exist at '$Binary'."
}
if (-not (Test-Path -LiteralPath $WorkDir -PathType Container)) {
    Fail-Harness "the workdir does not exist at '$WorkDir'."
}

$LogDir    = Join-Path $WorkDir 'logs'
$CfgDir    = Join-Path $WorkDir 'cfg'
$DataDir   = Join-Path $WorkDir 'data'
$TmpDir    = Join-Path $WorkDir 'tmp'
$CacheDir  = Join-Path $WorkDir 'cache'
$StdoutLog = Join-Path $WorkDir 'node.stdout'
$StderrLog = Join-Path $WorkDir 'node.stderr'
$NodeOut   = Join-Path $WorkDir 'node.out'
$ExitFile  = Join-Path $WorkDir 'node.exit'

foreach ($d in @($LogDir, $CfgDir, $DataDir, $TmpDir, $CacheDir)) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

# --- log predicates ---------------------------------------------------------
# The node holds its log file open while it runs. Rust's std opens files on
# Windows with FILE_SHARE_READ|WRITE|DELETE, so reading concurrently is fine;
# a transient failure is still treated as "no match yet" rather than as an
# error, because this loop's only job is to decide when to STOP WAITING. Every
# real assertion runs afterwards, in bash, against the same files.
function Get-LogText {
    $files = @(Get-ChildItem -LiteralPath $LogDir -Filter 'freenet.*.log' -File -ErrorAction SilentlyContinue)
    if ($files.Count -eq 0) { return '' }
    $parts = New-Object System.Collections.Generic.List[string]
    foreach ($f in $files) {
        try {
            $stream = [System.IO.File]::Open($f.FullName, [System.IO.FileMode]::Open,
                [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete)
            try {
                $reader = New-Object System.IO.StreamReader($stream)
                try { $parts.Add($reader.ReadToEnd()) } finally { $reader.Dispose() }
            } finally { $stream.Dispose() }
        } catch {
            # Unreadable this instant; try again on the next poll.
        }
    }
    return ($parts -join "`n")
}

function Test-CheckStarted([string]$Text) {
    return $Text.Contains($MarkerCheckRan)
}

# Mirrors `node_decided_to_update`: match the trigger REGEX, then subtract the
# refusal, which shares the phrase. Both halves come from the canary's own
# constants, so the subtlety documented there (a fixed substring misses
# "triggering immediate auto-update") cannot be lost in translation here.
function Test-DecidedToUpdate([string]$Text) {
    foreach ($line in ($Text -split "`n")) {
        if ($line -match $MarkerTriggeredRe -and -not $line.Contains($MarkerNotTriggered)) {
            return $true
        }
    }
    return $false
}

# Mirrors `node_check_settled`: a completion line OR a trigger.
function Test-CheckSettled([string]$Text) {
    if ($Text.Contains($MarkerCheckComplete)) { return $true }
    return (Test-DecidedToUpdate $Text)
}

# --- environment for the node ----------------------------------------------
# FREENET_SUPERVISED: tell the node a supervisor is present, exactly as the
# Windows wrapper does, so it takes the real exit-42 path instead of logging a
# "no supervisor" error and staying put. Without it Gate A's exit-code observer
# -- the half that catches an inverted comparator when the trigger line's
# wording has drifted -- can never fire.
$env:FREENET_SUPERVISED = '1'
# Release builds rate-limit the log. A dropped line is indistinguishable from
# one never emitted, and the directions are not symmetric: losing the parse
# WARN while the completion line survives leaves every negative check satisfied
# and the gate green on a broken binary. Same reasoning as the Linux runner.
$env:FREENET_DISABLE_LOG_RATE_LIMIT = '1'
# `client_api.rs` unconditionally create_dir_all's `temp_dir()/freenet/webs`,
# which on Windows resolves through TMP/TEMP. That directory is vestigial, but
# a failed mkdir panics the node before the update task spawns -- which is
# exactly how a healthy v0.2.124 binary was reported as having no update check
# at all (#5290). Scope it into the workdir so nothing outside can decide the
# verdict.
$env:TMP  = $TmpDir
$env:TEMP = $TmpDir
# The webapp cache does not follow --data-dir; the explicit override is what
# keeps it inside the workdir.
$env:FREENET_WEBAPP_CACHE_DIR = (Join-Path $CacheDir 'freenet-webapp')

# Honest about the one thing that is NOT isolated. See the .DESCRIPTION block.
$sharedState = Join-Path ([Environment]::GetFolderPath('UserProfile')) '.local\state\freenet'
Write-Host "canary-run-node-windows: NOTE - the node's update state dir is NOT isolated on Windows ($sharedState). dirs::home_dir() resolves via SHGetKnownFolderPath and no environment variable overrides it. Harmless on a hosted runner; on a developer machine this canary shares the real node's poll bucket and backoff state."

Write-Host "canary-run-node-windows: starting $Binary (network=$NetworkPort ws=$WsPort, budget ${TimeoutSecs}s)"

$argList = @(
    'network',
    '--config-dir', $CfgDir,
    '--data-dir', $DataDir,
    '--log-dir', $LogDir,
    '--network-port', "$NetworkPort",
    '--ws-api-port', "$WsPort"
)

# `Start-Process -PassThru` rather than the call operator, because this needs a
# real Process handle: HasExited, ExitCode, and a kill that works. This is the
# whole reason Windows gets a runner instead of a port of the Linux path --
# there, `set -m` + `exec timeout` + a process-GROUP kill are load-bearing, and
# Git Bash has no reliable equivalent over a native .exe. A `wait` on a process
# a group kill did not reach hangs until the job timeout, and a blocking
# release gate that hangs is worse than one that is absent.
try {
    $proc = Start-Process -FilePath $Binary -ArgumentList $argList `
        -RedirectStandardOutput $StdoutLog -RedirectStandardError $StderrLog `
        -NoNewWindow -PassThru
} catch {
    Fail-Harness "could not start '$Binary': $($_.Exception.Message)"
}

# node.out is what `node_exited_on_fatal_abort` greps, and it must exist on
# EVERY exit path -- including the ones below that return early. Building it in
# a finally block is the only way that holds.
function Write-NodeOut {
    $combined = New-Object System.Collections.Generic.List[string]
    foreach ($f in @($StdoutLog, $StderrLog)) {
        if (Test-Path -LiteralPath $f) {
            try { $combined.Add([System.IO.File]::ReadAllText($f)) } catch { }
        }
    }
    Set-Content -LiteralPath $NodeOut -Value ($combined -join "`n") -Encoding UTF8 -NoNewline
}

$nodeExit = $null
try {
    $deadline = (Get-Date).AddSeconds($TimeoutSecs)
    while ((Get-Date) -lt $deadline) {
        if ($proc.HasExited) { break }
        $text = Get-LogText
        if (Test-CheckStarted $text) {
            # The check has STARTED. Wait for it to FINISH -- never a fixed
            # short sleep. The marker above is logged BEFORE the network
            # request, which is bounded by PROBE_CHAIN_TIMEOUT (10s), so
            # stopping the node before then reads the resulting silence as
            # health: the #5236 vacuous pass, on a binary carrying the exact
            # bug this gate exists to catch.
            $outcomeDeadline = (Get-Date).AddSeconds($OutcomeWaitSecs)
            if ($outcomeDeadline -gt $deadline) { $outcomeDeadline = $deadline }
            while ((Get-Date) -lt $outcomeDeadline) {
                $text = Get-LogText
                if (Test-CheckSettled $text) { break }
                if ($proc.HasExited) { break }
                Start-Sleep -Seconds 1
            }
            # A node that decided to update exits 42 on its own. Killing it here
            # would replace that with the forced-termination code and silently
            # defeat the exit-code observer -- the gate would report "no update
            # requested" for a node that requested one.
            if (Test-DecidedToUpdate (Get-LogText)) {
                $settleDeadline = (Get-Date).AddSeconds(60)
                if ($settleDeadline -gt $deadline) { $settleDeadline = $deadline }
                while (-not $proc.HasExited -and (Get-Date) -lt $settleDeadline) {
                    Start-Sleep -Seconds 2
                }
            }
            break
        }
        Start-Sleep -Seconds 3
    }

    if (-not $proc.HasExited) {
        # /T so the whole tree goes: the node spawns children, and one left
        # holding the UDP or WS port makes the NEXT attempt exit 43 and be
        # diagnosed as a port collision that this run caused.
        Write-Host "canary-run-node-windows: stopping the node (pid $($proc.Id))"
        & taskkill.exe /PID $proc.Id /T /F 2>&1 | ForEach-Object { Write-Host $_ }
        if (-not $proc.WaitForExit(30000)) {
            Fail-Harness "the node (pid $($proc.Id)) did not exit within 30s of taskkill /T /F. Refusing to report an exit code that was not observed."
        }
    }

    $nodeExit = $proc.ExitCode
} finally {
    Write-NodeOut
}

if ($null -eq $nodeExit) {
    Fail-Harness "did not observe a node exit code."
}

# The node's exit code goes in a FILE, never in this script's own exit status:
# with one channel, "the runner could not start the binary" and "the node
# exited 1" are the same number, and the gate would read a broken harness as a
# broken updater. See the PLATFORM SEAM block in auto-update-canary.sh.
Set-Content -LiteralPath $ExitFile -Value "$nodeExit" -Encoding ASCII
Write-Host "canary-run-node-windows: node exited with code $nodeExit"
exit 0
