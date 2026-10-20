<#
.SYNOPSIS
    Stage-0 trial on VM03: re-run trip generation for a sample of Kashiwa (12217)
    residents who visit Tokyo, with the same settings as ver3.0, and compare the
    mode shares with the ver3.0 output.

.DESCRIPTION
    Read-only for C:\Pseudo-PFLOW (ver3.0 environment). Everything is written under
    -OutRoot. Steps:
      1. copy the param-group files used for ver3.0 (pref12_*.properties and
         city_code_to_param_group.csv) into this working copy
      2. sample -N persons of 12217 who have an activity in Tokyo (muni 13xxx)
      3. build, then run TripGenerator_WebAPI_refactor against the local WebAPI
      4. report time per person and compare representative modes with ver3.0

    All new options (tour.enabled, car.extraCost.*) stay OFF. Only debug.costLog is ON
    (it writes the cost of every mode; it does not change any decision).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\windows\okuma_stage0_trial.ps1
    powershell -ExecutionPolicy Bypass -File .\scripts\windows\okuma_stage0_trial.ps1 -N 5000
    powershell -ExecutionPolicy Bypass -File .\scripts\windows\okuma_stage0_trial.ps1 -PublicApi

    -PublicApi uses the public WebAPI (pflow-api.csis.u-tokyo.ac.jp) instead of the
    local one, and writes to <OutRoot>_public. Keep it to small trials.
#>
param(
    [int]$N = 1000,
    [int]$Seed = 42,
    [int]$NumThreads = 4,
    [string]$City = "12217",
    [string]$OutRoot = "C:\okuma\out\stage0_trial",
    [string]$Ver3Root = "C:\Pseudo-PFLOW",
    [switch]$SkipBuild,
    [switch]$PublicApi
)

$ErrorActionPreference = "Stop"
$ApiBase = "http://localhost"
if ($PublicApi) {
    $ApiBase = "https://pflow-api.csis.u-tokyo.ac.jp"
    $OutRoot = $OutRoot + "_public"
}
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectDir = (Resolve-Path "$ScriptDir\..\..").Path
$Pref = $City.Substring(0, 2).TrimStart("0")
$Ver3Project = Join-Path $Ver3Root "Pseudo-PFLOW"
$Ver3Activity = Join-Path $Ver3Root "output\pref_$Pref\activity\$Pref\person_$City.csv"
$Ver3Trip = Join-Path $Ver3Root "output\pref_$Pref\trip\$Pref\trip_$City.csv"

function Step($msg) { Write-Host ""; Write-Host "== $msg" -ForegroundColor Cyan }

# ---------------------------------------------------------------- 0. checks
Step "0. checks"
if (-not $env:PFLOW_API_USER -or -not $env:PFLOW_API_PASS) {
    Write-Error "PFLOW_API_USER / PFLOW_API_PASS are not set."
}
foreach ($p in @($Ver3Project, $Ver3Activity, $Ver3Trip)) {
    if (-not (Test-Path $p)) { Write-Error "not found: $p" }
}
Write-Host "project : $ProjectDir"
Write-Host "ver3.0  : $Ver3Root (read only)"
Write-Host "output  : $OutRoot"
Write-Host "webapi  : $ApiBase"

# ---------------------------------------------------------------- 1. param groups
Step "1. copy ver3.0 param-group files"
$copies = @(
    @{ From = "data\tuning\city_code_to_param_group.csv"; To = "data\tuning" },
    @{ From = "config\tuning\param_groups\pref$($City.Substring(0,2))_*.properties"; To = "config\tuning\param_groups" }
)
foreach ($c in $copies) {
    $dst = Join-Path $ProjectDir $c.To
    New-Item -ItemType Directory -Path $dst -Force | Out-Null
    $src = Get-ChildItem -Path (Join-Path $Ver3Project $c.From)
    if (-not $src) { Write-Error "not found: $(Join-Path $Ver3Project $c.From)" }
    foreach ($f in $src) {
        Copy-Item $f.FullName $dst -Force
        Write-Host ("  {0}  sha256={1}" -f $f.Name, (Get-FileHash $f.FullName).Hash.Substring(0, 12))
    }
}
$group = (Select-String -Path (Join-Path $ProjectDir "data\tuning\city_code_to_param_group.csv") -Pattern "^$City,").Line
Write-Host "  mapping: $group"

# ---------------------------------------------------------------- 2. sample persons
Step "2. sample $N persons of $City who visit Tokyo (seed=$Seed)"
$tokyo = New-Object 'System.Collections.Generic.HashSet[string]'
$reader = New-Object System.IO.StreamReader($Ver3Activity)
$nAll = New-Object 'System.Collections.Generic.HashSet[string]'
while ($null -ne ($line = $reader.ReadLine())) {
    $c = $line.Split(",")
    [void]$nAll.Add($c[0])
    if ($c[9].StartsWith("13")) { [void]$tokyo.Add($c[0]) }
}
$reader.Close()
Write-Host ("  persons: {0}, visiting Tokyo: {1}" -f $nAll.Count, $tokyo.Count)

$rng = New-Object System.Random($Seed)
$sorted = $tokyo | Sort-Object { [long]$_ }
$sample = New-Object 'System.Collections.Generic.HashSet[string]'
$sorted | Sort-Object { $rng.Next() } | Select-Object -First $N | ForEach-Object { [void]$sample.Add($_) }

$actDir = Join-Path $OutRoot "activity\$Pref"
New-Item -ItemType Directory -Path $actDir -Force | Out-Null
$actOut = Join-Path $actDir "person_$City.csv"
$reader = New-Object System.IO.StreamReader($Ver3Activity)
$writer = New-Object System.IO.StreamWriter($actOut, $false, (New-Object System.Text.UTF8Encoding($false)))
$rows = 0
while ($null -ne ($line = $reader.ReadLine())) {
    if ($sample.Contains($line.Substring(0, $line.IndexOf(",")))) { $writer.WriteLine($line); $rows++ }
}
$reader.Close(); $writer.Close()
$sample | Sort-Object { [long]$_ } | Set-Content -Path (Join-Path $OutRoot "sample_pids.txt") -Encoding ASCII
Write-Host ("  wrote {0} persons / {1} activity rows -> {2}" -f $sample.Count, $rows, $actOut)

# ---------------------------------------------------------------- 3. build and run
$cfg = Join-Path $OutRoot "stage0.properties"
@(
    "outputDir=$($OutRoot.Replace('\','/'))/",
    "api.baseURL=$ApiBase",
    "debug.costLog=true"
) | Set-Content -Path $cfg -Encoding ASCII
$logDir = Join-Path $OutRoot "logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
if (-not $env:MAVEN_OPTS) { $env:MAVEN_OPTS = "-Xms2g -Xmx20g -XX:+UseG1GC" }

Push-Location $ProjectDir
try {
    $ErrorActionPreference = "Continue"
    if (-not $SkipBuild) {
        Step "3a. build"
        & mvn -q compile 2>&1 | Tee-Object -FilePath (Join-Path $logDir "build.log")
        if ($LASTEXITCODE -ne 0) { throw "build failed (see $logDir\build.log)" }
    }
    Step "3b. run trip generation ($N persons, $NumThreads threads)"
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & mvn -q exec:java `
        "-DnumThreads=$NumThreads" "-DbatchSize=5000" "-DforceTrip=true" "-DrouteCache.maxEntries=5000" `
        "-Dexec.mainClass=pseudo.gen.TripGenerator_WebAPI_refactor" `
        "-Dexec.args=$Pref 1" `
        "-Dconfig.file=$cfg" 2>&1 | Tee-Object -FilePath (Join-Path $logDir "trip.log")
    $sw.Stop()
    if ($LASTEXITCODE -ne 0) { throw "trip generation failed (see $logDir\trip.log)" }
} finally {
    $ErrorActionPreference = "Stop"
    Pop-Location
}

# ---------------------------------------------------------------- 4. report
Step "4. report"
$newTrip = Join-Path $OutRoot "trip\$Pref\trip_$City.csv"
if (-not (Test-Path $newTrip)) { Write-Error "no output: $newTrip" }

function ModeCounts($path, $pids) {
    # representative mode per trip (person id + trip id), col 0 / col 9 / col 11
    $seen = New-Object 'System.Collections.Generic.HashSet[string]'
    $cnt = @{}
    $r = New-Object System.IO.StreamReader($path)
    while ($null -ne ($line = $r.ReadLine())) {
        $c = $line.Split(",")
        if (-not $pids.Contains($c[0])) { continue }
        if (-not $seen.Add($c[0] + "_" + $c[9])) { continue }
        $cnt[$c[11]] = 1 + [int]$cnt[$c[11]]
    }
    $r.Close()
    return $cnt
}
$old = ModeCounts $Ver3Trip $sample
$new = ModeCounts $newTrip $sample
$label = @{ "0" = "stay"; "1" = "walk"; "2" = "bicycle"; "3" = "car"; "4" = "train"; "5" = "bus"; "6" = "mixed" }
$oldSum = ($old.Values | Measure-Object -Sum).Sum
$newSum = ($new.Values | Measure-Object -Sum).Sum

$sec = $sw.Elapsed.TotalSeconds
$out = New-Object System.Collections.Generic.List[string]
$out.Add(("time: {0:N0} s for {1} persons = {2:N2} s/person ({3} threads)" -f $sec, $sample.Count, ($sec / $sample.Count), $NumThreads))
$out.Add(("estimate for all {0} Tokyo visitors of {1}: {2:N1} h" -f $tokyo.Count, $City, ($sec / $sample.Count * $tokyo.Count / 3600)))
$out.Add("")
$out.Add(("{0,-8} {1,14} {2,14}" -f "mode", "ver3.0", "stage0"))
foreach ($k in (@($old.Keys) + @($new.Keys) | Sort-Object -Unique)) {
    $out.Add(("{0,-8} {1,6} ({2,5:N1}%) {3,6} ({4,5:N1}%)" -f $label[$k], [int]$old[$k], (100.0 * [int]$old[$k] / $oldSum), [int]$new[$k], (100.0 * [int]$new[$k] / $newSum)))
}
$out.Add(("{0,-8} {1,14} {2,14}" -f "trips", $oldSum, $newSum))
$out | ForEach-Object { Write-Host $_ }
$out | Set-Content -Path (Join-Path $OutRoot "summary.txt") -Encoding ASCII
Write-Host ""
Write-Host "summary : $(Join-Path $OutRoot 'summary.txt')" -ForegroundColor Green
Write-Host "costlog : $(Join-Path $OutRoot "costlog\$Pref\costlog_$City.csv")" -ForegroundColor Green
