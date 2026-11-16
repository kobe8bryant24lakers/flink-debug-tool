#Requires -Version 7.0
<#
.SYNOPSIS
Build and smoke-test the portable Windows x64 app using the JDK in JAVA_HOME.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $JarPath,
    [Parameter(Mandatory)] [string] $OutputDirectory,
    [Parameter(Mandatory)] [ValidatePattern('^[0-9]+\.[0-9]+\.[0-9]+$')] [string] $Version
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $IsWindows -or [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne 'X64') {
    throw 'Run this script on Windows x64.'
}
if (-not $env:JAVA_HOME) { throw 'JAVA_HOME must point to a Windows x64 JDK 21.' }

$appName = 'Flink State Lens'
$jarName = 'flink-debug-tool-desktop.jar'
$zipName = "flink-debug-tool-$Version-windows-x64.zip"
$originalJar = (Get-Item -LiteralPath $JarPath).FullName
$java = Join-Path $env:JAVA_HOME 'bin/java.exe'
$jpackage = Join-Path $env:JAVA_HOME 'bin/jpackage.exe'
if (-not (Test-Path -LiteralPath $originalJar -PathType Leaf) -or
    -not (Test-Path -LiteralPath $jpackage -PathType Leaf) -or
    -not (Test-Path -LiteralPath $java -PathType Leaf)) {
    throw 'Supply a shaded desktop JAR and a JDK with java.exe and jpackage.exe.'
}
$out = [IO.Path]::GetFullPath($OutputDirectory, $PWD.ProviderPath)
$publishedZip = Join-Path $out $zipName
if (Test-Path -LiteralPath $publishedZip) { throw "Archive already exists: $publishedZip" }

function New-ChildStartInfo {
    param([string] $Executable, [string[]] $CommandArgs, [string] $WorkingDirectory,
          [switch] $WithoutJavaEnvironment)
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $Executable
    $start.WorkingDirectory = $WorkingDirectory
    $start.UseShellExecute = $false
    # ArgumentList handles quoting for the native Windows command line, including spaces and Unicode.
    foreach ($argument in $CommandArgs) { $start.ArgumentList.Add($argument) }
    if ($WithoutJavaEnvironment) {
        foreach ($variable in @('JAVA_HOME', 'JDK_HOME', 'JRE_HOME', 'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS', 'CLASSPATH')) {
            $null = $start.Environment.Remove($variable)
        }
        $start.Environment['PATH'] = "$env:SystemRoot\System32;$env:SystemRoot"
    }
    return $start
}

function Stop-ChildProcess {
    param([Diagnostics.Process] $Process)
    try {
        if (-not $Process.HasExited) {
            try {
                $Process.Kill($true)
            } catch [InvalidOperationException] {
                if (-not $Process.HasExited) { throw }
            }
            if (-not $Process.WaitForExit(10000)) { throw "Process $($Process.Id) did not stop." }
        }
    } finally {
        $Process.Dispose()
    }
}

function Invoke-Checked {
    param([string] $Executable, [string[]] $CommandArgs, [int] $TimeoutSeconds = 120,
          [switch] $WithoutJavaEnvironment)
    $start = New-ChildStartInfo $Executable $CommandArgs $workspace -WithoutJavaEnvironment:$WithoutJavaEnvironment
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.StandardOutputEncoding = [Text.Encoding]::UTF8
    $start.StandardErrorEncoding = [Text.Encoding]::UTF8
    $process = [Diagnostics.Process]::Start($start)
    try {
        # Drain both streams concurrently so a verbose JVM cannot fill a pipe and stall.
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            throw "$([IO.Path]::GetFileName($Executable)) timed out after $TimeoutSeconds seconds."
        }
        if (-not [Threading.Tasks.Task]::WaitAll([Threading.Tasks.Task[]] @($stdout, $stderr), 10000)) {
            throw "Could not collect output from $Executable."
        }
        $errors = $stderr.GetAwaiter().GetResult()
        $result = $stdout.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0) {
            throw "$([IO.Path]::GetFileName($Executable)) exited with code $($process.ExitCode).`n$errors`n$result"
        }
        if ($errors) { Write-Host $errors.TrimEnd() }
        return $result
    } finally {
        Stop-ChildProcess $process
    }
}

function Test-SwingLauncher {
    param([string] $Executable, [string] $Metadata)
    $start = New-ChildStartInfo $Executable @('--open', $Metadata) $workspace -WithoutJavaEnvironment
    $process = [Diagnostics.Process]::Start($start)
    try {
        $clock = [Diagnostics.Stopwatch]::StartNew()
        do {
            $process.Refresh()
            if ($process.HasExited) { throw "Desktop launcher exited before opening a window (code $($process.ExitCode))." }
            if ($process.MainWindowHandle -ne [IntPtr]::Zero -and $process.MainWindowTitle.StartsWith($appName)) {
                Write-Host "Visible Windows desktop window: $($process.MainWindowTitle)"
                return
            }
            Start-Sleep -Milliseconds 250
        } while ($clock.Elapsed.TotalSeconds -lt 45)
        throw "No '$appName' window from the packaged launcher within 45 seconds (last title: '$($process.MainWindowTitle)')."
    } finally {
        # Swing exits via the normal close path when possible; never leave a CI process running.
        try {
            if (-not $process.HasExited -and $process.CloseMainWindow()) { $null = $process.WaitForExit(5000) }
        } finally {
            Stop-ChildProcess $process
        }
    }
}

$workspace = [IO.Directory]::CreateDirectory((Join-Path ([IO.Path]::GetTempPath()) "flink-pack-$([Guid]::NewGuid().ToString('N').Substring(0, 12))")).FullName
try {
    # Use the JDK release file for an exact, locale-independent check.
    $jdkRelease = Get-Content -LiteralPath (Join-Path $env:JAVA_HOME 'release') -Raw
    if ($jdkRelease -notmatch '(?m)^JAVA_VERSION="21(?:\.[^"]*)?"\r?$' -or $jdkRelease -notmatch '(?m)^OS_ARCH="(?:amd64|x86_64)"\r?$') {
        throw 'The package requires a Windows x64 JDK 21.'
    }

    $inputDirectory = [IO.Directory]::CreateDirectory((Join-Path $workspace 'input')).FullName
    $nativeDirectory = [IO.Directory]::CreateDirectory((Join-Path $workspace 'image')).FullName
    $stagedJar = Join-Path $inputDirectory $jarName
    Copy-Item -LiteralPath $originalJar -Destination $stagedJar

    Write-Host 'Building native Windows app image and bundled JDK runtime.'
    $null = Invoke-Checked $jpackage @(
        '--type', 'app-image', '--name', $appName, '--app-version', $Version,
        '--input', $inputDirectory, '--dest', $nativeDirectory,
        '--main-jar', $jarName, '--main-class', 'dev.flinkdebug.Main',
        '--java-options', '--add-opens=java.base/java.util=ALL-UNNAMED',
        '--add-modules', 'ALL-MODULE-PATH',
        '--jlink-options', '--strip-debug --no-man-pages --no-header-files'
    ) -TimeoutSeconds 300

    $image = Join-Path $nativeDirectory $appName
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../LICENSE') -Destination $image
    if (-not (Test-Path -LiteralPath (Join-Path $image 'runtime/legal/java.base/LICENSE') -PathType Leaf)) {
        throw 'The packaged Java runtime is missing its license notices.'
    }

    $stagedZip = Join-Path $workspace $zipName
    [IO.Compression.ZipFile]::CreateFromDirectory($image, $stagedZip, [IO.Compression.CompressionLevel]::Optimal, $true)
    # Test the ZIP recipients will use, from a different path containing spaces and non-ASCII characters.
    $extracted = Join-Path $workspace 'Extracted app 测试'
    [IO.Compression.ZipFile]::ExtractToDirectory($stagedZip, $extracted)
    $portable = Join-Path $extracted $appName
    $launcher = Join-Path $portable "$appName.exe"
    $bundledJava = Join-Path $portable 'runtime/bin/java.exe'
    $bundledJar = Join-Path $portable "app/$jarName"
    foreach ($file in @($launcher, $bundledJava, $bundledJar)) {
        if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { throw "Packaged file is missing: $file" }
    }
    if ((Get-FileHash -LiteralPath $originalJar).Hash -ne (Get-FileHash -LiteralPath $bundledJar).Hash) {
        throw 'The packaged JAR does not match the shaded Maven artifact.'
    }

    Write-Host 'Creating a real Flink savepoint with bundled Java and RocksDB.'
    $snapshot = Join-Path $workspace 'Example savepoint 测试'
    $metadata = Join-Path $snapshot '_metadata'
    $fixture = Invoke-Checked $bundledJava @('-Dorg.slf4j.simpleLogger.defaultLogLevel=warn', '-jar', $bundledJar,
        '--create-example', $snapshot) -WithoutJavaEnvironment
    Write-Host $fixture.TrimEnd()
    if (-not (Test-Path -LiteralPath $metadata -PathType Leaf)) { throw 'The example savepoint did not produce _metadata.' }

    Write-Host 'Checking the standalone JAR and packaged runtime against the generated snapshot.'
    $inspection = (Invoke-Checked $java @('-jar', $originalJar, '--inspect', $metadata)) | ConvertFrom-Json
    $operators = @($inspection.operators)
    if ($operators.Count -ne 1 -or $operators[0].operatorId -notmatch '^[0-9a-f]{32}$') {
        throw 'The standalone JAR did not find the expected fixture operator.'
    }
    $operator = $operators[0].operatorId
    $preview = (Invoke-Checked $bundledJava @('-jar', $bundledJar, '--preview', $metadata,
        '--operator', $operator, '--state', 'order-map', '--key-contains', 'alice',
        '--map-key-contains', 'last') -WithoutJavaEnvironment) | ConvertFrom-Json
    $rows = @($preview.entries)
    if ($rows.Count -ne 1 -or $rows[0].value -ne '5' -or -not $preview.page.complete -or $preview.page.hasMore) {
        throw 'Bundled preview did not decode the expected filtered MapState entry.'
    }
    $query = (Invoke-Checked $bundledJava @('-Dorg.slf4j.simpleLogger.defaultLogLevel=warn', '-jar', $bundledJar,
        '--query', $metadata, '--operator', $operator, '--state', 'order-count', '--kind', 'VALUE',
        '--key-type', 'STRING', '--value-type', 'LONG') -WithoutJavaEnvironment) | ConvertFrom-Json
    $alice = @($query.rows | Where-Object { $_.key -eq '"alice"' })
    if ($query.truncated -or @($query.rows).Count -ne 2 -or $alice.Count -ne 1 -or $alice[0].value -ne '12') {
        throw 'Bundled Flink did not restore the expected RocksDB ValueState.'
    }

    Write-Host 'Checking the packaged graphical launcher without a system Java on PATH.'
    Test-SwingLauncher $launcher $metadata
    $null = [IO.Directory]::CreateDirectory($out)
    Move-Item -LiteralPath $stagedZip -Destination $publishedZip
    Write-Host "Verified Windows archive: $publishedZip"
} finally {
    Remove-Item -LiteralPath $workspace -Recurse -Force
}
