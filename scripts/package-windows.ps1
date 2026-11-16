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
$frameTitle = 'Flink State Lens — 离线 Flink 状态分析'
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
    # ArgumentList quotes Windows process arguments; it cannot change the JDK launcher's code page.
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
          [string] $WorkingDirectory = $workspace, [switch] $WithoutJavaEnvironment)
    $start = New-ChildStartInfo $Executable $CommandArgs $WorkingDirectory -WithoutJavaEnvironment:$WithoutJavaEnvironment
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

function Set-Utf8LauncherManifest {
    param([string] $Executable)
    $kit = Get-ItemProperty -LiteralPath 'HKLM:\SOFTWARE\Microsoft\Windows Kits\Installed Roots' `
        -Name KitsRoot10 -ErrorAction SilentlyContinue
    $mt = $null
    if ($kit) {
        $sdkBin = Join-Path $kit.KitsRoot10 'bin'
        $mt = Get-ChildItem -LiteralPath $sdkBin -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match '^\d+\.\d+\.\d+\.\d+$' } |
            Sort-Object { [Version] $_.Name } -Descending |
            ForEach-Object { Join-Path $_.FullName 'x64/mt.exe' } |
            Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } |
            Select-Object -First 1
    }
    if (-not $mt) {
        $tool = Get-Command mt.exe -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($tool) { $mt = $tool.Source }
    }
    if (-not $mt) { throw 'Windows SDK manifest tool (mt.exe) is required to build the portable launcher.' }

    # jpackage's Windows native launcher converts JVM arguments using CP_ACP.
    # Set the process code page to UTF-8 (Windows 10 1903+) without replacing its other manifest settings.
    $manifestPath = Join-Path $workspace 'launcher.manifest'
    $null = Invoke-Checked $mt @('-nologo', "-inputresource:$Executable;#1", "-out:$manifestPath")
    $document = [Xml.XmlDocument]::new()
    $document.PreserveWhitespace = $true
    $document.Load($manifestPath)
    $namespaces = [Xml.XmlNamespaceManager]::new($document.NameTable)
    $assemblyNamespace = 'urn:schemas-microsoft-com:asm.v3'
    $codePageNamespace = 'http://schemas.microsoft.com/SMI/2019/WindowsSettings'
    $namespaces.AddNamespace('app', $assemblyNamespace)
    $namespaces.AddNamespace('settings', $codePageNamespace)
    $assembly = $document.DocumentElement
    $application = $assembly.SelectSingleNode('app:application', $namespaces)
    if (-not $application) {
        $application = $document.CreateElement('application', $assemblyNamespace)
        $null = $assembly.AppendChild($application)
    }
    $settings = $application.SelectSingleNode('app:windowsSettings', $namespaces)
    if (-not $settings) {
        $settings = $document.CreateElement('windowsSettings', $assemblyNamespace)
        $null = $application.AppendChild($settings)
    }
    $codePage = $settings.SelectSingleNode('settings:activeCodePage', $namespaces)
    if (-not $codePage) {
        $codePage = $document.CreateElement('activeCodePage', $codePageNamespace)
        $null = $settings.AppendChild($codePage)
    }
    $codePage.InnerText = 'UTF-8'
    $document.Save($manifestPath)
    $null = Invoke-Checked $mt @('-nologo', '-manifest', $manifestPath, "-outputresource:$Executable;#1")
}

function Test-SwingLauncher {
    param([string] $Executable, [string] $Metadata, [string] $WorkingDirectory)
    $start = New-ChildStartInfo $Executable @('--open', $Metadata) $WorkingDirectory -WithoutJavaEnvironment
    # jpackage reexecs itself when its app directory is not on PATH. Add only that
    # bundled directory so the started PID owns the Swing window we verify below.
    $start.Environment['PATH'] += ';' + (Join-Path ([IO.Path]::GetDirectoryName($Executable)) 'app')
    $process = [Diagnostics.Process]::Start($start)
    try {
        $clock = [Diagnostics.Stopwatch]::StartNew()
        do {
            $process.Refresh()
            if ($process.HasExited) { throw "Desktop launcher exited before opening a window (code $($process.ExitCode))." }
            if ($process.MainWindowHandle -ne [IntPtr]::Zero -and $process.MainWindowTitle -ceq $frameTitle) {
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
    Set-Utf8LauncherManifest (Join-Path $image "$appName.exe")
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../LICENSE') -Destination $image
    if (-not (Test-Path -LiteralPath (Join-Path $image 'runtime/legal/java.base/LICENSE') -PathType Leaf)) {
        throw 'The packaged Java runtime is missing its license notices.'
    }

    $stagedZip = Join-Path $workspace $zipName
    [IO.Compression.ZipFile]::CreateFromDirectory($image, $stagedZip, [IO.Compression.CompressionLevel]::Optimal, $true)
    # The regular java.exe launcher uses the system ANSI code page. Test its CLI with
    # relative, representable arguments; use the UTF-8 native launcher from a Chinese path below.
    $extracted = Join-Path $workspace 'Extracted app'
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
    $snapshot = Join-Path $workspace 'Example savepoint'
    $metadata = Join-Path $snapshot '_metadata'
    $portableJarArgument = [IO.Path]::GetRelativePath($portable, $bundledJar)
    $portableSnapshotArgument = [IO.Path]::GetRelativePath($portable, $snapshot)
    $portableMetadataArgument = [IO.Path]::GetRelativePath($portable, $metadata)
    $fixture = Invoke-Checked $bundledJava @('-Dorg.slf4j.simpleLogger.defaultLogLevel=warn', '-jar', $portableJarArgument,
        '--create-example', $portableSnapshotArgument) -WorkingDirectory $portable -WithoutJavaEnvironment
    Write-Host $fixture.TrimEnd()
    if (-not (Test-Path -LiteralPath $metadata -PathType Leaf)) { throw 'The example savepoint did not produce _metadata.' }

    Write-Host 'Checking the standalone JAR and packaged runtime against the generated snapshot.'
    $stagedJarArgument = [IO.Path]::GetRelativePath($workspace, $stagedJar)
    $stagedMetadataArgument = [IO.Path]::GetRelativePath($workspace, $metadata)
    $inspection = (Invoke-Checked $java @('-jar', $stagedJarArgument, '--inspect', $stagedMetadataArgument)) | ConvertFrom-Json
    $operators = @($inspection.operators)
    if ($operators.Count -ne 1 -or $operators[0].operatorId -notmatch '^[0-9a-f]{32}$') {
        throw 'The standalone JAR did not find the expected fixture operator.'
    }
    $operator = $operators[0].operatorId
    $preview = (Invoke-Checked $bundledJava @('-jar', $portableJarArgument, '--preview', $portableMetadataArgument,
        '--operator', $operator, '--state', 'order-map', '--key-contains', 'alice',
        '--map-key-contains', 'last') -WorkingDirectory $portable -WithoutJavaEnvironment) | ConvertFrom-Json
    $rows = @($preview.entries)
    if ($rows.Count -ne 1 -or $rows[0].value -ne '5' -or -not $preview.page.complete -or $preview.page.hasMore) {
        throw 'Bundled preview did not decode the expected filtered MapState entry.'
    }
    $query = (Invoke-Checked $bundledJava @('-Dorg.slf4j.simpleLogger.defaultLogLevel=warn', '-jar', $portableJarArgument,
        '--query', $portableMetadataArgument, '--operator', $operator, '--state', 'order-count', '--kind', 'VALUE',
        '--key-type', 'STRING', '--value-type', 'LONG') -WorkingDirectory $portable -WithoutJavaEnvironment) | ConvertFrom-Json
    $alice = @($query.rows | Where-Object { $_.key -eq '"alice"' })
    if ($query.truncated -or @($query.rows).Count -ne 2 -or $alice.Count -ne 1 -or $alice[0].value -ne '12') {
        throw 'Bundled Flink did not restore the expected RocksDB ValueState.'
    }

    Write-Host 'Checking the packaged graphical launcher from a Chinese path without a system Java on PATH.'
    $unicodeExtracted = Join-Path $workspace 'Extracted app 测试'
    [IO.Directory]::Move($extracted, $unicodeExtracted)
    $unicodePortable = Join-Path $unicodeExtracted $appName
    $unicodeLauncher = Join-Path $unicodePortable "$appName.exe"
    Test-SwingLauncher $unicodeLauncher ([IO.Path]::GetRelativePath($unicodePortable, $metadata)) $unicodePortable
    $null = [IO.Directory]::CreateDirectory($out)
    Move-Item -LiteralPath $stagedZip -Destination $publishedZip
    Write-Host "Verified Windows archive: $publishedZip"
} finally {
    Remove-Item -LiteralPath $workspace -Recurse -Force
}
