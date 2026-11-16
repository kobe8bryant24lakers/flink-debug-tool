# Builds the Windows DLL for the exact FRocksDB Java bindings in Flink 1.20.5.
# Run with PowerShell 7 on Windows x64, with JDK 17, CMake, and VS 2022 C++ Build Tools.
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (!$IsWindows -or [Runtime.InteropServices.RuntimeInformation]::OSArchitecture -ne 'X64') {
    throw 'This script requires Windows x64.'
}

# 6.20.3-ververica-2.0 includes the fork's additional periodic-compaction JNI methods.
$frocksRevision = '030daed4a99132aba57eed1a65ca8d42fbe0c8f0'
$vcpkgRevision = '74e6536215718009aae747d86d84b78376bf9e09'
$codecs = @('snappy', 'lz4', 'zlib', 'zstd')
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$stage = Join-Path $root 'target/windows-native'
$dll = Join-Path $stage 'librocksdbjni-win64.dll'
$licenses = Join-Path $stage 'META-INF/windows-native-licenses'
$manifest = Join-Path $stage 'sha256.txt'
$scriptHash = (Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash
$noticeFiles = @('FRocksDB-LICENSE.Apache.txt', 'FRocksDB-COPYING.txt', 'sources.txt') + @($codecs | ForEach-Object { "$_.txt" })

function Invoke-Checked([string] $Executable, [string[]] $Arguments) {
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Executable failed with exit code $LASTEXITCODE." }
}

function Get-StageManifest {
    "script=$scriptHash"
    "$((Get-FileHash -LiteralPath $dll -Algorithm SHA256).Hash)  librocksdbjni-win64.dll"
    foreach ($notice in $noticeFiles) {
        "$((Get-FileHash -LiteralPath (Join-Path $licenses $notice) -Algorithm SHA256).Hash)  META-INF/windows-native-licenses/$notice"
    }
}

$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
if (!(Test-Path -LiteralPath $vswhere -PathType Leaf)) { throw 'Visual Studio Build Tools were not found.' }
$vsInstall = (Invoke-Checked $vswhere @('-latest', '-products', '*', '-version', '[17.0,18.0)',
    '-requires', 'Microsoft.VisualStudio.Component.VC.Tools.x86.x64', '-property', 'installationPath') | Select-Object -First 1)
if (!$vsInstall) { throw 'Visual Studio 2022 with the MSVC x64 tools is required.' }
$dumpbin = Get-ChildItem -Directory -LiteralPath (Join-Path $vsInstall 'VC/Tools/MSVC') |
    Sort-Object { [version]$_.Name } -Descending |
    ForEach-Object { Join-Path $_.FullName 'bin/Hostx64/x64/dumpbin.exe' } |
    Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
if (!$dumpbin) { throw 'MSVC dumpbin.exe was not found.' }

function Assert-NativeLibrary {
    $data = [IO.File]::ReadAllBytes($dll)
    if ($data.Length -lt 256 -or [BitConverter]::ToUInt16($data, 0) -ne 0x5a4d) { throw 'Windows JNI is not a PE DLL.' }
    $header = [BitConverter]::ToInt32($data, 0x3c)
    if ($header -lt 0 -or $header -gt $data.Length - 24 -or
        [BitConverter]::ToUInt32($data, $header) -ne 0x4550 -or
        [BitConverter]::ToUInt16($data, $header + 4) -ne 0x8664) { throw 'Windows JNI is not x64.' }

    $imports = (Invoke-Checked $dumpbin @('/NOLOGO', '/DEPENDENTS', $dll)) -join "`n"
    if ($imports -match '(?i)\b(?:msvc[pr]\d+\w*|vcruntime\d+\w*|ucrtbase[d]?|api-ms-win-crt-[\w-]+|(?:lib)?(?:snappy|lz4|zlib|zstd)\w*)\.dll\b') {
        throw "The JNI DLL unexpectedly imports an external runtime or compression codec: $($Matches[0])."
    }
    $exports = (Invoke-Checked $dumpbin @('/NOLOGO', '/EXPORTS', $dll)) -join "`n"
    foreach ($method in @('ColumnFamilyOptions_periodicCompactionSeconds', 'ColumnFamilyOptions_setPeriodicCompactionSeconds',
        'Options_periodicCompactionSeconds', 'Options_setPeriodicCompactionSeconds')) {
        if ($exports -notmatch "\bJava_org_rocksdb_$method\b") { throw "The JNI DLL lacks FRocksDB method: $method." }
    }
}

if ((Test-Path -LiteralPath $manifest -PathType Leaf) -and (Test-Path -LiteralPath $dll -PathType Leaf) -and
    @($noticeFiles | Where-Object { !(Test-Path -LiteralPath (Join-Path $licenses $_) -PathType Leaf) }).Count -eq 0) {
    $expected = (Get-Content -LiteralPath $manifest) -join "`n"
    if ($expected -ceq ((Get-StageManifest) -join "`n")) {
        Assert-NativeLibrary
        Write-Host "Verified cached Windows RocksDB DLL: $dll"
        exit 0
    }
}

if (!$env:JAVA_HOME -or !(Test-Path -LiteralPath (Join-Path $env:JAVA_HOME 'bin/javac.exe') -PathType Leaf)) {
    throw 'Set JAVA_HOME to a JDK 17 installation before building the DLL.'
}
$cmake = (Get-Command cmake.exe).Source
$tempDirectory = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { [IO.Path]::GetTempPath() }
$buildRoot = Join-Path $tempDirectory ('flink-rocksdb-' + [Guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Path $buildRoot | Out-Null
function Expand-Source([string] $Repository, [string] $Revision) {
    $name = $Repository.Split('/')[-1]
    $archive = Join-Path $buildRoot "$name.zip"
    Invoke-WebRequest -Uri "https://codeload.github.com/$Repository/zip/$Revision" -OutFile $archive -MaximumRetryCount 3
    Expand-Archive -LiteralPath $archive -DestinationPath $buildRoot
    return (Join-Path $buildRoot "$name-$Revision")
}

try {
    $rocks = Expand-Source 'ververica/frocksdb' $frocksRevision
    $vcpkg = Expand-Source 'microsoft/vcpkg' $vcpkgRevision
    Invoke-Checked (Join-Path $vcpkg 'bootstrap-vcpkg.bat') @('-disableMetrics')
    $triplet = 'x64-windows-static'
    Invoke-Checked (Join-Path $vcpkg 'vcpkg.exe') (@('install', "--vcpkg-root=$vcpkg", '--disable-metrics') +
        @($codecs | ForEach-Object { "${_}:$triplet" }))
    $installed = Join-Path $vcpkg "installed/$triplet"

    # FRocksDB's MSVC build takes its codec inputs from these environment variables.
    $libraryNames = @{
        SNAPPY = @('snappy.lib'); LZ4 = @('lz4.lib', 'liblz4.lib');
        ZLIB = @('zlib.lib', 'zlibstatic.lib', 'z.lib'); ZSTD = @('zstd.lib', 'libzstd.lib', 'zstd_static.lib', 'libzstd_static.lib')
    }
    foreach ($codec in $libraryNames.Keys) {
        $library = $libraryNames[$codec] | ForEach-Object { Join-Path $installed "lib/$_" } |
            Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
        if (!$library) { throw "Could not find the static $codec library built by vcpkg." }
        [Environment]::SetEnvironmentVariable("${codec}_INCLUDE", (Join-Path $installed 'include'))
        [Environment]::SetEnvironmentVariable("${codec}_LIB_RELEASE", $library)
    }

    # Upstream CMake also generates JNI headers for a few test bridge classes. Supply the
    # exact build-only JARs from Maven Central rather than relying on its legacy S3 URL.
    $testLibraries = Join-Path $rocks 'java/test-libs'
    New-Item -ItemType Directory -Path $testLibraries -Force | Out-Null
    foreach ($artifact in @('junit/junit/4.12/junit-4.12.jar', 'org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar',
        'org/mockito/mockito-all/1.10.19/mockito-all-1.10.19.jar', 'cglib/cglib/2.2.2/cglib-2.2.2.jar',
        'org/assertj/assertj-core/1.7.1/assertj-core-1.7.1.jar')) {
        Invoke-WebRequest -Uri "https://repo.maven.apache.org/maven2/$artifact" -MaximumRetryCount 3 `
            -OutFile (Join-Path $testLibraries ($artifact.Split('/')[-1]))
    }

    $build = Join-Path $buildRoot 'build'
    Invoke-Checked $cmake @('-S', $rocks, '-B', $build, '-G', 'Visual Studio 17 2022', '-A', 'x64',
        "-DCMAKE_GENERATOR_INSTANCE=$vsInstall", '-DCMAKE_BUILD_TYPE=Release',
        '-DCMAKE_POLICY_VERSION_MINIMUM=3.5', '-DCMAKE_POLICY_DEFAULT_CMP0091=NEW', '-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded',
        '-DCMAKE_SHARED_LINKER_FLAGS_RELEASE=/DEBUG:NONE',
        '-DWITH_JNI=ON', '-DWITH_MD_LIBRARY=OFF', '-DWITH_RUNTIME_DEBUG=OFF', '-DWITH_WINDOWS_UTF8_FILENAMES=ON',
        '-DWITH_SNAPPY=ON', '-DWITH_LZ4=ON', '-DWITH_ZLIB=ON', '-DWITH_ZSTD=ON',
        '-DWITH_FOLLY_DISTRIBUTED_MUTEX=OFF', '-DWITH_TESTS=OFF', '-DWITH_TOOLS=OFF', '-DWITH_CORE_TOOLS=OFF',
        '-DWITH_BENCHMARK_TOOLS=OFF', '-DROCKSDB_BUILD_SHARED=OFF', '-DFAIL_ON_WARNINGS=OFF')
    Invoke-Checked $cmake @('--build', $build, '--config', 'Release', '--target', 'rocksdbjni-shared', '--parallel', '4')

    New-Item -ItemType Directory -Path $licenses -Force | Out-Null
    if (Test-Path -LiteralPath $manifest) { Remove-Item -LiteralPath $manifest }
    Copy-Item -LiteralPath (Join-Path $build 'java/Release/rocksdbjni-shared.dll') -Destination $dll -Force
    Copy-Item -LiteralPath (Join-Path $rocks 'LICENSE.Apache') -Destination (Join-Path $licenses 'FRocksDB-LICENSE.Apache.txt') -Force
    Copy-Item -LiteralPath (Join-Path $rocks 'COPYING') -Destination (Join-Path $licenses 'FRocksDB-COPYING.txt') -Force
    foreach ($codec in $codecs) {
        Copy-Item -LiteralPath (Join-Path $installed "share/$codec/copyright") -Destination (Join-Path $licenses "$codec.txt") -Force
    }
    @("FRocksDB source: https://github.com/ververica/frocksdb/tree/$frocksRevision",
      "Static codec ports (Snappy, LZ4, zlib, Zstandard): https://github.com/microsoft/vcpkg/tree/$vcpkgRevision/ports") |
        Set-Content -LiteralPath (Join-Path $licenses 'sources.txt') -Encoding ascii
    Assert-NativeLibrary
    Get-StageManifest | Set-Content -LiteralPath $manifest -Encoding ascii
    Write-Host "Built and verified Windows RocksDB DLL: $dll"
} finally {
    Remove-Item -LiteralPath $buildRoot -Recurse -Force
}
