param(
    [ValidateSet('Check', 'Shell', 'Ports', 'Build', 'Flash', 'Monitor', 'Control')]
    [string]$Action = 'Check',
    [ValidateSet('left', 'right')]
    [string]$Hand = 'left',
    [string]$Port = 'auto'
)

$ErrorActionPreference = 'Stop'
$projectDir = Split-Path -Parent $PSScriptRoot
$env:IDF_PATH = 'E:\Esp32\Espressif\frameworks\esp-idf-v6.1'
$env:IDF_TOOLS_PATH = 'E:\Esp32\Espressif'
$env:IDF_PYTHON_ENV_PATH = 'E:\Esp32\Espressif\python_env\idf6.1_py3.11_env'
$env:PYTHONUTF8 = '1'
$env:CCACHE_DIR = Join-Path $projectDir '.cache\ccache'
$python = Join-Path $env:IDF_PYTHON_ENV_PATH 'Scripts\python.exe'
$idf = Join-Path $env:IDF_PATH 'tools\idf.py'
if (!(Test-Path $python) -or !(Test-Path $idf)) {
    throw 'ESP-IDF 6.1 or its Python environment is missing. Check tools/idf.ps1.'
}
Set-Location $projectDir

if ($Action -eq 'Ports') {
    & $python -m serial.tools.list_ports -v
    exit $LASTEXITCODE
}

if ($Action -in @('Flash', 'Monitor', 'Control')) {
    if ($Port -eq 'auto') {
        $portsJson = & $python -c 'import json; from serial.tools.list_ports import comports; print(json.dumps([p.device for p in comports() if p.vid is not None]))'
        if ($LASTEXITCODE -ne 0) { throw 'Could not scan serial ports.' }
        $usbPorts = @((ConvertFrom-Json $portsJson) | Where-Object { $_ -is [string] -and $_ -match '^COM[0-9]+$' })
        if ($usbPorts.Count -eq 1) {
            $Port = $usbPorts[0]
        } else {
            & $python -m serial.tools.list_ports -v
            if ($usbPorts.Count -eq 0) { throw 'No USB serial device found. Connect the glove and run again.' }
            $Port = Read-Host 'More than one USB device found. Enter the glove port (for example COM14)'
        }
    }
    if ($Port -notmatch '^COM[0-9]+$') { throw 'Invalid port. Use a name such as COM14.' }
    Write-Host "Serial port: $Port"
}

if ($Action -eq 'Control') {
    & $python (Join-Path $PSScriptRoot 'glove_serial_control.py') --port $Port --baud 921600
    exit $LASTEXITCODE
}

if ($Action -eq 'Flash') {
    $cloudFile = Join-Path $projectDir 'main\cloud_config.h'
    if (!(Test-Path $cloudFile) -or (Get-Content $cloudFile -Raw) -match 'replace-with-server-token') {
        throw 'Fill in main/cloud_config.h with the server IMU_TOKEN before flashing.'
    }
}

$env:PATH = (Join-Path $env:IDF_PYTHON_ENV_PATH 'Scripts') + ';' + $env:PATH
. (Join-Path $env:IDF_PATH 'export.ps1')
if (!$?) { throw 'ESP-IDF environment activation failed.' }
if ($Action -eq 'Shell') {
    Write-Host 'ESP-IDF 6.1 is ready. Project directory selected.'
    return
}
if ($Action -eq 'Check') {
    & $python $idf --version
    exit $LASTEXITCODE
}

$idfArgs = @('-B', "build_$Hand", '-D', "SDKCONFIG=sdkconfig.$Hand", '-D', "SDKCONFIG_DEFAULTS=sdkconfig.defaults.$Hand")
if ($Action -eq 'Build') {
    & $python $idf @idfArgs build
} elseif ($Action -eq 'Flash') {
    & $python $idf @idfArgs -p $Port flash
} elseif ($Action -eq 'Monitor') {
    & $python $idf @idfArgs -p $Port -b 921600 monitor
}
exit $LASTEXITCODE
