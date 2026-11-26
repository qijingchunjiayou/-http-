$ErrorActionPreference = 'Stop'
$projectDir = Split-Path -Parent $PSScriptRoot
$workspaceDir = Split-Path -Parent $projectDir
$launcher = Join-Path $workspaceDir 'serial-control.cmd'
$python = 'E:\Esp32\Espressif\python_env\idf6.1_py3.11_env\Scripts\python.exe'
if (!(Test-Path -LiteralPath $launcher) -or !(Test-Path -LiteralPath $python)) {
    throw 'Capture launcher or configured Python environment is missing.'
}
& $python -c 'import serial, msvcrt'
if ($LASTEXITCODE -ne 0) { throw 'Capture dependency check failed.' }
Write-Host 'Capture dependencies OK'

$desktop = [Environment]::GetFolderPath('Desktop')
$shortcutPath = Join-Path $desktop 'Glove Capture.lnk'
if (Test-Path -LiteralPath $shortcutPath) {
    throw 'Desktop shortcut already exists. Inspect it before replacing.'
}
$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = Join-Path $env:SystemRoot 'System32\cmd.exe'
$shortcut.Arguments = '/d /c ""{0}""' -f $launcher
$shortcut.WorkingDirectory = $workspaceDir
$shortcut.Description = 'Glove recording: F2 checks 22 IMUs before start, then stops; F3 status.'
$shortcut.IconLocation = (Join-Path $env:SystemRoot 'System32\shell32.dll') + ',16'
$shortcut.WindowStyle = 1
$shortcut.Save()

$saved = $shell.CreateShortcut($shortcutPath)
if ($saved.Arguments -ne $shortcut.Arguments -or $saved.TargetPath -ne $shortcut.TargetPath) {
    throw 'Shortcut verification failed.'
}
Write-Host "Desktop shortcut installed and verified: $shortcutPath"
