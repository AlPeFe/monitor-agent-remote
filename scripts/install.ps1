# monitor-agent-remote: install.ps1
#
# Registra la central como TAREA PROGRAMADA al arranque de Windows (usuario
# SYSTEM, sin necesidad de iniciar sesión) y la arranca. El panel y la API
# quedan disponibles en http://<IP>:<puerto>/.
#
# Ejecutar como Administrador.
#
# Uso:
#   .\install.ps1                                   # autodetecta el .exe junto al script
#   .\install.ps1 -Exe C:\MonitorCentral\monitor-agent-remote.exe
#   .\install.ps1 -OpenFirewall -ListenPort 9600    # abre el puerto para la LAN
#   .\install.ps1 -TaskName MonitorCentral
#
# Requisitos: monitor-agent-remote.exe (release) junto a este script o en -Exe.
# El directorio de trabajo de la tarea es la carpeta del .exe: ahí viven
# config.toml (semilla) y monitor.db.

[CmdletBinding()]
param(
    [string]$Exe = "",
    [string]$TaskName = "MonitorCentral",
    [int]$ListenPort = 9600,
    [switch]$SkipFirewall,
    [switch]$OpenFirewall
)

$ErrorActionPreference = "Stop"

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p  = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
    Write-Host "ERROR: este script necesita una consola elevada (Administrador)." -ForegroundColor Red
    Write-Host "Ejecuta:  Start-Process powershell -Verb RunAs -ArgumentList '-File \"$PSCommandPath\"'"
    exit 1
}

# 1) Localizar el ejecutable
if (-not $Exe) {
    $Exe = Join-Path $PSScriptRoot "monitor-agent-remote.exe"
}
if (-not (Test-Path $Exe)) {
    Write-Host "ERROR: no encuentro monitor-agent-remote.exe en '$Exe'." -ForegroundColor Red
    Write-Host "Copia el release junto a este script o pasa -Exe <ruta>."
    exit 1
}
$Exe  = (Resolve-Path $Exe).Path
$Work = Split-Path $Exe -Parent

# 2) Comprobaciones previas
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Write-Host "AVISO: la tarea '$TaskName' ya existe; se reemplaza." -ForegroundColor Yellow
}

# 3) Tarea al arranque del sistema
Write-Host "Registrando la tarea programada '$TaskName'..." -ForegroundColor Cyan
$action = New-ScheduledTaskAction -Execute $Exe -WorkingDirectory $Work
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
# Sin límite de tiempo de ejecución (es un servicio de fondo) y con reintentos
# automáticos si el proceso se cae.
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Principal $principal -Settings $settings -Force | Out-Null

# 4) Firewall (opcional): el panel escucha en 0.0.0.0:$ListenPort
if ($OpenFirewall -and -not $SkipFirewall) {
    $ruleName = "monitor-agent-remote-$ListenPort"
    Write-Host "Abriendo el puerto $ListenPort en el firewall (regla '$ruleName')..." -ForegroundColor Cyan
    New-NetFirewallRule -DisplayName $ruleName -Direction Inbound -Action Allow `
        -Protocol TCP -LocalPort $ListenPort -Profile Private,Domain | Out-Null
}

# 5) Arrancar ya
Write-Host "Arrancando la central..." -ForegroundColor Cyan
Start-ScheduledTask -TaskName $TaskName
Start-Sleep -Seconds 2

$state = (Get-ScheduledTask -TaskName $TaskName).State
Write-Host ""
if ($state -eq "Running") {
    Write-Host "monitor-agent-remote instalado y corriendo." -ForegroundColor Green
} else {
    Write-Host "AVISO: la tarea quedo en estado '$state'." -ForegroundColor Yellow
}
Write-Host "  Tarea    : $TaskName ($state, al arranque del sistema)"
Write-Host "  Ejecutable: $Exe"
Write-Host "  Datos    : $Work (monitor.db, config.toml)"
Write-Host "  Log      : $Workmonitor-agent-remote.log"
Write-Host "  Panel    : http://localhost:$ListenPort"
Write-Host ""
Write-Host "Gestion: Stop-ScheduledTask -TaskName $TaskName | Start-ScheduledTask -TaskName $TaskName | uninstall.ps1" -ForegroundColor DarkGray
