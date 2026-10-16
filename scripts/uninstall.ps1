# monitor-agent-remote: uninstall.ps1
#
# Detiene y elimina la tarea programada de la central. NO borra los datos
# (monitor.db, config.toml): hazlo a mano si quieres empezar de cero.
#
# Ejecutar como Administrador.

[CmdletBinding()]
param(
    [string]$TaskName = "MonitorCentral",
    [int]$ListenPort = 9600,
    [switch]$RemoveFirewallRule
)

$ErrorActionPreference = "Stop"

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p  = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
    Write-Host "ERROR: este script necesita una consola elevada (Administrador)." -ForegroundColor Red
    exit 1
}

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $task) {
    Write-Host "No existe la tarea '$TaskName'." -ForegroundColor Yellow
} else {
    Write-Host "Parando y eliminando la tarea '$TaskName'..." -ForegroundColor Cyan
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Tarea eliminada." -ForegroundColor Green
}

if ($RemoveFirewallRule) {
    $ruleName = "monitor-agent-remote-$ListenPort"
    if (Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue) {
        Remove-NetFirewallRule -DisplayName $ruleName
        Write-Host "Regla de firewall '$ruleName' eliminada." -ForegroundColor Green
    }
}

Write-Host "Los datos (monitor.db, config.toml) siguen donde estaban." -ForegroundColor DarkGray
