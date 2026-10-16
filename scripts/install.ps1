# monitor-agent-remote: install.ps1
#
# Instalador para Windows (x64) de la central:
#
#   1. Comprueba el sistema (Windows 10 / Server 2016 o superior, x64).
#   2. Comprueba el runtime de Visual C++ 2015-2022 (x64) y, si falta, lo
#      descarga e instala en silencio. SQLite va dentro del ejecutable
#      (rusqlite bundled): no hace falta instalar nada más.
#   3. Copia el ejecutable a la carpeta de instalación (ahí viven monitor.db,
#      config.toml y monitor-agent-remote.log) y crea un config.toml de
#      arranque.
#   4. Registra la tarea programada al arranque del sistema (usuario SYSTEM,
#      sin límite de tiempo y con reintentos si el proceso se cae).
#   5. Abre el puerto en el firewall (opcional, -OpenFirewall), necesario para
#      que los agentes de la red local puedan avisar.
#   6. Arranca, espera a que el panel responda y resume los siguientes pasos.
#
# Re-ejecutable: si la tarea ya existe, actualiza el binario y la reinicia.
# El config.toml y la base de datos NO se tocan.
#
# Uso (consola ELEVADA):
#   .\install.ps1
#   .\install.ps1 -InstallDir D:\MonitorCentral -Listen 0.0.0.0:9600 -OpenFirewall
#   .\install.ps1 -SkipDeps        # no descargar/instalar dependencias
#   .\install.ps1 -NoStart
#
# Requisitos: monitor-agent-remote.exe (release) en esta misma carpeta o -Exe.

[CmdletBinding()]
param(
    [string]$InstallDir = "C:\MonitorCentral",
    [string]$Exe = "",
    [string]$Listen = "127.0.0.1:9600",
    [string]$TaskName = "MonitorCentral",
    [switch]$OpenFirewall,
    [switch]$SkipDeps,
    [switch]$NoStart
)

$ErrorActionPreference = "Stop"
$PanelPort = [int](($Listen -split ':')[-1])

function Write-Paso($t) { Write-Host "`n== $t" -ForegroundColor Cyan }
function Write-Ok($t) { Write-Host "   $t" -ForegroundColor Green }
function Write-Config($ruta, $texto) {
    # UTF-8 SIN BOM (Set-Content -Encoding UTF8 en PS 5.1 mete BOM y el parser
    # TOML del binario lo rechaza: la central no arrancaría).
    [System.IO.File]::WriteAllText($ruta, $texto, (New-Object System.Text.UTF8Encoding($false)))
}
function Write-Aviso($t) { Write-Host "   $t" -ForegroundColor Yellow }
function Write-Mal($t) { Write-Host "   $t" -ForegroundColor Red }

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
    Write-Mal "Este instalador necesita una consola elevada (Administrador)."
    Write-Host "Ejecuta:  Start-Process powershell -Verb RunAs -ArgumentList '-File `"$PSCommandPath`"'"
    exit 1
}

Write-Host ""
Write-Host "  monitor-agent-remote - instalador Windows" -ForegroundColor Magenta
Write-Host "  -----------------------------------------" -ForegroundColor DarkGray

# ---------------------------------------------------------------- 1. Sistema
Write-Paso "1/6 Comprobando el sistema"
if (-not [Environment]::Is64BitOperatingSystem) {
    Write-Mal "Se requiere un Windows de 64 bits."
    exit 1
}
$cv = Get-ItemProperty "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion"
$major = 0
if ($cv.CurrentMajorVersionNumber) { $major = [int]$cv.CurrentMajorVersionNumber } else { $major = 6 }
$build = if ($cv.CurrentBuild) { [int]$cv.CurrentBuild } else { 0 }
Write-Ok ("Windows {0}.{1} build {2} ({3})" -f $major, $cv.CurrentMinorVersionNumber, $build, $cv.ProductName)
if ($major -lt 10) {
    Write-Mal "La central necesita Windows 10 o Server 2016 (build 14393) o superior."
    exit 1
}
if ($build -lt 14393) { Write-Aviso "Build antigua: se recomienda Windows 10 1607+ / Server 2016+." }
if ($env:PROCESSOR_ARCHITECTURE -notlike "AMD64") {
    Write-Aviso "El binario de este release es x64; en $env:PROCESSOR_ARCHITECTURE podría no arrancar."
}

# ------------------------------------------------------------ 2. Dependencias
Write-Paso "2/6 Dependencias (runtime de Visual C++ 2015-2022 x64)"
function Test-VcRuntime {
    foreach ($k in @("HKLM:\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64",
                     "HKLM:\SOFTWARE\WOW6432Node\Microsoft\VisualStudio\14.0\VC\Runtimes\x64")) {
        if (Test-Path $k) {
            $installed = (Get-ItemProperty -Path $k -ErrorAction SilentlyContinue).Installed
            if ($installed -eq 1) { return $true }
        }
    }
    return (Test-Path "$env:SystemRoot\System32\vcruntime140.dll") -and
           (Test-Path "$env:SystemRoot\System32\msvcp140.dll")
}

if (Test-VcRuntime) {
    Write-Ok "ya está instalado (SQLite va dentro del ejecutable: no hace falta nada más)"
} elseif ($SkipDeps) {
    Write-Aviso "falta y se ha pedido -SkipDeps: la central podría no arrancar."
    Write-Aviso "Instálalo a mano: https://aka.ms/vs/17/release/vc_redist.x64.exe"
} else {
    Write-Host "   no está: descargando el redistribuidor oficial..." -ForegroundColor Cyan
    $tmp = Join-Path $env:TEMP "vc_redist.x64.exe"
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri "https://aka.ms/vs/17/release/vc_redist.x64.exe" -OutFile $tmp -UseBasicParsing
        $p = Start-Process -FilePath $tmp -ArgumentList "/install", "/quiet", "/norestart" -Wait -PassThru
        if (@(0, 3010, 1638) -contains $p.ExitCode) {
            if (Test-VcRuntime) { Write-Ok "instalado" }
            else { Write-Aviso "el instalador devolvió $($p.ExitCode); puede requerir reiniciar" }
        } else {
            Write-Aviso "el instalador devolvió $($p.ExitCode). Si la central no arranca, instálalo a mano: https://aka.ms/vs/17/release/vc_redist.x64.exe"
        }
        Remove-Item $tmp -ErrorAction SilentlyContinue
    } catch {
        Write-Aviso "no se pudo descargar/instalar automáticamente: $_"
        Write-Aviso "Hazlo a mano: https://aka.ms/vs/17/release/vc_redist.x64.exe  (o usa -SkipDeps)"
    }
}

# ------------------------------------------------------------- 3. Instalación
Write-Paso "3/6 Copiando la central a $InstallDir"
if (-not $Exe) { $Exe = Join-Path $PSScriptRoot "monitor-agent-remote.exe" }
if (-not (Test-Path $Exe)) {
    Write-Mal "No encuentro monitor-agent-remote.exe en '$Exe'."
    Write-Host "   Descomprime el release completo o pasa -Exe <ruta al .exe>."
    exit 1
}
$Exe = (Resolve-Path $Exe).Path
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$Target = Join-Path $InstallDir "monitor-agent-remote.exe"

$tarea = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($tarea -and $tarea.State -eq "Running") {
    Write-Host "   parando la tarea para actualizar el binario..." -ForegroundColor Cyan
    Stop-ScheduledTask -TaskName $TaskName
    Start-Sleep -Seconds 2
}
if ($Exe.ToLower() -ne $Target.ToLower()) {
    Copy-Item -Path $Exe -Destination $Target -Force
    Write-Ok "binario copiado ($([math]::Round((Get-Item $Target).Length / 1MB, 1)) MB)"
} else {
    Write-Ok "el binario ya está en su sitio"
}

$Config = Join-Path $InstallDir "config.toml"
if (-not (Test-Path $Config)) {
    $contenido = @"
# monitor-agent-remote - configuración de arranque (OPCIONAL).
# Sólo se lee la PRIMERA vez (antes de existir monitor.db): después, listen,
# token y retención se gestionan desde el panel de ajustes y se guardan en la
# base de datos.

db_path = "monitor.db"

# 0.0.0.0:$PanelPort para aceptar agentes de la red local (se aplica en vivo).
listen = "$Listen"

# Opcional: si lo defines, /api/ingest y /api/settings exigen
# Authorization: Bearer <token>. El panel de estado sigue siendo público.
#token = "CAMBIAME"
"@
    Write-Config $Config $contenido
    Write-Ok "config.toml creado"
} else {
    Write-Ok "config.toml existente: se respeta tal cual"
}

# ------------------------------------------------------------ 4. Tarea/arranque
Write-Paso "4/6 Registrando la tarea '$TaskName' (al arranque del sistema)"
$action = New-ScheduledTaskAction -Execute $Target -WorkingDirectory $InstallDir
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Principal $principal -Settings $settings -Force | Out-Null
if ($tarea) { Write-Ok "tarea actualizada" } else { Write-Ok "tarea registrada" }

# ---------------------------------------------------------------- 5. Firewall
Write-Paso "5/6 Firewall"
if ($OpenFirewall) {
    $ruleName = "monitor-agent-remote-$PanelPort"
    if (-not (Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -DisplayName $ruleName -Direction Inbound -Action Allow `
            -Protocol TCP -LocalPort $PanelPort -Profile Private,Domain | Out-Null
        Write-Ok "puerto $PanelPort abierto (perfiles privado/dominio)"
    } else {
        Write-Ok "la regla '$ruleName' ya existía"
    }
} else {
    Write-Ok "sin cambios (usa -OpenFirewall para que los agentes de la LAN puedan avisar)"
}

# --------------------------------------------------------------- 6. Arranque
Write-Paso "6/6 Arrancando y comprobando"
if ($NoStart) {
    Write-Aviso "-NoStart: la tarea queda registrada pero parada (Start-ScheduledTask -TaskName $TaskName)"
} else {
    Start-ScheduledTask -TaskName $TaskName
    $host_ = if ($Listen -like "0.0.0.0*") { "127.0.0.1" } else { ($Listen -split ':')[0] }
    $url = "http://${host_}:$PanelPort/api/agents"
    $ok = $false
    for ($i = 1; $i -le 20; $i++) {
        Start-Sleep -Milliseconds 700
        try {
            $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 3
            if ($r.StatusCode -eq 200) { $ok = $true; break }
        } catch { }
    }
    if ($ok) { Write-Ok "el panel responde en $url" }
    else { Write-Aviso "el panel aún no responde; revisa $InstallDir\monitor-agent-remote.log" }
}

$ip = (Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
    Where-Object { $_.IPAddress -notlike "127.*" -and $_.PrefixOrigin -ne "WellKnown" } |
    Select-Object -First 1).IPAddress
Write-Host ""
Write-Host "  Listo." -ForegroundColor Green
Write-Host "  ---------------------------------------------------------------"
Write-Host "  Tarea      : $TaskName  (al arranque del sistema, usuario SYSTEM)"
Write-Host "  Carpeta    : $InstallDir   (monitor.db, config.toml, log)"
Write-Host "  Log        : $InstallDir\monitor-agent-remote.log"
Write-Host "  Panel      : http://localhost:$PanelPort"
if ($ip) { Write-Host "               http://${ip}:$PanelPort   (desde otro equipo)" }
Write-Host ""
Write-Host "  Siguientes pasos:" -ForegroundColor Cyan
Write-Host "    1. Abre el panel y, si vas a recibir agentes de la red, pon"
Write-Host "       listen = 0.0.0.0:$PanelPort y un token (se aplica en vivo)."
Write-Host "    2. En cada servidor con monitor-agent, pestaña Exporter:"
Write-Host "         endpoint = <IP-de-esta-central>:$PanelPort"
Write-Host "         agent_id = nombre del servidor"
Write-Host "         token    = el mismo, si lo has definido"
Write-Host "       y usa 'Probar conexión con el remoto'."
Write-Host ""
Write-Host "  Gestión:  Stop-ScheduledTask -TaskName $TaskName | Start-ScheduledTask -TaskName $TaskName" -ForegroundColor DarkGray
Write-Host "            .\install.ps1     (actualizar a una versión nueva)" -ForegroundColor DarkGray
Write-Host "            .\uninstall.ps1   (desinstalar)" -ForegroundColor DarkGray
Write-Host ""
