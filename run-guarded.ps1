# 自动重启守护脚本（Windows / PowerShell）
# 用法：.\run-guarded.ps1
# 代理进程退出后自动重启（Ctrl+C 视为主动停止，不会重启）。
# 日志同时输出到控制台和 logs\proxy-YYYYMMDD.log，窗口误关/崩溃后仍有据可查。
$ErrorActionPreference = "Continue"

$python = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "python" }
$logDir = Join-Path $PSScriptRoot "logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

Write-Host "=== llm-key-pool-proxy guard started (stop with Ctrl+C) ===" -ForegroundColor Cyan

while ($true) {
    $stamp = Get-Date -Format "yyyyMMdd"
    $logFile = Join-Path $logDir "proxy-$stamp.log"

    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] starting proxy, log: $logFile" -ForegroundColor Cyan
    $process = Start-Process -FilePath $python `
        -ArgumentList "proxy_server.py" `
        -WorkingDirectory $PSScriptRoot `
        -NoNewWindow `
        -PassThru `
        -RedirectStandardOutput $logFile `
        -RedirectStandardError "$logFile.err"

    # 等待进程退出；Ctrl+C 发给整个进程组，脚本随 python 一起被中断
    Wait-Process -Id $process.Id
    $code = $process.ExitCode

    if ($code -eq 0) {
        Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] proxy exited cleanly (code 0), guard stopping" -ForegroundColor Cyan
        break
    }
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] proxy exited with code $code, restarting in 3s..." -ForegroundColor Yellow
    Start-Sleep -Seconds 3
}
