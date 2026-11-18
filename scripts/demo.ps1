# Launches a local Conf-Chat network for the class demo (Windows PowerShell).
#   - 2 headless Chord nodes (pure storage/routing peers) that print ring status
#   - N chat peers, each in its own console window
# Usage:  powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 [-Users 3]
param([int]$Users = 3, [int]$BasePort = 5000)

$root = Split-Path -Parent $PSScriptRoot
$py = (Get-Command python).Source

function Start-Peer([string]$title, [string]$peerArgs) {
    Start-Process -FilePath "cmd.exe" -WorkingDirectory $root `
        -ArgumentList "/k title $title && `"$py`" -m confchat $peerArgs"
}

Start-Peer "node-$BasePort (bootstrap)" "--port $BasePort --headless --status 10"
Start-Sleep -Seconds 1
Start-Peer "node-$($BasePort + 1)" "--port $($BasePort + 1) --join 127.0.0.1:$BasePort --headless --status 10"
Start-Sleep -Seconds 1
for ($i = 1; $i -le $Users; $i++) {
    $port = $BasePort + 100 + $i
    Start-Peer "chat-$port" "--port $port --join 127.0.0.1:$BasePort"
    Start-Sleep -Milliseconds 500
}
Write-Host "Started 2 DHT nodes and $Users chat peers (ports $($BasePort + 101)..$($BasePort + 100 + $Users))."
