$hostName = if ($env:STACK_HOST) { $env:STACK_HOST } else { "localhost" }

Write-Host "ORLB API:           http://$hostName:8080"
Write-Host "Prometheus UI:      http://$hostName:9090"
Write-Host "Alertmanager UI:    http://$hostName:9093"
Write-Host "Grafana UI:         http://$hostName:3000"
Write-Host ""
Write-Host "Credentials"
Write-Host "-----------"
Write-Host "Grafana: admin / admin"
