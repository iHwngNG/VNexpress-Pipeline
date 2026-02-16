# Airflow Docker Management Script
# PowerShell script để quản lý Airflow Docker services

# Colors for output
$Green = "Green"
$Yellow = "Yellow"
$Red = "Red"

function Show-Menu {
    Write-Host "`n=== Airflow Docker Manager ===" -ForegroundColor $Green
    Write-Host "1. Build images (clean)"
    Write-Host "2. Start services"
    Write-Host "3. Stop services"
    Write-Host "4. Restart services"
    Write-Host "5. View logs (all)"
    Write-Host "6. View logs (webserver)"
    Write-Host "7. View logs (scheduler)"
    Write-Host "8. Clean up (remove volumes)"
    Write-Host "9. Full reset (clean + build + start)"
    Write-Host "0. Exit"
    Write-Host "==============================`n" -ForegroundColor $Green
}

function Build-Clean {
    Write-Host "`n[BUILD] Building images with --no-cache..." -ForegroundColor $Yellow
    docker-compose down
    docker system prune -f
    docker-compose build --no-cache
    Write-Host "[BUILD] Complete!" -ForegroundColor $Green
}

function Start-Services {
    Write-Host "`n[START] Starting services..." -ForegroundColor $Yellow
    docker-compose up -d
    Write-Host "[START] Complete!" -ForegroundColor $Green
    Write-Host "`nAirflow UI: http://localhost:8080" -ForegroundColor $Green
    Write-Host "Username: airflow" -ForegroundColor $Green
    Write-Host "Password: airflow" -ForegroundColor $Green
}

function Stop-Services {
    Write-Host "`n[STOP] Stopping services..." -ForegroundColor $Yellow
    docker-compose down
    Write-Host "[STOP] Complete!" -ForegroundColor $Green
}

function Restart-Services {
    Write-Host "`n[RESTART] Restarting services..." -ForegroundColor $Yellow
    docker-compose restart
    Write-Host "[RESTART] Complete!" -ForegroundColor $Green
}

function View-Logs-All {
    Write-Host "`n[LOGS] Viewing all logs (Ctrl+C to exit)..." -ForegroundColor $Yellow
    docker-compose logs -f
}

function View-Logs-Webserver {
    Write-Host "`n[LOGS] Viewing webserver logs (Ctrl+C to exit)..." -ForegroundColor $Yellow
    docker-compose logs -f airflow-webserver
}

function View-Logs-Scheduler {
    Write-Host "`n[LOGS] Viewing scheduler logs (Ctrl+C to exit)..." -ForegroundColor $Yellow
    docker-compose logs -f airflow-scheduler
}

function Clean-Up {
    Write-Host "`n[CLEANUP] Removing containers and volumes..." -ForegroundColor $Yellow
    $confirm = Read-Host "This will delete all data. Continue? (y/n)"
    if ($confirm -eq 'y') {
        docker-compose down -v
        Write-Host "[CLEANUP] Complete!" -ForegroundColor $Green
    } else {
        Write-Host "[CLEANUP] Cancelled" -ForegroundColor $Yellow
    }
}

function Full-Reset {
    Write-Host "`n[RESET] Full reset: clean + build + start..." -ForegroundColor $Yellow
    $confirm = Read-Host "This will delete all data and rebuild. Continue? (y/n)"
    if ($confirm -eq 'y') {
        docker-compose down -v
        docker system prune -f
        docker-compose build --no-cache
        docker-compose up -d
        Write-Host "[RESET] Complete!" -ForegroundColor $Green
        Write-Host "`nAirflow UI: http://localhost:8080" -ForegroundColor $Green
    } else {
        Write-Host "[RESET] Cancelled" -ForegroundColor $Yellow
    }
}

# Main loop
do {
    Show-Menu
    $choice = Read-Host "Select option"
    
    switch ($choice) {
        '1' { Build-Clean }
        '2' { Start-Services }
        '3' { Stop-Services }
        '4' { Restart-Services }
        '5' { View-Logs-All }
        '6' { View-Logs-Webserver }
        '7' { View-Logs-Scheduler }
        '8' { Clean-Up }
        '9' { Full-Reset }
        '0' { 
            Write-Host "`nExiting..." -ForegroundColor $Green
            break 
        }
        default { 
            Write-Host "`nInvalid option!" -ForegroundColor $Red 
        }
    }
    
    if ($choice -ne '0') {
        Read-Host "`nPress Enter to continue"
    }
} while ($choice -ne '0')
