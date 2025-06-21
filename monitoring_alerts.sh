#!/bin/bash

# monitoring_alerts.sh - Set up monitoring and alerting for rawprocessor
# This sets up basic monitoring scripts and email alerts

# Configuration
EMAIL_RECIPIENT="admin@yourdomain.com"
HEALTH_URL="http://localhost:8080/health"
LOG_FILE="/var/log/rawprocessor-monitor.log"

# Create monitoring script
cat > /usr/local/bin/rawprocessor-monitor << 'EOF'
#!/bin/bash

# Rawprocessor monitoring and alerting script
EMAIL_RECIPIENT="admin@yourdomain.com"
HEALTH_URL="http://localhost:8080/health"
LOG_FILE="/var/log/rawprocessor-monitor.log"
ALERT_FILE="/tmp/rawprocessor-alert-sent"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> $LOG_FILE
}

send_alert() {
    local subject="$1"
    local message="$2"
    
    # Check if alert was already sent in the last hour
    if [[ -f $ALERT_FILE ]] && [[ $(find $ALERT_FILE -mmin -60 2>/dev/null) ]]; then
        log_message "Alert suppressed (already sent within last hour): $subject"
        return
    fi
    
    # Send email alert (requires mail/sendmail to be configured)
    {
        echo "Subject: [ALERT] Rawprocessor - $subject"
        echo "To: $EMAIL_RECIPIENT"
        echo ""
        echo "Rawprocessor Alert:"
        echo ""
        echo "$message"
        echo ""
        echo "Server: $(hostname)"
        echo "Time: $(date)"
        echo ""
        echo "Check status with: rawprocessor-status status"
        echo "View logs with: rawprocessor-status logs"
    } | sendmail $EMAIL_RECIPIENT 2>/dev/null
    
    # Mark alert as sent
    touch $ALERT_FILE
    log_message "Alert sent: $subject"
}

check_service_status() {
    # Check if main service is running
    if ! systemctl is-active --quiet rawprocessor; then
        send_alert "Service Down" "Rawprocessor main service is not running"
        return 1
    fi
    
    # Check if health service is running
    if ! systemctl is-active --quiet rawprocessor-health; then
        send_alert "Health Service Down" "Rawprocessor health service is not running"
        return 1
    fi
    
    return 0
}

check_health_endpoint() {
    # Check health endpoint
    local health_response
    health_response=$(curl -s -w "%{http_code}" -o /tmp/health_response.json $HEALTH_URL 2>/dev/null)
    local http_code=${health_response: -3}
    
    if [[ $http_code != "200" ]]; then
        send_alert "Health Check Failed" "Health endpoint returned HTTP $http_code"
        return 1
    fi
    
    # Parse health status
    local status
    status=$(jq -r '.status' /tmp/health_response.json 2>/dev/null)
    
    if [[ $status == "error" ]]; then
        local error_details
        error_details=$(jq -r '.checks | to_entries[] | select(.value.status == "error") | "\(.key): \(.value.message)"' /tmp/health_response.json 2>/dev/null)
        send_alert "Health Check Errors" "Health check reported errors:\n\n$error_details"
        return 1
    elif [[ $status == "warning" ]]; then
        log_message "Health check reported warnings - status: $status"
    fi
    
    return 0
}

check_disk_space() {
    # Check disk space for logs
    local disk_usage
    disk_usage=$(df /opt/rawprocessor/logs | awk 'NR==2 {print $5}' | sed 's/%//')
    
    if [[ $disk_usage -gt 90 ]]; then
        send_alert "Disk Space Critical" "Log directory is ${disk_usage}% full"
        return 1
    elif [[ $disk_usage -gt 80 ]]; then
        log_message "Warning: Log directory is ${disk_usage}% full"
    fi
    
    return 0
}

check_queue_backlog() {
    # Check for excessive queue backlogs
    local health_response
    health_response=$(curl -s $HEALTH_URL 2>/dev/null)
    
    if [[ -n $health_response ]]; then
        local pending_jobs
        pending_jobs=$(echo $health_response | jq -r '.checks.processing_queue.pending // 0')
        
        if [[ $pending_jobs -gt 10000 ]]; then
            send_alert "High Queue Backlog" "Processing queue has $pending_jobs pending jobs"
            return 1
        fi
        
        local wp_pending
        wp_pending=$(echo $health_response | jq -r '.checks.wp_queues.total_pending // 0')
        
        if [[ $wp_pending -gt 1000 ]]; then
            send_alert "High WP Queue Backlog" "WordPress queues have $wp_pending pending jobs"
            return 1
        fi
    fi
    
    return 0
}

# Main monitoring function
main() {
    log_message "Starting monitoring check"
    
    local errors=0
    
    check_service_status || ((errors++))
    check_health_endpoint || ((errors++))
    check_disk_space || ((errors++))
    check_queue_backlog || ((errors++))
    
    if [[ $errors -eq 0 ]]; then
        log_message "All checks passed"
        # Remove alert suppression if all is well
        rm -f $ALERT_FILE
    else
        log_message "Monitoring detected $errors issues"
    fi
}

# Run main function
main "$@"
EOF

chmod +x /usr/local/bin/rawprocessor-monitor

# Create cron job for monitoring (every 5 minutes)
cat > /etc/cron.d/rawprocessor-monitor << 'EOF'
# Rawprocessor monitoring - runs every 5 minutes
*/5 * * * * root /usr/local/bin/rawprocessor-monitor >/dev/null 2>&1
EOF

# Create daily health report
cat > /usr/local/bin/rawprocessor-daily-report << 'EOF'
#!/bin/bash

# Daily health report for rawprocessor
EMAIL_RECIPIENT="admin@yourdomain.com"
HEALTH_URL="http://localhost:8080/health"

generate_report() {
    echo "=== Rawprocessor Daily Health Report ==="
    echo "Date: $(date)"
    echo "Server: $(hostname)"
    echo ""
    
    echo "=== Service Status ==="
    systemctl status rawprocessor --no-pager -l | head -20
    echo ""
    systemctl status rawprocessor-health --no-pager -l | head -20
    echo ""
    
    echo "=== Health Check ==="
    curl -s $HEALTH_URL | python3 -m json.tool 2>/dev/null || echo "Health endpoint not available"
    echo ""
    
    echo "=== Resource Usage ==="
    echo "Memory Usage:"
    free -h
    echo ""
    echo "Disk Usage:"
    df -h /opt/rawprocessor
    echo ""
    echo "CPU Load:"
    uptime
    echo ""
    
    echo "=== Recent Log Summary ==="
    echo "Errors in last 24 hours:"
    journalctl -u rawprocessor --since "24 hours ago" --no-pager | grep -i error | wc -l
    echo ""
    echo "Warnings in last 24 hours:"
    journalctl -u rawprocessor --since "24 hours ago" --no-pager | grep -i warning | wc -l
    echo ""
    
    echo "=== Recent Errors ==="
    journalctl -u rawprocessor --since "24 hours ago" --no-pager | grep -i error | tail -10
    echo ""
    
    echo "=== Processing Statistics ==="
    # Get processing stats from health endpoint
    local health_data
    health_data=$(curl -s $HEALTH_URL 2>/dev/null)
    if [[ -n $health_data ]]; then
        echo $health_data | jq -r '
            "Processing Queue: \(.checks.processing_queue.total_jobs) total, \(.checks.processing_queue.pending) pending, \(.checks.processing_queue.failed) failed",
            "WP Queues: \(.checks.wp_queues.total_pending) pending, \(.checks.wp_queues.total_failed) failed",
            "Uptime: \(.uptime_seconds | tonumber | . / 3600 | floor) hours"
        ' 2>/dev/null
    fi
}

# Generate and send report
{
    echo "Subject: Rawprocessor Daily Health Report - $(hostname)"
    echo "To: $EMAIL_RECIPIENT"
    echo ""
    generate_report
} | sendmail $EMAIL_RECIPIENT 2>/dev/null

# Also save to log file
generate_report >> /var/log/rawprocessor-daily-reports.log
EOF

chmod +x /usr/local/bin/rawprocessor-daily-report

# Create cron job for daily report (at 8 AM)
cat >> /etc/cron.d/rawprocessor-monitor << 'EOF'

# Daily health report at 8 AM
0 8 * * * root /usr/local/bin/rawprocessor-daily-report >/dev/null 2>&1
EOF

# Create log cleanup script
cat > /usr/local/bin/rawprocessor-cleanup << 'EOF'
#!/bin/bash

# Cleanup old logs and temporary files
LOG_DIR="/opt/rawprocessor/logs"
TEMP_DIR="/tmp"

# Clean logs older than 90 days
find $LOG_DIR -name "*.log.*" -mtime +90 -delete 2>/dev/null

# Clean temp health response files
find $TEMP_DIR -name "health_response.json" -mtime +1 -delete 2>/dev/null

# Clean old monitoring logs
find /var/log -name "rawprocessor-*.log" -mtime +30 -delete 2>/dev/null

# Rotate large log files if needed
for log_file in $LOG_DIR/*.log; do
    if [[ -f $log_file ]] && [[ $(stat -f%z "$log_file" 2>/dev/null || stat -c%s "$log_file" 2>/dev/null) -gt 1073741824 ]]; then
        # File is larger than 1GB, force rotation
        /usr/sbin/logrotate -f /etc/logrotate.d/rawprocessor
        break
    fi
done

echo "$(date): Cleanup completed" >> /var/log/rawprocessor-cleanup.log
EOF

chmod +x /usr/local/bin/rawprocessor-cleanup

# Add weekly cleanup job
cat >> /etc/cron.d/rawprocessor-monitor << 'EOF'

# Weekly cleanup on Sunday at 2 AM
0 2 * * 0 root /usr/local/bin/rawprocessor-cleanup >/dev/null 2>&1
EOF

echo "Monitoring and alerting setup completed!"
echo ""
echo "Created scripts:"
echo "  - /usr/local/bin/rawprocessor-monitor (runs every 5 minutes)"
echo "  - /usr/local/bin/rawprocessor-daily-report (runs daily at 8 AM)"
echo "  - /usr/local/bin/rawprocessor-cleanup (runs weekly)"
echo ""
echo "Cron jobs added to: /etc/cron.d/rawprocessor-monitor"
echo ""
echo "Configure email recipient in:"
echo "  - /usr/local/bin/rawprocessor-monitor"
echo "  - /usr/local/bin/rawprocessor-daily-report"
echo ""
echo "Make sure sendmail/postfix is configured for email alerts!"
echo ""
echo "Test monitoring with: /usr/local/bin/rawprocessor-monitor"
echo "Test daily report with: /usr/local/bin/rawprocessor-daily-report"