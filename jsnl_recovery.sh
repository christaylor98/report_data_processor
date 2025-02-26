#!/bin/bash
# JSNL Processor Recovery Script
# Performs recovery actions and sends alerts when necessary

# Configuration
PUSHOVER_USER_KEY="your_user_key_here"
PUSHOVER_API_TOKEN="your_api_token_here"
LOG_FILE="/var/log/jsnl_processor.log"
SLEEP_START_HOUR=22  # 10 PM
SLEEP_END_HOUR=8     # 8 AM

# Check if we're in sleep hours
function is_sleep_hours() {
    local current_hour=$(date +%H)
    if [ "$current_hour" -ge "$SLEEP_START_HOUR" ] || [ "$current_hour" -lt "$SLEEP_END_HOUR" ]; then
        return 0  # True, it is sleep hours
    else
        return 1  # False, it is not sleep hours
    fi
}

# Send Pushover alert
function send_alert() {
    local title="$1"
    local message="$2"
    local priority="${3:-0}"  # Default priority is 0

    # Skip alerts during sleep hours
    if is_sleep_hours; then
        echo "Sleep hours (10PM - 8AM): Alert suppressed"
        echo "Would have sent: $title - $message"
        return 0
    fi

    # Send the alert via Pushover API
    curl -s \
        --form-string "token=$PUSHOVER_API_TOKEN" \
        --form-string "user=$PUSHOVER_USER_KEY" \
        --form-string "title=$title" \
        --form-string "message=$message" \
        --form-string "priority=$priority" \
        https://api.pushover.net/1/messages.json
    
    return $?
}

# Log a message
function log_message() {
    local message="$1"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $message" >> "$LOG_FILE"
    echo "$message"
}

# Check failure count from logs
function get_failure_count() {
    local failures=$(grep -c "Error running JSNL processor" "$LOG_FILE")
    echo "$failures"
}

# Try to restart network
function restart_network() {
    log_message "Attempting to restart network services..."
    systemctl restart NetworkManager.service
    sleep 5
}

# Try to restart MariaDB
function restart_mariadb() {
    log_message "Attempting to restart MariaDB..."
    systemctl restart mariadb.service
    sleep 5
}

# Check if MariaDB is running
function check_mariadb() {
    if ! systemctl is-active --quiet mariadb.service; then
        log_message "MariaDB is not running. Attempting to start it..."
        systemctl start mariadb.service
        sleep 5
        if ! systemctl is-active --quiet mariadb.service; then
            log_message "Failed to start MariaDB"
            return 1
        fi
    fi
    return 0
}

# Main recovery procedure
function perform_recovery() {
    log_message "Starting recovery procedure for JSNL Processor"
    
    # 1. Check and restart MariaDB if needed
    if ! check_mariadb; then
        log_message "MariaDB recovery failed"
        send_alert "JSNL Processor Failure" "Failed to recover MariaDB service. Manual intervention required." 1
        return 1
    fi
    
    # 2. Try restarting network services
    restart_network
    
    # 3. Restart the JSNL processor service
    log_message "Restarting JSNL processor service..."
    systemctl reset-failed jsnl_processor.service
    systemctl restart jsnl_processor.service
    
    # 4. Check if service started successfully
    sleep 5
    if ! systemctl is-active --quiet jsnl_processor.service; then
        log_message "Failed to restart JSNL processor service"
        send_alert "JSNL Processor Failure" "Service failed and did not recover after automatic intervention. Check logs." 1
        return 1
    fi
    
    log_message "Recovery procedure completed successfully"
    return 0
}

# Main script execution
log_message "Recovery script triggered"

# Get failure count
failures=$(get_failure_count)
log_message "Detected $failures failures in logs"

# Perform recovery
if perform_recovery; then
    log_message "Recovery successful"
    send_alert "JSNL Processor Recovery" "Service was automatically recovered after failure" 0
else
    log_message "Recovery failed"
    # Critical alert already sent in perform_recovery function
fi

exit 0 