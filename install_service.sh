#!/bin/bash

# install_service.sh - Install rawprocessor as systemd service
# Run as root: sudo ./install_service.sh

set -e

echo "Installing Rawprocessor as systemd service..."

# Configuration
SERVICE_NAME="rawprocessor"
INSTALL_DIR="/opt/rawprocessor"
SERVICE_USER="rawprocessor"
SERVICE_GROUP="rawprocessor"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   log_error "This script must be run as root (use sudo)"
   exit 1
fi

# Check if source directory exists
if [[ ! -d "." ]] || [[ ! -f "main.py" ]]; then
    log_error "Please run this script from the rawprocessor project directory"
    exit 1
fi

# Step 1: Create service user
log_info "Creating service user and group..."
if ! getent group $SERVICE_GROUP > /dev/null 2>&1; then
    groupadd --system $SERVICE_GROUP
    log_info "Created group: $SERVICE_GROUP"
fi

if ! getent passwd $SERVICE_USER > /dev/null 2>&1; then
    useradd --system --gid $SERVICE_GROUP --home-dir $INSTALL_DIR --shell /bin/false $SERVICE_USER
    log_info "Created user: $SERVICE_USER"
fi

# Step 2: Create installation directory
log_info "Setting up installation directory..."
mkdir -p $INSTALL_DIR
mkdir -p $INSTALL_DIR/logs

# Step 3: Copy application files
log_info "Copying application files..."
cp -r . $INSTALL_DIR/
cd $INSTALL_DIR

# Step 4: Set up Python virtual environment
log_info "Setting up Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Add health check dependencies
pip install aiohttp psutil

# Step 5: Set proper ownership and permissions
log_info "Setting file permissions..."
chown -R $SERVICE_USER:$SERVICE_GROUP $INSTALL_DIR
chmod -R 755 $INSTALL_DIR
chmod -R 755 $INSTALL_DIR/logs

# Make main.py and health_server.py executable
chmod +x $INSTALL_DIR/main.py
chmod +x $INSTALL_DIR/health_server.py

# Step 6: Install systemd service file
log_info "Installing systemd service..."
cp $INSTALL_DIR/rawprocessor.service /etc/systemd/system/
systemctl daemon-reload

# Step 7: Install logrotate configuration
log_info "Installing log rotation..."
cp $INSTALL_DIR/rawprocessor.logrotate /etc/logrotate.d/rawprocessor

# Step 8: Set up health check service
log_info "Setting up health check service..."
cat > /etc/systemd/system/rawprocessor-health.service << EOF
[Unit]
Description=Rawprocessor Health Check Server
After=network.target
PartOf=rawprocessor.service

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$INSTALL_DIR
Environment=PATH=$INSTALL_DIR/.venv/bin
ExecStart=$INSTALL_DIR/.venv/bin/python health_server.py
Restart=always
RestartSec=5
EnvironmentFile=$INSTALL_DIR/.env

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload

# Step 9: Enable services
log_info "Enabling services..."
systemctl enable $SERVICE_NAME
systemctl enable rawprocessor-health

# Step 10: Verify environment file
if [[ ! -f $INSTALL_DIR/.env ]]; then
    log_warn "Creating template .env file - please configure it!"
    cat > $INSTALL_DIR/.env << EOF
# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017
MONGO_DB=autodex

# Add other environment variables as needed
EOF
    chown $SERVICE_USER:$SERVICE_GROUP $INSTALL_DIR/.env
    chmod 600 $INSTALL_DIR/.env
fi

# Step 11: Create monitoring script
log_info "Creating monitoring script..."
cat > /usr/local/bin/rawprocessor-status << 'EOF'
#!/bin/bash
# Rawprocessor monitoring script

case "$1" in
    status)
        systemctl status rawprocessor rawprocessor-health
        ;;
    health)
        curl -s http://localhost:8080/health | python3 -m json.tool
        ;;
    logs)
        journalctl -u rawprocessor -f --since "1 hour ago"
        ;;
    restart)
        systemctl restart rawprocessor rawprocessor-health
        echo "Services restarted"
        ;;
    stop)
        systemctl stop rawprocessor rawprocessor-health
        echo "Services stopped"
        ;;
    start)
        systemctl start rawprocessor-health
        sleep 2
        systemctl start rawprocessor
        echo "Services started"
        ;;
    *)
        echo "Usage: $0 {status|health|logs|restart|stop|start}"
        exit 1
        ;;
esac
EOF

chmod +x /usr/local/bin/rawprocessor-status

# Installation complete
log_info "Installation completed successfully!"
echo
echo "Next steps:"
echo "1. Configure $INSTALL_DIR/.env with your settings"
echo "2. Start the services: sudo systemctl start rawprocessor-health rawprocessor"
echo "3. Check status: rawprocessor-status status"
echo "4. View health: rawprocessor-status health"
echo "5. Monitor logs: rawprocessor-status logs"
echo
echo "Service files created:"
echo "  - /etc/systemd/system/rawprocessor.service"
echo "  - /etc/systemd/system/rawprocessor-health.service"
echo "  - /etc/logrotate.d/rawprocessor"
echo "  - /usr/local/bin/rawprocessor-status"
echo
log_warn "Remember to configure your .env file before starting!"