# Rawprocessor Production Setup Guide

Complete guide for setting up rawprocessor as a production systemd service with monitoring.

## 📋 Prerequisites

- Linux server with systemd
- Python 3.8+
- MongoDB running
- Root/sudo access
- Email server configured (for alerts)

## 🚀 Installation Steps

### 1. Prepare the Installation

```bash
# Navigate to your rawprocessor project directory
cd /path/to/rawprocessor

# Make the installation script executable
chmod +x install_service.sh

# Run the installation (as root)
sudo ./install_service.sh
```

### 2. Configure Environment

Edit the environment file:

```bash
sudo nano /opt/rawprocessor/.env
```

Configure your settings:
```env
# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017
MONGO_DB=autodex

# Add other environment variables as needed
```

### 3. Start the Services

```bash
# Start health check service first
sudo systemctl start rawprocessor-health

# Wait a moment, then start main service
sudo systemctl start rawprocessor

# Check status
rawprocessor-status status
```

### 4. Verify Installation

```bash
# Check service status
rawprocessor-status status

# Check health endpoint
rawprocessor-status health

# View logs
rawprocessor-status logs
```

## 📊 Monitoring & Management

### Available Commands

```bash
# Service management
rawprocessor-status start     # Start services
rawprocessor-status stop      # Stop services
rawprocessor-status restart   # Restart services
rawprocessor-status status    # Show status
rawprocessor-status health    # Check health
rawprocessor-status logs      # View logs

# Manual monitoring
/usr/local/bin/rawprocessor-monitor        # Run health check
/usr/local/bin/rawprocessor-daily-report   # Generate daily report
```

### Health Check Endpoints

The health server runs on `localhost:8080` and provides:

- `GET /health` - Comprehensive health check
- `GET /ready` - Simple readiness check  
- `GET /metrics` - Prometheus-style metrics

### Automated Monitoring

The system automatically:
- **Monitors every 5 minutes** for issues
- **Sends email alerts** for problems
- **Generates daily reports** at 8 AM
- **Cleans up logs** weekly

## 🔧 Configuration

### Email Alerts

Configure email recipient in:
```bash
sudo nano /usr/local/bin/rawprocessor-monitor
sudo nano /usr/local/bin/rawprocessor-daily-report
```

Change this line:
```bash
EMAIL_RECIPIENT="your-email@domain.com"
```

### Resource Limits

Adjust in `/etc/systemd/system/rawprocessor.service`:
```ini
MemoryMax=4G        # Maximum memory
CPUQuota=200%       # CPU limit (200% = 2 cores)
LimitNOFILE=65536   # File descriptors
```

### Log Rotation

Logs are automatically rotated daily and kept for 30 days. Configure in:
```bash
sudo nano /etc/logrotate.d/rawprocessor
```

## 📁 File Locations

```
/opt/rawprocessor/                 # Main installation
├── main.py                       # Main daemon
├── health_server.py              # Health check server
├── .env                          # Environment config
├── logs/                         # Application logs
└── .venv/                        # Python virtual environment

/etc/systemd/system/
├── rawprocessor.service          # Main service
└── rawprocessor-health.service   # Health service

/usr/local/bin/
├── rawprocessor-status           # Management script
├── rawprocessor-monitor          # Monitoring script
├── rawprocessor-daily-report     # Daily reporting
└── rawprocessor-cleanup          # Log cleanup

/etc/logrotate.d/rawprocessor     # Log rotation config
/etc/cron.d/rawprocessor-monitor  # Monitoring cron jobs
```

## 🚨 Troubleshooting

### Service Won't Start

```bash
# Check service status
systemctl status rawprocessor -l

# Check logs
journalctl -u rawprocessor -f

# Verify environment
sudo -u rawprocessor cat /opt/rawprocessor/.env

# Test manually
sudo -u rawprocessor /opt/rawprocessor/.venv/bin/python /opt/rawprocessor/main.py --test-one --site yoursite
```

### Health Check Fails

```bash
# Test health endpoint directly
curl http://localhost:8080/health

# Check health service
systemctl status rawprocessor-health

# Verify MongoDB connection
sudo -u rawprocessor /opt/rawprocessor/.venv/bin/python -c "
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
import os
from dotenv import load_dotenv

load_dotenv('/opt/rawprocessor/.env')
async def test():
    client = AsyncIOMotorClient(os.getenv('MONGO_URI'))
    result = await client.admin.command('ping')
    print('MongoDB OK:', result)

asyncio.run(test())
"
```

### High Resource Usage

```bash
# Check resource usage
systemctl status rawprocessor
top -p $(pgrep -f rawprocessor)

# Adjust limits in service file
sudo nano /etc/systemd/system/rawprocessor.service
sudo systemctl daemon-reload
sudo systemctl restart rawprocessor
```

### Database Issues

```bash
# Check MongoDB status
systemctl status mongod

# Check database connections
sudo -u rawprocessor /opt/rawprocessor/.venv/bin/python /opt/rawprocessor/main.py --status

# Check queue status
sudo -u rawprocessor /opt/rawprocessor/.venv/bin/python /opt/rawprocessor/main.py --queue-status
```

## 🔄 Updates

To update the service:

```bash
# Stop services
sudo systemctl stop rawprocessor rawprocessor-health

# Update code
cd /opt/rawprocessor
sudo -u rawprocessor git pull  # or copy new files

# Update dependencies if needed
sudo -u rawprocessor /opt/rawprocessor/.venv/bin/pip install -r requirements.txt

# Restart services
sudo systemctl start rawprocessor-health
sudo systemctl start rawprocessor

# Verify
rawprocessor-status status
```

## 📈 Performance Tuning

### For High Volume

Adjust these settings in `/etc/systemd/system/rawprocessor.service`:

```ini
# Increase memory limit
MemoryMax=8G

# Increase CPU quota
CPUQuota=400%

# Increase file descriptors
LimitNOFILE=131072
```

### MongoDB Optimization

```bash
# Ensure proper indexes exist
# Connect to MongoDB and run:
# db.raw.createIndex({"listing_status": 1, "_id": 1})
# db.processing_queue.createIndex({"status": 1, "priority": 1, "scheduled_at": 1})
```

## 🛡️ Security

The service runs with restricted permissions:
- Dedicated `rawprocessor` user
- No shell access
- Limited filesystem access
- Private temporary directory
- No new privileges

Environment file permissions:
```bash
# Verify secure permissions
ls -la /opt/rawprocessor/.env
# Should show: -rw------- rawprocessor rawprocessor
```

## 📞 Support

For issues:
1. Check logs: `rawprocessor-status logs`
2. Check health: `rawprocessor-status health`
3. Run monitoring: `/usr/local/bin/rawprocessor-monitor`
4. Review daily reports: `/var/log/rawprocessor-daily-reports.log`