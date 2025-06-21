"""
health_server.py

Enhanced health check HTTP server for rawprocessor service monitoring.
Supports configurable host/port and optional authentication.

FIXED: Motor database boolean evaluation issues.
"""

import asyncio
import os
import sys
import base64
import hashlib
from datetime import datetime, timedelta
from aiohttp import web, ClientTimeout
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import json
import psutil

# Load environment
load_dotenv()
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB", "autodex")

# Health server configuration from environment
HEALTH_HOST = os.environ.get("HEALTH_HOST", "localhost")  # Set to "0.0.0.0" for all interfaces
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))
HEALTH_AUTH_USER = os.environ.get("HEALTH_AUTH_USER")  # Optional basic auth
HEALTH_AUTH_PASS = os.environ.get("HEALTH_AUTH_PASS")  # Optional basic auth

class HealthChecker:
    def __init__(self):
        self.client = None
        self.db = None
        self.start_time = datetime.utcnow()
        
    async def initialize(self):
        """Initialize database connection for health checks."""
        try:
            self.client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.db = self.client[DB_NAME]
            # Test connection
            await self.client.admin.command('ping')
            print(f"Health server connected to MongoDB at {datetime.utcnow()}")
        except Exception as ex:
            print(f"Health server failed to connect to MongoDB: {ex}")
            self.client = None
            self.db = None

    async def check_mongodb(self):
        """Check MongoDB connectivity and basic operations."""
        if self.client is None:
            return {"status": "error", "message": "No MongoDB connection"}
        
        try:
            # Test basic connectivity
            await self.client.admin.command('ping')
            
            # Test if collections exist and are accessible
            raw_count = await self.db.raw.count_documents({}, limit=1)
            
            return {
                "status": "healthy",
                "ping": "ok",
                "collections_accessible": True,
                "sample_count": raw_count
            }
        except Exception as ex:
            return {"status": "error", "message": str(ex)}

    async def check_processing_queue(self):
        """Check processing queue health."""
        if self.db is None:
            return {"status": "error", "message": "No database connection"}
        
        try:
            # Get queue statistics
            total_jobs = await self.db.processing_queue.count_documents({})
            pending_jobs = await self.db.processing_queue.count_documents({"status": "pending"})
            failed_jobs = await self.db.processing_queue.count_documents({"status": "failed"})
            processing_jobs = await self.db.processing_queue.count_documents({"status": "processing"})
            
            # Check for stuck jobs (processing for more than 30 minutes)
            thirty_min_ago = datetime.utcnow() - timedelta(minutes=30)
            stuck_jobs = await self.db.processing_queue.count_documents({
                "status": "processing",
                "started_at": {"$lt": thirty_min_ago}
            })
            
            status = "healthy"
            warnings = []
            
            if stuck_jobs > 0:
                warnings.append(f"{stuck_jobs} jobs stuck in processing")
                status = "warning"
            
            if failed_jobs > 100:
                warnings.append(f"High number of failed jobs: {failed_jobs}")
                status = "warning"
            
            return {
                "status": status,
                "total_jobs": total_jobs,
                "pending": pending_jobs,
                "processing": processing_jobs,
                "failed": failed_jobs,
                "stuck": stuck_jobs,
                "warnings": warnings
            }
        except Exception as ex:
            return {"status": "error", "message": str(ex)}

    async def check_wp_queues(self):
        """Check WordPress sync queues for all sites."""
        if self.db is None:
            return {"status": "error", "message": "No database connection"}
        
        try:
            # Get all site collections
            collections = await self.db.list_collection_names()
            wp_queues = [col for col in collections if col.startswith("wp_sync_queue_")]
            
            queue_stats = {}
            total_pending = 0
            total_failed = 0
            
            for queue_name in wp_queues:
                site = queue_name.replace("wp_sync_queue_", "")
                collection = self.db[queue_name]
                
                pending = await collection.count_documents({"status": "pending"})
                failed = await collection.count_documents({"status": "failed"})
                
                queue_stats[site] = {
                    "pending": pending,
                    "failed": failed
                }
                
                total_pending += pending
                total_failed += failed
            
            status = "healthy"
            if total_failed > 50:
                status = "warning"
            
            return {
                "status": status,
                "total_pending": total_pending,
                "total_failed": total_failed,
                "sites": queue_stats
            }
        except Exception as ex:
            return {"status": "error", "message": str(ex)}

    async def check_system_resources(self):
        """Check system resource usage."""
        try:
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Disk usage for logs directory
            try:
                disk_usage = psutil.disk_usage('/opt/rawprocessor/logs')
                disk_percent = (disk_usage.used / disk_usage.total) * 100
            except:
                disk_percent = 0
            
            status = "healthy"
            warnings = []
            
            if memory_percent > 85:
                warnings.append(f"High memory usage: {memory_percent:.1f}%")
                status = "warning"
            
            if cpu_percent > 90:
                warnings.append(f"High CPU usage: {cpu_percent:.1f}%")
                status = "warning"
            
            if disk_percent > 85:
                warnings.append(f"High disk usage: {disk_percent:.1f}%")
                status = "warning"
            
            return {
                "status": status,
                "memory_percent": memory_percent,
                "cpu_percent": cpu_percent,
                "disk_percent": disk_percent,
                "warnings": warnings
            }
        except Exception as ex:
            return {"status": "error", "message": str(ex)}

# Global health checker instance
health_checker = HealthChecker()

def check_auth(request):
    """Check basic authentication if configured."""
    if not HEALTH_AUTH_USER or not HEALTH_AUTH_PASS:
        return True  # No auth required
    
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Basic '):
        return False
    
    try:
        # Decode basic auth
        encoded_credentials = auth_header[6:]  # Remove 'Basic '
        decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
        username, password = decoded_credentials.split(':', 1)
        
        return username == HEALTH_AUTH_USER and password == HEALTH_AUTH_PASS
    except:
        return False

@web.middleware
async def auth_middleware(request, handler):
    """Authentication middleware."""
    if not check_auth(request):
        return web.Response(
            text='Authentication required',
            status=401,
            headers={'WWW-Authenticate': 'Basic realm="Rawprocessor Health"'}
        )
    
    return await handler(request)

async def health_endpoint(request):
    """Main health check endpoint."""
    try:
        # Run all health checks
        mongodb_health = await health_checker.check_mongodb()
        queue_health = await health_checker.check_processing_queue()
        wp_health = await health_checker.check_wp_queues()
        system_health = await health_checker.check_system_resources()
        
        # Determine overall status
        statuses = [
            mongodb_health.get("status"),
            queue_health.get("status"),
            wp_health.get("status"),
            system_health.get("status")
        ]
        
        if "error" in statuses:
            overall_status = "error"
        elif "warning" in statuses:
            overall_status = "warning"
        else:
            overall_status = "healthy"
        
        uptime_seconds = (datetime.utcnow() - health_checker.start_time).total_seconds()
        
        response = {
            "status": overall_status,
            "timestamp": datetime.utcnow().isoformat(),
            "uptime_seconds": uptime_seconds,
            "server_info": {
                "host": HEALTH_HOST,
                "port": HEALTH_PORT,
                "auth_enabled": bool(HEALTH_AUTH_USER)
            },
            "checks": {
                "mongodb": mongodb_health,
                "processing_queue": queue_health,
                "wp_queues": wp_health,
                "system": system_health
            }
        }
        
        # Return appropriate HTTP status
        if overall_status == "error":
            return web.json_response(response, status=503)
        elif overall_status == "warning":
            return web.json_response(response, status=200)
        else:
            return web.json_response(response, status=200)
            
    except Exception as ex:
        return web.json_response({
            "status": "error",
            "message": str(ex),
            "timestamp": datetime.utcnow().isoformat()
        }, status=503)

async def ready_endpoint(request):
    """Readiness check for load balancers."""
    try:
        # Simple MongoDB ping
        mongodb_health = await health_checker.check_mongodb()
        
        if mongodb_health.get("status") == "healthy":
            return web.json_response({"status": "ready"}, status=200)
        else:
            return web.json_response({"status": "not_ready"}, status=503)
            
    except Exception as ex:
        return web.json_response({"status": "not_ready", "error": str(ex)}, status=503)

async def metrics_endpoint(request):
    """Prometheus-style metrics endpoint."""
    try:
        queue_health = await health_checker.check_processing_queue()
        wp_health = await health_checker.check_wp_queues()
        system_health = await health_checker.check_system_resources()
        
        uptime_seconds = (datetime.utcnow() - health_checker.start_time).total_seconds()
        
        metrics = [
            f'rawprocessor_uptime_seconds {uptime_seconds}',
            f'rawprocessor_processing_queue_total {queue_health.get("total_jobs", 0)}',
            f'rawprocessor_processing_queue_pending {queue_health.get("pending", 0)}',
            f'rawprocessor_processing_queue_failed {queue_health.get("failed", 0)}',
            f'rawprocessor_wp_queue_pending {wp_health.get("total_pending", 0)}',
            f'rawprocessor_wp_queue_failed {wp_health.get("total_failed", 0)}',
            f'rawprocessor_memory_percent {system_health.get("memory_percent", 0)}',
            f'rawprocessor_cpu_percent {system_health.get("cpu_percent", 0)}',
            f'rawprocessor_disk_percent {system_health.get("disk_percent", 0)}',
        ]
        
        return web.Response(text='\n'.join(metrics) + '\n', content_type='text/plain')
        
    except Exception as ex:
        return web.Response(text=f'# Error: {ex}\n', content_type='text/plain', status=503)

async def info_endpoint(request):
    """Information endpoint showing configuration."""
    info = {
        "service": "rawprocessor-health",
        "version": "1.0.0",
        "host": HEALTH_HOST,
        "port": HEALTH_PORT,
        "auth_enabled": bool(HEALTH_AUTH_USER),
        "endpoints": {
            "/health": "Comprehensive health check",
            "/ready": "Simple readiness check",
            "/metrics": "Prometheus metrics",
            "/info": "Service information"
        }
    }
    return web.json_response(info)

async def init_app():
    """Initialize the health check web application."""
    await health_checker.initialize()
    
    app = web.Application()
    
    # Add auth middleware if authentication is configured
    if HEALTH_AUTH_USER and HEALTH_AUTH_PASS:
        app.middlewares.append(auth_middleware)
        print(f"Authentication enabled for user: {HEALTH_AUTH_USER}")
    
    app.router.add_get('/health', health_endpoint)
    app.router.add_get('/ready', ready_endpoint)
    app.router.add_get('/metrics', metrics_endpoint)
    app.router.add_get('/info', info_endpoint)
    app.router.add_get('/', info_endpoint)  # Default route
    
    return app

async def main():
    """Main entry point."""
    try:
        app = await init_app()
        
        print(f"Starting health check server on {HEALTH_HOST}:{HEALTH_PORT}")
        print("Endpoints available:")
        print("  /health  - Comprehensive health check")
        print("  /ready   - Simple readiness check")
        print("  /metrics - Prometheus metrics")
        print("  /info    - Service information")
        
        if HEALTH_HOST == "0.0.0.0":
            print("⚠️  Server listening on ALL interfaces - ensure proper firewall configuration!")
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(runner, HEALTH_HOST, HEALTH_PORT)
        await site.start()
        
        # Keep the server running
        while True:
            await asyncio.sleep(3600)
            
    except KeyboardInterrupt:
        print("Health server stopped")
    except Exception as ex:
        print(f"Health server error: {ex}")
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())