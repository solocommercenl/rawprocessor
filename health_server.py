"""
health_server.py

Enhanced health check HTTP server for rawprocessor service monitoring.
Supports configurable host/port and optional authentication.

FIXED: All BPM field references corrected throughout monitoring and configuration.
"""

import asyncio
import sys
import base64
from datetime import datetime, timedelta
from aiohttp import web, ClientTimeout
from motor.motor_asyncio import AsyncIOMotorClient
import json
import psutil

from config import config

class HealthChecker:
    def __init__(self):
        self.client = None
        self.db = None
        self.start_time = datetime.utcnow()
        
    async def initialize(self):
        """Initialize database connection for health checks."""
        try:
            self.client = AsyncIOMotorClient(
                config.MONGO_URI, 
                serverSelectionTimeoutMS=config.HEALTH_TIMEOUT
            )
            self.db = self.client[config.MONGO_DB]
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
            
            # Check for stuck jobs (processing for more than configured timeout)
            timeout_minutes = config.QUEUE_MAX_PROCESSING_TIME // 60
            timeout_ago = datetime.utcnow() - timedelta(minutes=timeout_minutes)
            stuck_jobs = await self.db.processing_queue.count_documents({
                "status": "processing",
                "started_at": {"$lt": timeout_ago}
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
                "warnings": warnings,
                "configuration": {
                    "max_workers": config.QUEUE_MAX_WORKERS,
                    "batch_size": config.QUEUE_BATCH_SIZE,
                    "timeout_minutes": timeout_minutes
                }
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

    async def check_configuration(self):
        """Check configuration health and consistency."""
        try:
            warnings = []
            issues = []
            
            # Check critical configuration
            if not config.MONGO_URI:
                issues.append("MONGO_URI not configured")
            
            if config.QUEUE_MAX_WORKERS < 1:
                issues.append(f"QUEUE_MAX_WORKERS ({config.QUEUE_MAX_WORKERS}) must be at least 1")
            
            if config.MIN_IMAGES_REQUIRED < 1:
                issues.append(f"MIN_IMAGES_REQUIRED ({config.MIN_IMAGES_REQUIRED}) must be at least 1")
            
            # Check for potential issues
            if config.QUEUE_MAX_WORKERS > 20:
                warnings.append(f"High worker count ({config.QUEUE_MAX_WORKERS}) may cause resource issues")
            
            if config.HEALTH_HOST == "0.0.0.0" and not config.HEALTH_AUTH_USER:
                warnings.append("Health server exposed publicly without authentication")
            
            if config.CLEANUP_INTERVAL_HOURS < 1:
                warnings.append(f"Very frequent cleanup interval ({config.CLEANUP_INTERVAL_HOURS}h)")
            
            status = "error" if issues else ("warning" if warnings else "healthy")
            
            return {
                "status": status,
                "issues": issues,
                "warnings": warnings,
                "config_summary": config.summary()
            }
        except Exception as ex:
            return {"status": "error", "message": str(ex)}

# Global health checker instance
health_checker = HealthChecker()

def check_auth(request):
    """Check basic authentication if configured."""
    if not config.HEALTH_AUTH_USER or not config.HEALTH_AUTH_PASS:
        return True  # No auth required
    
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Basic '):
        return False
    
    try:
        # Decode basic auth
        encoded_credentials = auth_header[6:]  # Remove 'Basic '
        decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
        username, password = decoded_credentials.split(':', 1)
        
        return username == config.HEALTH_AUTH_USER and password == config.HEALTH_AUTH_PASS
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

@web.middleware
async def security_middleware(request, handler):
    """Security headers middleware."""
    response = await handler(request)
    
    # Add security headers
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    
    # Warn about public exposure without auth
    if config.HEALTH_HOST == "0.0.0.0" and not config.HEALTH_AUTH_USER:
        response.headers['X-Security-Warning'] = 'Public exposure without authentication'
    
    return response

async def health_endpoint(request):
    """Main health check endpoint."""
    try:
        # Run all health checks
        mongodb_health = await health_checker.check_mongodb()
        queue_health = await health_checker.check_processing_queue()
        wp_health = await health_checker.check_wp_queues()
        system_health = await health_checker.check_system_resources()
        config_health = await health_checker.check_configuration()
        
        # Determine overall status
        statuses = [
            mongodb_health.get("status"),
            queue_health.get("status"),
            wp_health.get("status"),
            system_health.get("status"),
            config_health.get("status")
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
                "host": config.HEALTH_HOST,
                "port": config.HEALTH_PORT,
                "auth_enabled": bool(config.HEALTH_AUTH_USER)
            },
            "checks": {
                "mongodb": mongodb_health,
                "processing_queue": queue_health,
                "wp_queues": wp_health,
                "system": system_health,
                "configuration": config_health
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
            f'rawprocessor_config_max_workers {config.QUEUE_MAX_WORKERS}',
            f'rawprocessor_config_batch_size {config.QUEUE_BATCH_SIZE}',
            f'rawprocessor_config_min_images {config.MIN_IMAGES_REQUIRED}',
        ]
        
        return web.Response(text='\n'.join(metrics) + '\n', content_type='text/plain')
        
    except Exception as ex:
        return web.Response(text=f'# Error: {ex}\n', content_type='text/plain', status=503)

async def config_endpoint(request):
    """Configuration information endpoint."""
    try:
        config_summary = config.summary()
        
        # Redact sensitive information
        if 'database' in config_summary and 'uri_configured' in config_summary['database']:
            config_summary['database']['uri'] = '***REDACTED***' if config_summary['database']['uri_configured'] else 'NOT_SET'
        
        return web.json_response({
            "configuration": config_summary,
            "timestamp": datetime.utcnow().isoformat()
        })
    except Exception as ex:
        return web.json_response({"error": str(ex)}, status=500)

async def info_endpoint(request):
    """Information endpoint showing configuration."""
    info = {
        "service": "rawprocessor-health",
        "version": "1.0.0",
        "host": config.HEALTH_HOST,
        "port": config.HEALTH_PORT,
        "auth_enabled": bool(config.HEALTH_AUTH_USER),
        "endpoints": {
            "/health": "Comprehensive health check",
            "/ready": "Simple readiness check",
            "/metrics": "Prometheus metrics",
            "/config": "Configuration summary",
            "/info": "Service information"
        },
        "features": {
            "batch_processing": config.ENABLE_BATCH_PROCESSING,
            "periodic_cleanup": config.ENABLE_PERIODIC_CLEANUP,
            "performance_monitoring": config.ENABLE_PERFORMANCE_MONITORING
        }
    }
    return web.json_response(info)

async def init_app():
    """Initialize the health check web application."""
    await health_checker.initialize()
    
    app = web.Application()
    
    # Add security middleware
    app.middlewares.append(security_middleware)
    
    # Add auth middleware if authentication is configured
    if config.HEALTH_AUTH_USER and config.HEALTH_AUTH_PASS:
        app.middlewares.append(auth_middleware)
        print(f"Authentication enabled for user: {config.HEALTH_AUTH_USER}")
    elif config.HEALTH_HOST == "0.0.0.0":
        print("⚠️  WARNING: Health server exposed publicly WITHOUT authentication!")
    
    app.router.add_get('/health', health_endpoint)
    app.router.add_get('/ready', ready_endpoint)
    app.router.add_get('/metrics', metrics_endpoint)
    app.router.add_get('/config', config_endpoint)
    app.router.add_get('/info', info_endpoint)
    app.router.add_get('/', info_endpoint)  # Default route
    
    return app

async def main():
    """Main entry point."""
    try:
        app = await init_app()
        
        print(f"Starting health check server on {config.HEALTH_HOST}:{config.HEALTH_PORT}")
        print("Endpoints available:")
        print("  /health  - Comprehensive health check")
        print("  /ready   - Simple readiness check")
        print("  /metrics - Prometheus metrics")
        print("  /config  - Configuration summary")
        print("  /info    - Service information")
        
        if config.HEALTH_HOST == "0.0.0.0":
            if config.HEALTH_AUTH_USER:
                print("🔒 Server listening on ALL interfaces with authentication")
            else:
                print("⚠️  Server listening on ALL interfaces WITHOUT authentication!")
                print("   Consider setting HEALTH_AUTH_USER and HEALTH_AUTH_PASS")
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(runner, config.HEALTH_HOST, config.HEALTH_PORT)
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
    if not config.ENABLE_HEALTH_SERVER:
        print("Health server disabled by configuration (ENABLE_HEALTH_SERVER=false)")
        sys.exit(0)
    
    asyncio.run(main())