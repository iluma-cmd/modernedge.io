# Email Enrichment Service - Deployment Guide

## Background Service Deployment

The Email Enrichment Service has been optimized for production background service deployment with comprehensive health monitoring, graceful shutdown, and robust error handling.

## Service Modes

### 1. Full Service Mode (Recommended)
Runs both background job processor and health monitoring server:
```bash
python -m src.main serve --port 8000 --host 0.0.0.0
```

### 2. Daemon Mode
Runs only the background job processor (no health server):
```bash
python -m src.main daemon
```

### 3. Health Monitoring Only
Runs only the health monitoring server:
```bash
python -m src.main health-server --port 8000 --host 0.0.0.0
```

## Health Endpoints

The service provides comprehensive health monitoring endpoints:

- **`/health`** - Comprehensive health check with component status
- **`/health/live`** - Kubernetes-style liveness probe
- **`/health/ready`** - Kubernetes-style readiness probe  
- **`/metrics`** - Service metrics and statistics

## Docker Deployment

### Build and Run
```bash
# Build the image
docker build -t email-enrichment-service .

# Run with environment variables
docker run -d \
  --name email-enrichment \
  -p 8000:8000 \
  -e SUPABASE_URL="your_supabase_url" \
  -e SUPABASE_SERVICE_ROLE_KEY="your_service_key" \
  -e HUNTER_API_KEY="your_hunter_key" \
  -e PERPLEXITY_API_KEY="your_perplexity_key" \
  -e LOG_LEVEL="INFO" \
  -v /host/logs:/app/logs \
  email-enrichment-service
```

### Health Check
```bash
# Check service health
curl http://localhost:8000/health

# Check if service is ready
curl http://localhost:8000/health/ready

# Get metrics
curl http://localhost:8000/metrics
```

## Render.com Deployment

The service is configured for Render.com deployment with the provided `render.yaml`:

1. **Service Type**: Web service with health checks
2. **Health Check Path**: `/health/live`
3. **Auto-scaling**: Based on health status
4. **Environment Variables**: Configure via Render dashboard

## Linux System Service

### Installation
```bash
# Copy service file
sudo cp scripts/email-enrichment.service /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable service
sudo systemctl enable email-enrichment

# Start service
sudo systemctl start email-enrichment

# Check status
sudo systemctl status email-enrichment
```

### Logs
```bash
# View logs
sudo journalctl -u email-enrichment -f

# View service logs
tail -f /var/log/email-enrichment/service.log

# View error logs
tail -f /var/log/email-enrichment/errors.log
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SUPABASE_URL` | Supabase project URL | Required |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase service role key | Required |
| `HUNTER_API_KEY` | Hunter.io API key | Required |
| `PERPLEXITY_API_KEY` | Perplexity API key | Optional |
| `LOG_LEVEL` | Logging level | INFO |
| `MAX_WORKERS` | Maximum concurrent workers | 5 |
| `BATCH_SIZE` | Processing batch size | 10 |
| `JOB_POLLING_INTERVAL` | Job polling interval (seconds) | 30 |
| `HEALTH_CHECK_PORT` | Health monitoring port | 8000 |
| `LOG_FILE_ENABLED` | Enable file logging | true |
| `LOG_FILE_PATH` | Log file directory | logs |

### Production Configuration

For production deployment, ensure:

1. **Resource Limits**: Set appropriate memory and CPU limits
2. **Log Rotation**: Configured automatically with retention policies
3. **Health Monitoring**: Enable health checks for auto-scaling
4. **Security**: Use non-root user, restrict file permissions
5. **Backup**: Regular database backups of job status

## Monitoring and Alerting

### Health Check Integration

The service provides multiple health check endpoints for integration with:
- **Kubernetes**: Liveness and readiness probes
- **Docker**: Health check commands
- **Load Balancers**: Health status monitoring
- **Monitoring Tools**: Prometheus-style metrics

### Key Metrics to Monitor

- Service health status (`/health`)
- Database connectivity
- Job queue status and processing rates
- API rate limits and errors
- Memory and CPU usage
- Log error rates

## Troubleshooting

### Common Issues

1. **Service Won't Start**
   - Check configuration validation: `python scripts/start_service.py --config-check`
   - Verify environment variables
   - Check database connectivity

2. **Jobs Not Processing**
   - Check job queue status: `curl http://localhost:8000/metrics`
   - Verify API keys and rate limits
   - Check service logs for errors

3. **High Memory Usage**
   - Reduce `MAX_WORKERS` and `BATCH_SIZE`
   - Monitor job processing patterns
   - Check for memory leaks in logs

### Debug Mode

Enable debug mode for development:
```bash
export DEBUG_MODE=true
export LOG_LEVEL=DEBUG
python -m src.main serve
```

## Scaling Considerations

### Horizontal Scaling
- Run multiple service instances
- Use external job queue (Redis) for coordination
- Load balance health endpoints

### Vertical Scaling
- Adjust `MAX_WORKERS` based on CPU cores
- Increase `BATCH_SIZE` for higher throughput
- Monitor memory usage and set limits

## Security Best Practices

1. **Environment Variables**: Use secure secret management
2. **Network Security**: Restrict health endpoint access
3. **User Permissions**: Run as non-privileged user
4. **Log Security**: Secure log file access
5. **API Keys**: Rotate keys regularly
6. **Database**: Use connection pooling and SSL

## Backup and Recovery

### Database Backup
- Regular backups of job status and results
- Point-in-time recovery capability
- Test restore procedures

### Configuration Backup
- Version control environment configurations
- Document deployment procedures
- Maintain rollback procedures
