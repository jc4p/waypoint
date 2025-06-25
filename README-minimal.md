# Minimal Waypoint Setup for FID-Filtered Cast Streaming

This setup creates a lightweight Waypoint instance that:
- Connects to your Snapchain instance at 35.94.156.163:3381
- Filters casts at the source to only process specific FIDs
- Stores messages in Redis streams (no PostgreSQL needed)
- Measures end-to-end latency with Python consumer

## Prerequisites

- Docker and Docker Compose installed
- Redis running on the host machine (port 6379)
- At least 2GB RAM available

## Quick Start

1. **Configure FIDs to monitor**:
   Edit `.env.minimal` and set your allowed FIDs:
   ```bash
   WAYPOINT_FID_FILTER__ALLOWED_FIDS=3,5650,2,6529,7143,15983,1214,2433,4036,576,12142
   ```

2. **Build and start the services**:
   ```bash
   # Use the minimal environment file
   cp .env.minimal .env
   
   # Build and start
   sudo docker compose -f docker-compose.minimal.yml up -d --build
   ```

3. **Monitor the streams**:
   ```bash
   # View logs
   sudo docker compose -f docker-compose.minimal.yml logs -f
   
   # Run the stream monitor
   cd python-consumer
   python3 monitor.py
   ```

4. **Check latency statistics**:
   The Python consumer will log latency statistics every 100 messages:
   - End-to-end latency (cast creation to Python consumer)
   - Per-FID statistics (count, avg, min, max, percentiles)
   - Overall statistics

## Architecture

```
Snapchain (35.94.156.163:3381)
    ↓
Waypoint (FID Filter)
    ↓
Redis Streams (localhost:6379)
    ↓
Python Consumer (Latency Measurement)
```

## Configuration

### FID Filtering
- Set `WAYPOINT_FID_FILTER__ENABLED=true` to enable filtering
- List allowed FIDs in `WAYPOINT_FID_FILTER__ALLOWED_FIDS` (comma-separated)
- Only casts from these FIDs will be stored in Redis

### Memory Optimization
- No PostgreSQL database (Redis only)
- Reduced Redis pool size (20 connections)
- Smaller batch sizes (50 messages)
- Docker memory limits (500MB for Waypoint, 100MB for Python)

### Performance Tuning
- Adjust `WAYPOINT_REDIS__BATCH_SIZE` for throughput vs latency trade-off
- Modify `WAYPOINT_REDIS__POOL_SIZE` based on concurrent consumers
- Set Docker memory limits in `docker-compose.minimal.yml`

## Monitoring

### Stream Monitor
```bash
cd python-consumer
python3 monitor.py
```

Shows real-time information about:
- Stream lengths
- Consumer groups
- Pending messages
- Consumer idle times

### Latency Consumer
The Python consumer logs:
- Individual message latencies
- Aggregate statistics (every 100 messages)
- Percentiles (P50, P95, P99)
- Per-FID breakdown

## Troubleshooting

1. **No messages appearing**:
   - Check Redis is running: `redis-cli ping`
   - Verify Snapchain connection in Waypoint logs
   - Ensure FIDs in filter are active

2. **High memory usage**:
   - Reduce `WAYPOINT_REDIS__POOL_SIZE`
   - Lower `WAYPOINT_REDIS__BATCH_SIZE`
   - Add more FIDs to filter (fewer messages)

3. **Connection errors**:
   - Verify Redis is accessible on localhost:6379
   - Check Snapchain URL is correct
   - Ensure no firewall blocking connections

## Stopping the Services

```bash
sudo docker compose -f docker-compose.minimal.yml down
```

## Resource Usage

With default configuration:
- Waypoint: ~200-300MB RAM
- Redis: ~50-100MB RAM (depends on message volume)
- Python consumer: ~50MB RAM
- **Total**: Under 500MB RAM