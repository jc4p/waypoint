# Filtered By FID Only Quick Setup Guide

This guide provides copy-paste commands to run a minimal Waypoint setup that filters Farcaster casts by FID and measures end-to-end latency.

## Prerequisites

```bash
# Install Docker if not already installed
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install screen if not already installed
sudo apt-get update && sudo apt-get install -y screen

# Install Python dependencies
sudo apt-get install -y python3-pip python3-venv

# Start Redis on your host machine
sudo apt-get install -y redis-server
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Verify Redis is running
redis-cli ping
# Should return: PONG
```

## Step 1: Configure FIDs to Monitor

```bash
# Navigate to waypoint directory
cd /home/ubuntu/waypoint

# Edit the FID list (comma-separated, no spaces)
nano .env.minimal
```

Find this line and update with your FIDs:
```
WAYPOINT_FID_FILTER__ALLOWED_FIDS=3,5650,2,6529,7143,15983,1214,2433,4036,576,12142
```

Save and exit (Ctrl+X, Y, Enter)

## Step 2: Start Waypoint with Docker Compose

```bash
# Copy the minimal environment file
cp .env.minimal .env

# Build and start Waypoint (will take ~5 minutes first time)
sudo docker compose -f docker-compose.minimal.yml up -d --build

# Check if it's running
sudo docker compose -f docker-compose.minimal.yml ps

# View logs (Ctrl+C to exit)
sudo docker compose -f docker-compose.minimal.yml logs -f waypoint
```

## Step 3: Set Up Python Environment

```bash
# Create Python virtual environment
cd /home/ubuntu/waypoint/python-consumer
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Step 4: Run Consumer in Screen

```bash
# Create a new screen session for the consumer
screen -S consumer

# Inside the screen session, activate venv and run consumer
cd /home/ubuntu/waypoint/python-consumer
source venv/bin/activate
python consumer_protobuf.py

# Detach from screen: Press Ctrl+A, then D
# To reattach later: screen -r consumer
```

## Step 5: Run Monitor in Another Screen

```bash
# Create a new screen session for the monitor
screen -S monitor

# Inside the screen session, activate venv and run monitor
cd /home/ubuntu/waypoint/python-consumer
source venv/bin/activate
python monitor.py

# Detach from screen: Press Ctrl+A, then D
# To reattach later: screen -r monitor
```

## Managing Screen Sessions

```bash
# List all screen sessions
screen -ls

# Reattach to consumer
screen -r consumer

# Reattach to monitor
screen -r monitor

# Kill a screen session (from inside the session)
exit

# Kill a detached screen session
screen -X -S consumer quit
screen -X -S monitor quit
```

## Monitoring and Debugging

### Check Waypoint Logs
```bash
# All logs
sudo docker compose -f docker-compose.minimal.yml logs -f

# Just Waypoint logs
sudo docker compose -f docker-compose.minimal.yml logs -f waypoint

# Last 100 lines
sudo docker compose -f docker-compose.minimal.yml logs --tail 100 waypoint
```

### Check Redis Streams
```bash
# Connect to Redis CLI
redis-cli

# List all streams
KEYS hub:*:stream:*

# Check stream length
XLEN hub:snapchain.farcaster.xyz:stream:casts

# Exit Redis CLI
exit
```

### View Consumer Stats
```bash
# Attach to consumer screen
screen -r consumer

# You'll see latency stats every 100 messages:
# - Per-FID statistics
# - Percentiles (P50, P95, P99)
# - Overall averages
```

## Updating Configuration

### Change FIDs
```bash
# Edit the environment file
nano /home/ubuntu/waypoint/.env

# Update this line with new FIDs:
WAYPOINT_FID_FILTER__ALLOWED_FIDS=1,2,3,4,5

# Restart Waypoint
sudo docker compose -f docker-compose.minimal.yml restart waypoint
```

### Change Snapchain URL
```bash
# Edit the environment file
nano /home/ubuntu/waypoint/.env

# Update this line:
WAYPOINT_HUB__URL=35.94.156.163:3381

# Restart Waypoint
sudo docker compose -f docker-compose.minimal.yml restart waypoint
```

## Stopping Everything

```bash
# Stop Waypoint
sudo docker compose -f docker-compose.minimal.yml down

# Stop consumer (attach and exit)
screen -r consumer
# Press Ctrl+C
exit

# Stop monitor (attach and exit)
screen -r monitor
# Press Ctrl+C or 'q'
exit

# Or kill all screens at once
screen -X -S consumer quit
screen -X -S monitor quit
```

## Performance Tuning

### For Lower Latency
```bash
# Edit .env
nano /home/ubuntu/waypoint/.env

# Decrease batch size (processes messages faster)
WAYPOINT_REDIS__BATCH_SIZE=10

# Restart
sudo docker compose -f docker-compose.minimal.yml restart waypoint
```

### For Lower Memory Usage
```bash
# Edit .env
nano /home/ubuntu/waypoint/.env

# Reduce pool size
WAYPOINT_REDIS__POOL_SIZE=10

# Increase batch size (less frequent processing)
WAYPOINT_REDIS__BATCH_SIZE=100

# Restart
sudo docker compose -f docker-compose.minimal.yml restart waypoint
```

## Troubleshooting

### No messages appearing
```bash
# Check Waypoint is connected to Snapchain
sudo docker compose -f docker-compose.minimal.yml logs waypoint | grep -i "connected\|error"

# Check Redis has streams
redis-cli KEYS "hub:*:stream:*"

# Check FID filter is working
sudo docker compose -f docker-compose.minimal.yml logs waypoint | grep -i "fid filter"
```

### High memory usage
```bash
# Check Docker stats
sudo docker stats

# Reduce memory limits in docker-compose.minimal.yml
nano docker-compose.minimal.yml
# Change mem_limit values

# Restart
sudo docker compose -f docker-compose.minimal.yml up -d
```

### Consumer crashes
```bash
# Check consumer logs
screen -r consumer

# Check Redis connection
redis-cli ping

# Restart consumer
screen -X -S consumer quit
screen -dmS consumer bash -c 'cd /home/ubuntu/waypoint/python-consumer && source venv/bin/activate && python consumer_protobuf.py'
```

## Quick Status Check

```bash
# One-liner to check everything
echo "Waypoint: $(sudo docker compose -f docker-compose.minimal.yml ps waypoint | grep -c "Up")/1 running" && \
echo "Redis: $(redis-cli ping 2>/dev/null || echo "DOWN")" && \
echo "Consumer: $(screen -ls | grep -c consumer)/1 running" && \
echo "Monitor: $(screen -ls | grep -c monitor)/1 running" && \
echo "Streams: $(redis-cli --raw KEYS 'hub:*:stream:*' | wc -l) active"
```

