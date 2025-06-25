#!/usr/bin/env python3
import os
import time
import json
import redis
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import signal
import sys
from collections import defaultdict
import asyncio

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CastLatencyConsumer:
    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.redis_client = redis.from_url(self.redis_url, decode_responses=False)
        self.consumer_group = "python-latency-consumer"
        self.consumer_name = f"consumer-{os.getpid()}"
        self.running = True
        self.stats = defaultdict(lambda: {'count': 0, 'total_latency': 0, 'min_latency': float('inf'), 'max_latency': 0})
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    def create_consumer_groups(self):
        """Create consumer groups for the casts stream"""
        stream_key = "hub:*:stream:casts"
        
        # Find all matching stream keys
        keys = self.redis_client.keys(stream_key)
        
        for key in keys:
            try:
                # Try to create the consumer group
                self.redis_client.xgroup_create(key, self.consumer_group, id='0')
                logger.info(f"Created consumer group '{self.consumer_group}' for stream {key.decode()}")
            except redis.ResponseError as e:
                if "BUSYGROUP" in str(e):
                    logger.debug(f"Consumer group already exists for {key.decode()}")
                else:
                    logger.error(f"Error creating consumer group: {e}")
                    
    def parse_cast_timestamp(self, data: bytes) -> Tuple[int, int]:
        """Parse the cast timestamp from the protobuf data
        Returns: (cast_timestamp_ms, fid)
        """
        try:
            # This is a simplified parser - in production you'd use proper protobuf parsing
            # For now, we'll look for timestamp patterns in the data
            
            # The timestamp is typically stored as a Unix timestamp in the protobuf
            # We need to properly decode the HubEvent protobuf to get the exact timestamp
            # For this demo, we'll use the current time as a placeholder
            
            # In a real implementation, you would:
            # 1. Import the protobuf definitions
            # 2. Decode the HubEvent message
            # 3. Extract event.body.merge_message_body.message.data.timestamp
            
            # Placeholder: return current time and FID 0
            return int(time.time() * 1000), 0
            
        except Exception as e:
            logger.error(f"Error parsing cast timestamp: {e}")
            return int(time.time() * 1000), 0
            
    def process_message(self, stream_key: str, message_id: str, data: Dict[str, bytes]):
        """Process a single message and calculate latency"""
        try:
            # Get the raw protobuf data
            raw_data = data.get(b'data', b'')
            
            # Parse timestamp and FID from the cast
            cast_timestamp_ms, fid = self.parse_cast_timestamp(raw_data)
            
            # Calculate latency
            current_time_ms = int(time.time() * 1000)
            latency_ms = current_time_ms - cast_timestamp_ms
            
            # Update statistics
            stats = self.stats[fid]
            stats['count'] += 1
            stats['total_latency'] += latency_ms
            stats['min_latency'] = min(stats['min_latency'], latency_ms)
            stats['max_latency'] = max(stats['max_latency'], latency_ms)
            
            # Log the latency
            logger.info(f"Cast from FID {fid} - Latency: {latency_ms}ms (Message ID: {message_id})")
            
            # Log aggregate stats every 100 messages
            total_messages = sum(s['count'] for s in self.stats.values())
            if total_messages % 100 == 0:
                self.print_stats()
                
        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}")
            
    def print_stats(self):
        """Print aggregate statistics"""
        logger.info("=== Latency Statistics ===")
        
        total_messages = 0
        total_latency = 0
        
        for fid, stats in self.stats.items():
            if stats['count'] > 0:
                avg_latency = stats['total_latency'] / stats['count']
                logger.info(f"FID {fid}: Count={stats['count']}, Avg={avg_latency:.2f}ms, "
                          f"Min={stats['min_latency']}ms, Max={stats['max_latency']}ms")
                
                total_messages += stats['count']
                total_latency += stats['total_latency']
                
        if total_messages > 0:
            overall_avg = total_latency / total_messages
            logger.info(f"Overall: Total Messages={total_messages}, Avg Latency={overall_avg:.2f}ms")
            
    def consume_stream(self, stream_key: str):
        """Consume messages from a specific stream"""
        logger.info(f"Starting to consume from stream: {stream_key.decode()}")
        
        while self.running:
            try:
                # Read messages from the stream
                messages = self.redis_client.xreadgroup(
                    self.consumer_group,
                    self.consumer_name,
                    {stream_key: '>'},
                    count=10,
                    block=1000  # Block for 1 second
                )
                
                # Process messages
                for stream, stream_messages in messages:
                    for message_id, data in stream_messages:
                        self.process_message(stream.decode(), message_id.decode(), data)
                        
                        # Acknowledge the message
                        self.redis_client.xack(stream, self.consumer_group, message_id)
                        
            except redis.ConnectionError as e:
                logger.error(f"Redis connection error: {e}")
                time.sleep(5)  # Wait before reconnecting
            except Exception as e:
                logger.error(f"Error consuming stream: {e}")
                time.sleep(1)
                
    def run(self):
        """Main run loop"""
        logger.info("Starting Cast Latency Consumer...")
        
        # Create consumer groups
        self.create_consumer_groups()
        
        # Find all cast streams
        stream_pattern = "hub:*:stream:casts"
        keys = self.redis_client.keys(stream_pattern)
        
        if not keys:
            logger.warning(f"No streams found matching pattern: {stream_pattern}")
            logger.info("Waiting for streams to be created...")
            
            # Wait for streams to appear
            while self.running and not keys:
                time.sleep(5)
                keys = self.redis_client.keys(stream_pattern)
                
        logger.info(f"Found {len(keys)} cast stream(s)")
        
        # For simplicity, consume from the first stream found
        # In production, you might want to consume from multiple streams concurrently
        if keys and self.running:
            self.consume_stream(keys[0])
            
        # Print final statistics
        logger.info("Shutting down...")
        self.print_stats()
        
if __name__ == "__main__":
    consumer = CastLatencyConsumer()
    consumer.run()