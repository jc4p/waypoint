#!/usr/bin/env python3
import os
import time
import redis
import logging
import struct
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import signal
import sys
from collections import defaultdict

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
        self.stats = defaultdict(lambda: {
            'count': 0, 
            'total_latency': 0, 
            'min_latency': float('inf'), 
            'max_latency': 0,
            'latencies': []  # Store recent latencies for percentile calculation
        })
        
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
                    
    def decode_varint(self, data: bytes, offset: int) -> Tuple[int, int]:
        """Decode a protobuf varint and return (value, new_offset)"""
        result = 0
        shift = 0
        while offset < len(data):
            byte = data[offset]
            offset += 1
            result |= (byte & 0x7F) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
        return result, offset
        
    def parse_hub_event(self, data: bytes) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Parse HubEvent protobuf to extract timestamp and FID
        Returns: (event_timestamp_ms, cast_timestamp_ms, fid)
        """
        try:
            offset = 0
            event_timestamp = None
            cast_timestamp = None
            fid = None
            
            while offset < len(data):
                # Read field tag
                tag, offset = self.decode_varint(data, offset)
                if offset >= len(data):
                    break
                    
                field_number = tag >> 3
                wire_type = tag & 0x7
                
                # Field 1: type (varint)
                if field_number == 1 and wire_type == 0:
                    event_type, offset = self.decode_varint(data, offset)
                    
                # Field 2: id (varint) - this is the event timestamp
                elif field_number == 2 and wire_type == 0:
                    event_timestamp, offset = self.decode_varint(data, offset)
                    
                # Field 3: body (length-delimited)
                elif field_number == 3 and wire_type == 2:
                    length, offset = self.decode_varint(data, offset)
                    body_end = offset + length
                    
                    # Parse the body to find message data
                    body_offset = offset
                    while body_offset < body_end:
                        body_tag, body_offset = self.decode_varint(data, body_offset)
                        if body_offset >= body_end:
                            break
                            
                        body_field = body_tag >> 3
                        body_wire = body_tag & 0x7
                        
                        # Look for message field in body
                        if body_field == 1 and body_wire == 2:
                            msg_length, body_offset = self.decode_varint(data, body_offset)
                            msg_end = body_offset + msg_length
                            
                            # Parse message
                            msg_offset = body_offset
                            while msg_offset < msg_end:
                                msg_tag, msg_offset = self.decode_varint(data, msg_offset)
                                if msg_offset >= msg_end:
                                    break
                                    
                                msg_field = msg_tag >> 3
                                msg_wire = msg_tag & 0x7
                                
                                # Look for data field in message
                                if msg_field == 1 and msg_wire == 2:
                                    data_length, msg_offset = self.decode_varint(data, msg_offset)
                                    data_end = msg_offset + data_length
                                    
                                    # Parse message data
                                    data_offset = msg_offset
                                    while data_offset < data_end:
                                        data_tag, data_offset = self.decode_varint(data, data_offset)
                                        if data_offset >= data_end:
                                            break
                                            
                                        data_field = data_tag >> 3
                                        data_wire = data_tag & 0x7
                                        
                                        # Field 2: fid (varint)
                                        if data_field == 2 and data_wire == 0:
                                            fid, data_offset = self.decode_varint(data, data_offset)
                                            
                                        # Field 3: timestamp (varint)
                                        elif data_field == 3 and data_wire == 0:
                                            cast_timestamp, data_offset = self.decode_varint(data, data_offset)
                                            
                                        else:
                                            # Skip unknown fields
                                            if data_wire == 0:  # varint
                                                _, data_offset = self.decode_varint(data, data_offset)
                                            elif data_wire == 2:  # length-delimited
                                                length, data_offset = self.decode_varint(data, data_offset)
                                                data_offset += length
                                            else:
                                                data_offset = data_end
                                                
                                    msg_offset = data_end
                                else:
                                    # Skip unknown message fields
                                    if msg_wire == 0:
                                        _, msg_offset = self.decode_varint(data, msg_offset)
                                    elif msg_wire == 2:
                                        length, msg_offset = self.decode_varint(data, msg_offset)
                                        msg_offset += length
                                    else:
                                        msg_offset = msg_end
                                        
                            body_offset = msg_end
                        else:
                            # Skip unknown body fields
                            if body_wire == 0:
                                _, body_offset = self.decode_varint(data, body_offset)
                            elif body_wire == 2:
                                length, body_offset = self.decode_varint(data, body_offset)
                                body_offset += length
                            else:
                                body_offset = body_end
                                
                    offset = body_end
                else:
                    # Skip unknown fields
                    if wire_type == 0:  # varint
                        _, offset = self.decode_varint(data, offset)
                    elif wire_type == 2:  # length-delimited
                        length, offset = self.decode_varint(data, offset)
                        offset += length
                    else:
                        # Can't skip other wire types safely
                        break
                        
            # Convert timestamps to milliseconds
            if event_timestamp:
                event_timestamp = event_timestamp * 1000  # Assuming it's in seconds
            if cast_timestamp:
                cast_timestamp = cast_timestamp * 1000  # Assuming it's in seconds
                
            return event_timestamp, cast_timestamp, fid
            
        except Exception as e:
            logger.error(f"Error parsing protobuf: {e}")
            return None, None, None
            
    def process_message(self, stream_key: str, message_id: str, data: Dict[str, bytes]):
        """Process a single message and calculate latency"""
        try:
            # Get the raw protobuf data
            raw_data = data.get(b'data', b'')
            
            # Parse the HubEvent
            event_timestamp_ms, cast_timestamp_ms, fid = self.parse_hub_event(raw_data)
            
            if not all([cast_timestamp_ms, fid]):
                logger.debug(f"Could not extract timestamp/FID from message {message_id}")
                return
                
            # Calculate latencies
            current_time_ms = int(time.time() * 1000)
            
            # End-to-end latency: from cast creation to Python consumer
            e2e_latency_ms = current_time_ms - cast_timestamp_ms
            
            # Processing latency: from hub event to Python consumer
            processing_latency_ms = None
            if event_timestamp_ms:
                processing_latency_ms = current_time_ms - event_timestamp_ms
            
            # Update statistics
            stats = self.stats[fid]
            stats['count'] += 1
            stats['total_latency'] += e2e_latency_ms
            stats['min_latency'] = min(stats['min_latency'], e2e_latency_ms)
            stats['max_latency'] = max(stats['max_latency'], e2e_latency_ms)
            
            # Keep last 1000 latencies for percentile calculation
            stats['latencies'].append(e2e_latency_ms)
            if len(stats['latencies']) > 1000:
                stats['latencies'].pop(0)
            
            # Log the latency
            log_msg = f"Cast from FID {fid} - E2E Latency: {e2e_latency_ms}ms"
            if processing_latency_ms:
                log_msg += f", Processing Latency: {processing_latency_ms}ms"
            log_msg += f" (Message ID: {message_id})"
            logger.info(log_msg)
            
            # Log aggregate stats every 100 messages
            total_messages = sum(s['count'] for s in self.stats.values())
            if total_messages % 100 == 0:
                self.print_stats()
                
        except Exception as e:
            logger.error(f"Error processing message {message_id}: {e}")
            
    def calculate_percentile(self, latencies: List[int], percentile: float) -> float:
        """Calculate percentile from a list of latencies"""
        if not latencies:
            return 0
        sorted_latencies = sorted(latencies)
        index = int(len(sorted_latencies) * percentile / 100)
        return sorted_latencies[min(index, len(sorted_latencies) - 1)]
            
    def print_stats(self):
        """Print aggregate statistics"""
        logger.info("=== Latency Statistics ===")
        
        total_messages = 0
        total_latency = 0
        all_latencies = []
        
        for fid, stats in self.stats.items():
            if stats['count'] > 0:
                avg_latency = stats['total_latency'] / stats['count']
                p50 = self.calculate_percentile(stats['latencies'], 50)
                p95 = self.calculate_percentile(stats['latencies'], 95)
                p99 = self.calculate_percentile(stats['latencies'], 99)
                
                logger.info(f"FID {fid}: Count={stats['count']}, "
                          f"Avg={avg_latency:.2f}ms, Min={stats['min_latency']}ms, "
                          f"Max={stats['max_latency']}ms, "
                          f"P50={p50}ms, P95={p95}ms, P99={p99}ms")
                
                total_messages += stats['count']
                total_latency += stats['total_latency']
                all_latencies.extend(stats['latencies'])
                
        if total_messages > 0:
            overall_avg = total_latency / total_messages
            overall_p50 = self.calculate_percentile(all_latencies, 50)
            overall_p95 = self.calculate_percentile(all_latencies, 95)
            overall_p99 = self.calculate_percentile(all_latencies, 99)
            
            logger.info(f"Overall: Total Messages={total_messages}, "
                      f"Avg Latency={overall_avg:.2f}ms, "
                      f"P50={overall_p50}ms, P95={overall_p95}ms, P99={overall_p99}ms")
            
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
        logger.info(f"Connecting to Redis at: {self.redis_url}")
        
        # Test Redis connection
        try:
            self.redis_client.ping()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return
        
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
                if keys:
                    logger.info(f"Found {len(keys)} cast stream(s)")
                    # Re-create consumer groups for new streams
                    self.create_consumer_groups()
                
        if keys and self.running:
            logger.info(f"Found {len(keys)} cast stream(s)")
            # For simplicity, consume from the first stream found
            self.consume_stream(keys[0])
            
        # Print final statistics
        logger.info("Shutting down...")
        self.print_stats()
        
if __name__ == "__main__":
    consumer = CastLatencyConsumer()
    consumer.run()