#!/usr/bin/env python3
import os
import time
import redis
import logging
from datetime import datetime
from typing import Dict
import curses
import signal
import sys

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamMonitor:
    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
        self.running = True
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        self.running = False
        
    def get_stream_info(self, stream_key: str) -> Dict:
        """Get information about a stream"""
        try:
            # Get stream length
            length = self.redis_client.xlen(stream_key)
            
            # Get stream info
            info = self.redis_client.xinfo_stream(stream_key)
            
            # Get consumer groups info
            groups = []
            try:
                groups_info = self.redis_client.xinfo_groups(stream_key)
                for group in groups_info:
                    # Get consumers in this group
                    consumers = self.redis_client.xinfo_consumers(stream_key, group['name'])
                    group['consumers'] = consumers
                    groups.append(group)
            except:
                pass
                
            return {
                'length': length,
                'first_entry': info.get('first-entry'),
                'last_entry': info.get('last-entry'),
                'groups': groups
            }
        except Exception as e:
            logger.error(f"Error getting stream info: {e}")
            return {}
            
    def monitor_with_curses(self, stdscr):
        """Monitor streams using curses for better display"""
        curses.curs_set(0)  # Hide cursor
        stdscr.nodelay(1)  # Non-blocking input
        
        while self.running:
            try:
                stdscr.clear()
                
                # Header
                stdscr.addstr(0, 0, "Waypoint Stream Monitor", curses.A_BOLD)
                stdscr.addstr(1, 0, f"Redis: {self.redis_url}")
                stdscr.addstr(2, 0, f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                stdscr.addstr(3, 0, "-" * 80)
                
                # Find all streams
                stream_pattern = "hub:*:stream:*"
                keys = sorted(self.redis_client.keys(stream_pattern))
                
                row = 5
                for key in keys:
                    if row >= curses.LINES - 2:
                        break
                        
                    info = self.get_stream_info(key)
                    if not info:
                        continue
                        
                    # Stream name
                    stdscr.addstr(row, 0, f"Stream: {key}", curses.A_BOLD)
                    row += 1
                    
                    # Stream stats
                    stdscr.addstr(row, 2, f"Length: {info['length']:,}")
                    row += 1
                    
                    if info.get('last_entry'):
                        last_id = info['last_entry']['id']
                        stdscr.addstr(row, 2, f"Last ID: {last_id}")
                        row += 1
                        
                    # Consumer groups
                    for group in info.get('groups', []):
                        stdscr.addstr(row, 2, f"Group: {group['name']} - Pending: {group['pending']}")
                        row += 1
                        
                        # Show consumers
                        for consumer in group.get('consumers', []):
                            idle_sec = consumer['idle'] / 1000
                            stdscr.addstr(row, 4, 
                                f"Consumer: {consumer['name']} - "
                                f"Pending: {consumer['pending']} - "
                                f"Idle: {idle_sec:.1f}s")
                            row += 1
                            
                    row += 1  # Space between streams
                    
                # Instructions
                if row < curses.LINES - 2:
                    stdscr.addstr(curses.LINES - 2, 0, "Press 'q' to quit", curses.A_DIM)
                
                stdscr.refresh()
                
                # Check for quit
                key = stdscr.getch()
                if key == ord('q'):
                    self.running = False
                    
                time.sleep(1)  # Update every second
                
            except KeyboardInterrupt:
                self.running = False
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                time.sleep(1)
                
    def run_simple(self):
        """Simple monitoring without curses"""
        logger.info("Starting Stream Monitor (simple mode)...")
        logger.info(f"Connecting to Redis at: {self.redis_url}")
        
        while self.running:
            try:
                print("\n" + "="*80)
                print(f"Waypoint Stream Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*80)
                
                # Find all streams
                stream_pattern = "hub:*:stream:*"
                keys = sorted(self.redis_client.keys(stream_pattern))
                
                if not keys:
                    print("No streams found")
                else:
                    for key in keys:
                        info = self.get_stream_info(key)
                        if not info:
                            continue
                            
                        print(f"\nStream: {key}")
                        print(f"  Length: {info['length']:,}")
                        
                        if info.get('last_entry'):
                            print(f"  Last ID: {info['last_entry']['id']}")
                            
                        for group in info.get('groups', []):
                            print(f"  Group: {group['name']}")
                            print(f"    Pending: {group['pending']}")
                            print(f"    Consumers: {len(group.get('consumers', []))}")
                            
                            for consumer in group.get('consumers', []):
                                idle_sec = consumer['idle'] / 1000
                                print(f"      {consumer['name']}: pending={consumer['pending']}, idle={idle_sec:.1f}s")
                                
                print("\nPress Ctrl+C to quit")
                time.sleep(5)  # Update every 5 seconds in simple mode
                
            except KeyboardInterrupt:
                self.running = False
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                time.sleep(5)
                
    def run(self):
        """Run the monitor"""
        # Test Redis connection
        try:
            self.redis_client.ping()
            logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return
            
        # Try to use curses for better display
        try:
            curses.wrapper(self.monitor_with_curses)
        except:
            # Fall back to simple mode
            logger.info("Falling back to simple monitoring mode")
            self.run_simple()
            
        logger.info("Monitor stopped")
        
if __name__ == "__main__":
    monitor = StreamMonitor()
    monitor.run()