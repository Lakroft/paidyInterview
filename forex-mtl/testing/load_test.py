#!/usr/bin/env python3
"""
Load Test Script for Forex-MTL

Runs load test against forex-mtl service with proxy monitoring.
Generates random currency pair requests and provides detailed statistics.
"""

import argparse
import asyncio
import aiohttp
import json
import random
import time
import subprocess
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# Supported currencies (same as in Scala code)
CURRENCIES = ['AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'NZD', 'SGD', 'USD']

class LoadTester:
    def __init__(self, rps: int, duration_minutes: int):
        self.rps = rps
        self.duration_seconds = duration_minutes * 60
        self.forex_url = "http://localhost:8087"
        self.proxy_url = "http://localhost:8088"
        
        # Statistics
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.start_time = None
        
    async def start_containers(self) -> bool:
        """Start test containers and wait for readiness."""
        print("Starting test containers...")
        
        # Start containers (from parent directory)
        process = subprocess.run([
            'docker-compose', '--profile', 'test', 'up', '-d'
        ], capture_output=True, text=True, cwd='..')
        
        if process.returncode != 0:
            print(f"Failed to start containers: {process.stderr}")
            return False
            
        print("Waiting for services to become ready...")
        
        # Wait for services to be ready
        max_wait = 60  # seconds
        start_wait = time.time()
        
        while time.time() - start_wait < max_wait:
            try:
                # Check forex-mtl health
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{self.forex_url}/rates?from=USD&to=EUR", 
                                         timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status in [200, 400]:  # 400 is also ok (service responding)
                            print("✓ forex-mtl-test ready")
                            break
            except:
                await asyncio.sleep(2)
        else:
            print("✗ Timeout waiting for forex-mtl-test")
            return False
            
        # Check proxy health
        start_wait = time.time()
        while time.time() - start_wait < 30:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{self.proxy_url}/get_logs",
                                         timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            print("✓ proxy ready")
                            return True
            except:
                await asyncio.sleep(1)
        
        print("✗ Timeout waiting for proxy")
        return False
    
    def generate_random_pair(self) -> Tuple[str, str]:
        """Generate random currency pair (from != to)."""
        from_currency = random.choice(CURRENCIES)
        to_currency = random.choice([c for c in CURRENCIES if c != from_currency])
        return from_currency, to_currency
    
    async def make_request(self, session: aiohttp.ClientSession) -> None:
        """Make single request to forex service."""
        from_cur, to_cur = self.generate_random_pair()
        url = f"{self.forex_url}/rates?from={from_cur}&to={to_cur}"
        
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                self.total_requests += 1
                if response.status == 200:
                    self.successful_requests += 1
                else:
                    self.failed_requests += 1
        except Exception as e:
            self.total_requests += 1
            self.failed_requests += 1
            
    async def run_load_test(self) -> None:
        """Run the load test with specified RPS and duration."""
        print(f"Starting load test: {self.rps} RPS for {self.duration_seconds/60:.1f} minutes")
        
        # Calculate and show expected end time
        start_datetime = datetime.now()
        end_datetime = start_datetime + timedelta(seconds=self.duration_seconds)
        print(f"Start time: {start_datetime.strftime('%H:%M:%S')}")
        print(f"Expected end time: {end_datetime.strftime('%H:%M:%S')}")
        
        
        self.start_time = time.time()
        interval = 1.0 / self.rps  # seconds between requests
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            next_request_time = time.time()
            last_progress_time = 0
            
            while time.time() - self.start_time < self.duration_seconds:
                current_time = time.time()
                elapsed = current_time - self.start_time
                
                # Schedule next request
                if current_time >= next_request_time:
                    task = asyncio.create_task(self.make_request(session))
                    tasks.append(task)
                    next_request_time += interval
                    
                # Show progress every 30 seconds
                if elapsed - last_progress_time >= 30:
                    await self.show_progress(elapsed)
                    last_progress_time = elapsed
                
                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.01)
            
            # Wait for remaining requests to complete
            print("Waiting for remaining requests to complete...")
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def show_progress(self, elapsed: float) -> None:
        """Show current progress with proxy stats."""
        remaining = self.duration_seconds - elapsed
        progress = (elapsed / self.duration_seconds) * 100
        
        # Get current proxy stats
        proxy_stats = await self.get_proxy_stats()
        oneframe_requests = proxy_stats.get('total_requests', 0)
        daily_percentage = (oneframe_requests / 1000.0) * 100
        
        current_time = datetime.now().strftime('%H:%M:%S')
        remaining_time = datetime.now() + timedelta(seconds=remaining)
        
        print(f"\n[{current_time}] Progress: {progress:.1f}%")
        print(f"  Forex requests: {self.total_requests} (success: {self.successful_requests}, errors: {self.failed_requests})")
        print(f"  One-Frame calls: {oneframe_requests} ({daily_percentage:.2f}% of daily limit)")
        print(f"  Time remaining: {remaining/60:.1f}min (end: {remaining_time.strftime('%H:%M:%S')})")
        
        if self.total_requests > 0:
            actual_rps = self.total_requests / elapsed
            print(f"  Actual RPS: {actual_rps:.1f}")
    
    async def get_proxy_stats(self) -> Dict:
        """Get statistics from proxy server."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.proxy_url}/get_logs") as response:
                    if response.status == 200:
                        return await response.json()
        except Exception as e:
            print(f"Error getting proxy stats: {e}")
        return {'total_requests': 0, 'logs': []}
    
    def stop_containers(self) -> None:
        """Stop test containers."""
        print("Stopping test containers...")
        subprocess.run(['docker-compose', '--profile', 'test', 'down'], 
                      capture_output=True, cwd='..')
        print("✓ Containers stopped")
    
    async def run_full_test(self) -> None:
        """Run complete load test cycle."""
        try:
            # Start containers
            if not await self.start_containers():
                print("Failed to start containers. Exiting.")
                return
            
            
            # Run load test
            await self.run_load_test()
            
            # Get results
            print("\nGetting results...")
            proxy_stats = await self.get_proxy_stats()
            
            # Calculate summary
            total_duration = time.time() - self.start_time
            oneframe_requests = proxy_stats.get('total_requests', 0)
            daily_limit_percentage = (oneframe_requests / 1000.0) * 100
            
            # Print summary
            print("\n" + "="*60)
            print("LOAD TEST SUMMARY")
            print("="*60)
            print(f"Test duration: {total_duration/60:.1f} minutes")
            print(f"Target RPS: {self.rps}")
            print(f"Отправлено запросов к forex: {self.total_requests}")
            print(f"Успешных (код 200): {self.successful_requests}")
            print(f"Ошибок: {self.failed_requests}")
            print(f"Запросов к one-frame: {oneframe_requests}")
            print(f"% от дневного лимита: {daily_limit_percentage:.2f}%")
            
            if self.total_requests > 0:
                success_rate = (self.successful_requests / self.total_requests) * 100
                print(f"Success rate: {success_rate:.1f}%")
                print(f"Actual RPS: {self.total_requests / total_duration:.1f}")
            
            # Show proxy details if available
            if proxy_stats.get('logs'):
                print(f"\nProxy log sample (last 3 requests):")
                for log in proxy_stats['logs'][-3:]:
                    print(f"  {log['timestamp']} {log['method']} {log['path']} -> "
                          f"{log['status_code']} ({log['latency_ms']}ms)")
            
        finally:
            self.stop_containers()

def main():
    parser = argparse.ArgumentParser(description='Load test forex-mtl service')
    parser.add_argument('--rps', type=int, default=10, 
                       help='Requests per second (default: 10)')
    parser.add_argument('--duration', type=int, default=10,
                       help='Test duration in minutes (default: 10)')
    
    args = parser.parse_args()
    
    print(f"Forex-MTL Load Test")
    print(f"RPS: {args.rps}")
    print(f"Duration: {args.duration} minutes")
    print(f"Total requests: ~{args.rps * args.duration * 60}")
    
    tester = LoadTester(args.rps, args.duration)
    
    try:
        asyncio.run(tester.run_full_test())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        tester.stop_containers()
    except Exception as e:
        print(f"\nTest failed: {e}")
        tester.stop_containers()
        sys.exit(1)

if __name__ == '__main__':
    main()