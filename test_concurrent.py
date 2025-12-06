"""
Concurrent Execution Test
Simulates 3 clients running simultaneously
"""

import subprocess
import time
from datetime import datetime

def test_concurrent_execution():
    """
    Test 3 concurrent PDF generations
    """
    
    clients = [
        ('Mayfair', 'London', 'alice@test.com', 'Alice Johnson'),
        ('Chelsea', 'London', 'bob@test.com', 'Bob Smith'),
        ('Belgravia', 'London', 'carol@test.com', 'Carol Williams')
    ]
    
    print("="*70)
    print("CONCURRENT EXECUTION TEST")
    print("="*70)
    print(f"Testing {len(clients)} simultaneous executions...")
    print(f"Start time: {datetime.now()}")
    print("="*70)
    
    processes = []
    
    # Launch all 3 simultaneously
    for area, city, email, name in clients:
        print(f"🚀 Launching: {area} -> {email}")
        
        p = subprocess.Popen([
            'python',
            'voxmill_master.py',
            '--area', area,
            '--city', city,
            '--email', email,
            '--name', name
        ])
        
        processes.append({
            'process': p,
            'area': area,
            'email': email
        })
        
        time.sleep(1)  # Stagger by 1 second
    
    # Wait for all to complete
    print(f"\n⏳ Waiting for all executions to complete...")
    
    for item in processes:
        item['process'].wait()
        print(f"   ✅ {item['area']} finished (exit code: {item['process'].returncode})")
    
    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)
    print("\nNEXT STEPS:")
    print("1. Check MongoDB fs.files collection - should have 3 PDFs")
    print("2. Check each PDF has correct area in metadata")
    print("3. Check each client received correct email")
    print("4. Verify NO file collision errors in logs")
    
if __name__ == '__main__':
    test_concurrent_execution()
