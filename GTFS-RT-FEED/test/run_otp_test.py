#!/usr/bin/env python3
"""
Simple test runner for OTP cancelled trips test using the exact parameters from the curl request.
"""

import subprocess
import sys
import os

def main():
    """Run the OTP test with the exact parameters from the curl request"""
    
    # Parameters from the curl request
    from_place = "NANDANAM METRO (bfcf9b7b9d25bf3659a12a14cc9e9362)::13.0309,80.23999"
    to_place = "Alandur Metro (SAL|0231)::13.003722,80.2015436"
    time_str = "9:03pm"
    date_str = "09-17-2025"
    mode = "BUS,WALK"
    num_itineraries = 100
    
    # Default URL (can be overridden with environment variables)
    otp_url = os.getenv('OTP_URL', 'http://0.0.0.0:8080')
    
    print("=" * 80)
    print("OTP CANCELLED TRIPS TEST RUNNER")
    print("=" * 80)
    print(f"OTP URL: {otp_url}")
    print(f"From: {from_place}")
    print(f"To: {to_place}")
    print(f"Time: {time_str}, Date: {date_str}")
    print(f"Mode: {mode}, Itineraries: {num_itineraries}")
    print("=" * 80)
    print()
    
    # Build command - use absolute path to the test script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_script = os.path.join(script_dir, 'test_otp_cancelled_trips.py')
    
    cmd = [
        sys.executable, test_script,
        '--otp-url', otp_url,
        '--from-place', from_place,
        '--to-place', to_place,
        '--time', time_str,
        '--date', date_str,
        '--mode', mode,
        '--num-itineraries', str(num_itineraries)
    ]
    
    try:
        # Run the test
        result = subprocess.run(cmd, check=True, capture_output=False)
        print("\n" + "=" * 80)
        print("Test completed successfully!")
        print("=" * 80)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\nTest failed with exit code: {e.returncode}")
        return False
    except FileNotFoundError:
        print("Error: test_otp_cancelled_trips.py not found. Make sure it's in the same directory.")
        return False
    except Exception as e:
        print(f"Error running test: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)