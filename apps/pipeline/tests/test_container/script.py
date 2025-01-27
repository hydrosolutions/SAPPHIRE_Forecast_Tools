# test_container/script.py
import os
import sys
import time

# Get the initial fail count
initial_fail_count = int(os.getenv('FAIL_COUNT', '0'))
attempt_number = os.getenv('ATTEMPT_NUMBER', '1')

print(f"Container starting. Initial fail count: {initial_fail_count}")
print(f"This is attempt number: {attempt_number}")

# Calculate remaining failures based on attempt number
remaining_failures = max(0, initial_fail_count - (int(float(attempt_number)) - 1))
print(f"Remaining failures: {remaining_failures}")

if remaining_failures > 0:
    print("Simulating failure...")
    sys.exit(1)
else:
    print("Simulating success...")
    time.sleep(2)  # Simulate some work
    print("Work completed successfully")
    sys.exit(0)