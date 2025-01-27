# run_tests.py
import luigi
import os
from test_preprocessing import TestPreprocessingGateway

def test_scenario(fail_count, expected_success):
    """
    Run a test scenario with specified number of failures
    """
    print(f"\nTesting scenario with {fail_count} failures...")

    # Remove old output file if it exists
    output_file = f'test_output_{fail_count}.txt'
    if os.path.exists(output_file):
        os.remove(output_file)

    try:
        result = luigi.build([
            TestPreprocessingGateway(fail_count=fail_count)
        ], local_scheduler=True)
        assert result == expected_success, f"Expected success={expected_success}, got {result}"
        print(f"Test passed! Task {'succeeded' if result else 'failed'} as expected.")
    except Exception as e:
        print(f"Test failed with error: {str(e)}")

def run_all_tests():
    # Test scenarios
    scenarios = [
        (0, True),   # Should succeed immediately
        (1, True),   # Should succeed after one retry
        (2, True),   # Should succeed after two retries
        (3, False),  # Should fail after all retries
    ]

    for fail_count, expected_success in scenarios:
        test_scenario(fail_count, expected_success)

if __name__ == "__main__":
    run_all_tests()