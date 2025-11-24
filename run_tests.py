#!/usr/bin/env python
"""
Simple test runner script for Socket Hub application.
Runs all tests and provides a summary.
"""

import os
import subprocess  # nosec: B404
import sys


def run_tests():
    """Run all tests and display results"""
    print("=" * 80)
    print("SOCKET HUB TEST SUITE")
    print("=" * 80)
    print()

    # Set UTF-8 encoding
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    # Run pytest with comprehensive output
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/",
        "-v",
        "--tb=short",
    ]

    result = subprocess.run(cmd, cwd=".", env=env, capture_output=False)  # nosec: B603

    print()
    print("=" * 80)
    if result.returncode == 0:
        print("[PASS] ALL TESTS PASSED")
    else:
        print("[FAIL] SOME TESTS FAILED")
    print("=" * 80)

    return result.returncode


def run_specific_test_file(test_file):
    """Run a specific test file"""
    print(f"Running tests from: {test_file}")
    print("=" * 80)
    print()

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        test_file,
        "-v",
        "--tb=short",
    ]

    result = subprocess.run(cmd, cwd=".", capture_output=False)  # nosec: B603

    return result.returncode


if __name__ == "__main__":
    exit_code = (
        run_specific_test_file(sys.argv[1]) if len(sys.argv) > 1 else run_tests()
    )

    sys.exit(exit_code)
