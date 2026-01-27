"""
Profiling utilities for preprocessing_runoff module.

This module provides timing instrumentation to identify performance bottlenecks
in iEasyHydro HF SDK data retrieval.

Usage:
    from .profiling import ProfileTimer, profiling_enabled, get_profiling_report

    # Enable profiling via environment variable
    # PREPROCESSING_PROFILING=true uv run preprocessing_runoff.py

    # Or programmatically
    with ProfileTimer("fetch_daily_average"):
        data = fetch_data()

    # Get report at end
    print(get_profiling_report())
"""

import os
import time
import logging
from contextlib import contextmanager
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)

# Global storage for timing data
_timings = defaultdict(list)
_call_counts = defaultdict(int)


def profiling_enabled() -> bool:
    """Check if profiling is enabled via environment variable."""
    return os.getenv('PREPROCESSING_PROFILING', '').lower() in ('true', '1', 'yes')


def reset_profiling():
    """Reset all profiling data."""
    global _timings, _call_counts
    _timings = defaultdict(list)
    _call_counts = defaultdict(int)


class ProfileTimer:
    """
    Context manager for timing code blocks.

    Usage:
        with ProfileTimer("api_call"):
            result = make_api_call()
    """

    def __init__(self, name: str, log_immediately: bool = False):
        """
        Initialize timer.

        Args:
            name: Name for this timing measurement
            log_immediately: If True, log timing when context exits
        """
        self.name = name
        self.log_immediately = log_immediately
        self.start_time: Optional[float] = None
        self.elapsed: Optional[float] = None

    def __enter__(self):
        if profiling_enabled():
            self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if profiling_enabled() and self.start_time is not None:
            self.elapsed = time.perf_counter() - self.start_time
            _timings[self.name].append(self.elapsed)
            _call_counts[self.name] += 1

            if self.log_immediately:
                print(f"[PROFILE] {self.name}: {self.elapsed:.3f}s")

        return False  # Don't suppress exceptions


@contextmanager
def profile_section(name: str, log_immediately: bool = True):
    """
    Context manager for profiling a section of code.

    Args:
        name: Name for this section
        log_immediately: If True, log timing when section completes
    """
    timer = ProfileTimer(name, log_immediately=log_immediately)
    with timer:
        yield timer


def get_profiling_summary() -> dict:
    """
    Get summary statistics for all profiled sections.

    Returns:
        Dictionary with timing statistics per section
    """
    summary = {}
    for name, times in _timings.items():
        if times:
            summary[name] = {
                'count': len(times),
                'total': sum(times),
                'mean': sum(times) / len(times),
                'min': min(times),
                'max': max(times),
            }
    return summary


def get_profiling_report() -> str:
    """
    Generate a human-readable profiling report.

    Returns:
        Formatted string with timing breakdown
    """
    if not profiling_enabled():
        return "Profiling disabled. Set PREPROCESSING_PROFILING=true to enable."

    summary = get_profiling_summary()

    if not summary:
        return "No profiling data collected."

    lines = [
        "",
        "=" * 70,
        "PROFILING REPORT",
        "=" * 70,
        "",
        f"{'Section':<40} {'Count':>6} {'Total':>10} {'Mean':>10} {'Min':>10} {'Max':>10}",
        "-" * 70,
    ]

    # Sort by total time descending
    sorted_items = sorted(summary.items(), key=lambda x: x[1]['total'], reverse=True)

    total_time = 0
    for name, stats in sorted_items:
        lines.append(
            f"{name:<40} {stats['count']:>6} {stats['total']:>9.2f}s {stats['mean']:>9.2f}s {stats['min']:>9.2f}s {stats['max']:>9.2f}s"
        )
        total_time += stats['total']

    lines.extend([
        "-" * 70,
        f"{'TOTAL':<40} {'':<6} {total_time:>9.2f}s",
        "=" * 70,
        "",
    ])

    return "\n".join(lines)


def log_profiling_report():
    """Log the profiling report if profiling is enabled."""
    if profiling_enabled():
        report = get_profiling_report()
        # Print directly to ensure output is visible
        # (logger may not have handlers configured in all contexts)
        print(report)
