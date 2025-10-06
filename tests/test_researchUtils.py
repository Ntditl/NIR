import pytest
import timeit
from investigations.researchUtils import measureAverageTime, REPEATS_DEFAULT

def test_measure_average_time_uses_timeit_repeat():
    call_count = 0

    def test_function():
        nonlocal call_count
        call_count += 1
        return "result"

    avgTime, result = measureAverageTime(test_function, repeats=3)

    assert avgTime > 0
    assert result == "result"
    assert call_count >= 3

def test_measure_average_time_default_repeats():
    def test_function():
        return 42

    avgTime, result = measureAverageTime(test_function)

    assert avgTime >= 0
    assert result == 42

