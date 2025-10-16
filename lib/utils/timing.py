import timeit


def measureAverageTime(func, repeats=3):
    times = timeit.repeat(func, repeat=repeats, number=1)
    avgTime = sum(times) / len(times)
    return avgTime


def measureExecutionTime(func):
    executionTime = timeit.timeit(func, number=1)
    return executionTime

