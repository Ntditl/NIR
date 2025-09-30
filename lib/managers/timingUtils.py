import timeit
from lib.db.connection import getDbConnection

class TimingUtils:
    @staticmethod
    def measureFunctionExecution(func, *args, repetitions: int = 5, **kwargs) -> float:
        def callWrapper():
            func(*args, **kwargs)
        timer = timeit.Timer(callWrapper)
        runs = timer.repeat(repeat=repetitions, number=1)
        total = 0.0
        i = 0
        while i < len(runs):
            total = total + runs[i]
            i = i + 1
        return total / len(runs)

    @staticmethod
    def measureSqlExecution(sqlStatement: str, repetitions: int = 5) -> float:
        def executionWrapper():
            with getDbConnection() as (connection, cursor):
                cursor.execute(sqlStatement)
        timer = timeit.Timer(executionWrapper)
        runs = timer.repeat(repeat=repetitions, number=1)
        total = 0.0
        i = 0
        while i < len(runs):
            total = total + runs[i]
            i = i + 1
        return total / len(runs)

    @staticmethod
    def measureDbQuery(queryCallable, repetitions: int = 5) -> float:
        def callWrapper():
            queryCallable()
        timer = timeit.Timer(callWrapper)
        runs = timer.repeat(repeat=repetitions, number=1)
        total = 0.0
        i = 0
        while i < len(runs):
            total = total + runs[i]
            i = i + 1
        return total / len(runs)
