import time
from lib.managers.timingUtils import TimingUtils
from lib.db.connection import getDbConnection


def fail(msg):
    raise RuntimeError(msg)


def check(cond, msg):
    if not cond:
        fail(msg)


def run():
    print('timing_manual start')
    def f():
        x = 0
        for i in range(1000):
            x = x + i
    t1 = TimingUtils.measureFunctionExecution(f, repetitions=3)
    check(t1 >= 0.0, 'measureFunctionExecution nonnegative')
    def qcall():
        with getDbConnection() as (conn, cur):
            cur.execute('SELECT 1')
            cur.fetchone()
    t2 = TimingUtils.measureDbQuery(qcall, repetitions=2)
    check(t2 >= 0.0, 'measureDbQuery nonnegative')
    t3 = TimingUtils.measureSqlExecution('SELECT 1', repetitions=2)
    check(t3 >= 0.0, 'measureSqlExecution nonnegative')
    print('timing_manual ok')

if __name__ == '__main__':
    run()

