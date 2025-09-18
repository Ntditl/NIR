from lib.timingUtils import TimingUtils
from lib.databaseConnection import getDbConnection
from lib.tableModels import getCreateTablesSql, getDropTablesSql


def ensureSchema():
    with getDbConnection() as (conn, cur):
        cmds = getDropTablesSql()
        i = 0
        while i < len(cmds):
            cur.execute(cmds[i])
            i = i + 1
        cmds = getCreateTablesSql()
        i = 0
        while i < len(cmds):
            cur.execute(cmds[i])
            i = i + 1


def testMeasureFunctionExecutionReturnsFloat():
    def sampleFunction(x):
        return x * x
    averageTime = TimingUtils.measureFunctionExecution(sampleFunction, 5, repetitions=3)
    if not isinstance(averageTime, float) or averageTime < 0:
        raise RuntimeError("measureFunctionExecution failed")


def testMeasureSqlExecutionSimpleQuery():
    sqlStatement = "SELECT 1"
    averageTime = TimingUtils.measureSqlExecution(sqlStatement, repetitions=3)
    if not isinstance(averageTime, float) or averageTime < 0:
        raise RuntimeError("measureSqlExecution failed")


def testMeasureDbQueryWithCallable():
    def queryCallable():
        with getDbConnection() as (connection, cursor):
            cursor.execute("INSERT INTO viewer (first_name, last_name, email, phone_number) VALUES ('a', 'b', 'c@d.com', '+0123')")
            cursor.execute("SELECT COUNT(*) FROM viewer;")
            count = cursor.fetchone()[0]
            if count <= 0:
                raise RuntimeError("no rows in viewer")
    averageTime = TimingUtils.measureDbQuery(queryCallable, repetitions=3)
    if not isinstance(averageTime, float) or averageTime < 0:
        raise RuntimeError("measureDbQuery failed")


def main():
    ensureSchema()
    testMeasureFunctionExecutionReturnsFloat()
    testMeasureSqlExecutionSimpleQuery()
    testMeasureDbQueryWithCallable()
    print("timingUtils checks passed")

if __name__ == "__main__":
    main()
