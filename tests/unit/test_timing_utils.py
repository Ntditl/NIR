from lib.timingUtils import TimingUtils
from lib.db.connection import getDbConnection
from lib.db.models import getCreateTablesSql, getDropTablesSql


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
        raise RuntimeError("ошибка измерения времени функции")


def testMeasureSqlExecutionSimpleQuery():
    sqlStatement = "SELECT 1"
    averageTime = TimingUtils.measureSqlExecution(sqlStatement, repetitions=3)
    if not isinstance(averageTime, float) or averageTime < 0:
        raise RuntimeError("ошибка измерения времени sql запроса")


def testMeasureDbQueryWithCallable():
    def queryCallable():
        with getDbConnection() as (connection, cursor):
            cursor.execute("INSERT INTO viewer (first_name, last_name, email, phone_number) VALUES ('a', 'b', 'c@d.com', '+0123')")
            cursor.execute("SELECT COUNT(*) FROM viewer;")
            count = cursor.fetchone()[0]
            if count <= 0:
                raise RuntimeError("нет строк в viewer")
    averageTime = TimingUtils.measureDbQuery(queryCallable, repetitions=3)
    if not isinstance(averageTime, float) or averageTime < 0:
        raise RuntimeError("ошибка измерения времени db запроса")


def main():
    ensureSchema()
    testMeasureFunctionExecutionReturnsFloat()
    testMeasureSqlExecutionSimpleQuery()
    testMeasureDbQueryWithCallable()
    print("Проверки timingUtils пройдены")

if __name__ == "__main__":
    main()
