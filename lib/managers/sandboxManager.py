from lib.db.connection import getDbConnection

class SandboxManager:
    def __init__(self, sandboxSchemaName: str = 'sandbox'):
        self.sandboxSchemaName = sandboxSchemaName

    def createSandboxSchema(self):
        with getDbConnection() as (conn, cur):
            print(f"Удаление старой схемы {self.sandboxSchemaName}...", flush=True)

            print("  Завершение активных подключений к схеме...", flush=True)
            cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database()
                AND pid <> pg_backend_pid()
                AND query ILIKE %s
            """, (f'%{self.sandboxSchemaName}%',))

            print("  Принудительное удаление схемы...", flush=True)
            cur.execute("DROP SCHEMA IF EXISTS " + self.sandboxSchemaName + " CASCADE;")
            print(f"  Схема {self.sandboxSchemaName} удалена", flush=True)

            print(f"Создание новой схемы {self.sandboxSchemaName}...", flush=True)
            cur.execute("CREATE SCHEMA " + self.sandboxSchemaName + ";")

            print("Получение списка таблиц из public схемы...", flush=True)
            tableNames = self._getPublicTables(cur)
            print(f"Найдено таблиц: {len(tableNames)}", flush=True)

            seqPlans = {}
            createPlans = {}

            print("Анализ структуры таблиц...", flush=True)
            for i in range(len(tableNames)):
                tableName = tableNames[i]
                colsInfo = self._getColumns(cur, tableName)
                pkDefs = self._getConstraintDefs(cur, tableName, 'p')
                uqDefs = self._getConstraintDefs(cur, tableName, 'u')
                ckDefs = self._getConstraintDefs(cur, tableName, 'c')
                colLines = []
                serialCols = []
                for j in range(len(colsInfo)):
                    colName, dataType, isNullable, colDefault, charLen, numPrec, numScale = colsInfo[j]
                    line = colName + " " + self._mapType(dataType, charLen, numPrec, numScale)
                    isSerial = False
                    if colDefault is not None:
                        if "nextval(" in colDefault:
                            isSerial = True
                        else:
                            line = line + " DEFAULT " + colDefault
                    if isNullable == 'NO':
                        line = line + " NOT NULL"
                    colLines.append(line)
                    if isSerial:
                        serialCols.append(colName)
                for j in range(len(pkDefs)):
                    colLines.append(pkDefs[j])
                for j in range(len(uqDefs)):
                    colLines.append(uqDefs[j])
                for j in range(len(ckDefs)):
                    colLines.append(ckDefs[j])
                createSql = "CREATE TABLE " + self.sandboxSchemaName + "." + tableName + " (" + ",".join(colLines) + ");"
                createPlans[tableName] = createSql
                seqPlans[tableName] = serialCols

            print("Создание таблиц в песочнице...", flush=True)
            for i in range(len(tableNames)):
                tbl = tableNames[i]
                print(f"  Создание таблицы {tbl}...", flush=True)
                cur.execute(createPlans[tbl])
                serialCols = seqPlans[tbl]
                for k in range(len(serialCols)):
                    col = serialCols[k]
                    seqName = self.sandboxSchemaName + "." + tbl + "_" + col + "_seq"
                    cur.execute("CREATE SEQUENCE " + seqName + ";")
                    cur.execute("ALTER TABLE " + self.sandboxSchemaName + "." + tbl + " ALTER COLUMN " + col + " SET DEFAULT nextval('" + seqName + "');")

            print("Копирование данных из public схемы...", flush=True)
            for i in range(len(tableNames)):
                tbl = tableNames[i]
                print(f"  Копирование данных таблицы {tbl}...", flush=True)
                cur.execute("INSERT INTO " + self.sandboxSchemaName + "." + tbl + " SELECT * FROM public." + tbl + ";")

            print("Синхронизация последовательностей...", flush=True)
            for i in range(len(tableNames)):
                tbl = tableNames[i]
                serialCols = seqPlans[tbl]
                for k in range(len(serialCols)):
                    col = serialCols[k]
                    seqName = self.sandboxSchemaName + "." + tbl + "_" + col + "_seq"
                    cur.execute("SELECT COALESCE(MAX(" + col + "),0) FROM " + self.sandboxSchemaName + "." + tbl)
                    maxVal = cur.fetchone()[0]
                    if maxVal is None or maxVal < 1:
                        cur.execute("SELECT setval('" + seqName + "', 1, false)")
                    else:
                        cur.execute("SELECT setval('" + seqName + "', " + str(maxVal) + ", true)")

            print("Создание внешних ключей...", flush=True)
            fkData = self._getForeignKeys(cur)
            for i in range(len(fkData)):
                childTable, conName, conDef = fkData[i]
                defText = conDef.replace('REFERENCES public.', 'REFERENCES ' + self.sandboxSchemaName + '.')

                refPos = defText.find('REFERENCES ')
                while refPos != -1:
                    startPos = refPos + 11
                    endPos = startPos
                    while endPos < len(defText) and (defText[endPos].isalnum() or defText[endPos] == '_'):
                        endPos += 1

                    if endPos < len(defText) and defText[endPos] == '(':
                        tableName = defText[startPos:endPos]
                        if '.' not in tableName:
                            defText = defText[:startPos] + self.sandboxSchemaName + '.' + defText[startPos:]
                            endPos += len(self.sandboxSchemaName) + 1

                    refPos = defText.find('REFERENCES ', endPos)

                cur.execute("ALTER TABLE " + self.sandboxSchemaName + "." + childTable + " ADD CONSTRAINT " + conName + " " + defText)

            print(f"Схема {self.sandboxSchemaName} успешно создана!", flush=True)

    def dropSandboxSchema(self):
        with getDbConnection() as (conn, cur):
            cur.execute("DROP SCHEMA IF EXISTS " + self.sandboxSchemaName + " CASCADE;")

    def resetSandbox(self):
        self.createSandboxSchema()

    def ensureMinimalData(self):
        from lib.data.generators import RandomDataGenerator
        with getDbConnection() as (conn, cur):
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = 'viewer'
                )
            """, (self.sandboxSchemaName,))
            tableExists = cur.fetchone()[0]

            if not tableExists:
                print(f"Таблица {self.sandboxSchemaName}.viewer не существует, пропускаем ensureMinimalData", flush=True)
                return

            cur.execute("SELECT COUNT(*) FROM " + self.sandboxSchemaName + ".viewer")
            viewerCount = cur.fetchone()[0]

        if viewerCount < 10:
            print(f"В таблице viewer только {viewerCount} записей, генерируем минимальный набор данных...", flush=True)
            dataGenerator = RandomDataGenerator()
            dataGenerator.setSchemaPrefix(self.sandboxSchemaName + ".")
            dataGenerator.generateMinimalDataset(10, 10, 10, 15)
        else:
            print(f"В таблице viewer уже {viewerCount} записей, генерация не требуется", flush=True)

    def _getPublicTables(self, cur):
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
        rows = cur.fetchall()
        tables = []
        for i in range(len(rows)):
            name = rows[i][0]
            tables.append(name)
        return tables

    def _getColumns(self, cur, tableName):
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default, character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
        """, (tableName,))
        return cur.fetchall()

    def _mapType(self, dataType, charLen, numPrec, numScale):
        t = dataType.lower()
        if t == 'character varying' and charLen is not None:
            return 'VARCHAR(' + str(charLen) + ')'
        if t == 'character' and charLen is not None:
            return 'CHAR(' + str(charLen) + ')'
        if t == 'timestamp with time zone':
            return 'TIMESTAMPTZ'
        if t == 'timestamp without time zone':
            return 'TIMESTAMP'
        if t == 'numeric':
            if numPrec is not None and numScale is not None:
                return 'NUMERIC(' + str(numPrec) + ',' + str(numScale) + ')'
            return 'NUMERIC'
        if t == 'integer':
            return 'INTEGER'
        if t == 'bigint':
            return 'BIGINT'
        if t == 'boolean':
            return 'BOOLEAN'
        if t == 'text':
            return 'TEXT'
        if t == 'date':
            return 'DATE'
        return dataType

    def _getConstraintDefs(self, cur, tableName, conType):
        cur.execute("""
            SELECT conname, pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            WHERE n.nspname='public' AND r.relname=%s AND c.contype=%s
        """, (tableName, conType))
        rows = cur.fetchall()
        defs = []
        for i in range(len(rows)):
            conName = rows[i][0]
            defBody = rows[i][1]
            if conType == 'p':
                defs.append('PRIMARY KEY ' + defBody[defBody.find('('):])
            elif conType == 'u':
                defs.append('UNIQUE ' + defBody[defBody.find('('):])
            elif conType == 'c':
                defs.append(defBody)
        return defs

    def _getForeignKeys(self, cur):
        cur.execute("""
            SELECT r.relname AS child, c.conname, pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class r ON r.oid = c.conrelid
            JOIN pg_namespace nr ON nr.oid = r.relnamespace
            WHERE c.contype='f' AND nr.nspname='public'
        """)
        rows = cur.fetchall()
        data = []
        for i in range(len(rows)):
            data.append((rows[i][0], rows[i][1], rows[i][2]))
        return data
