import datetime
import random
import time
import os
import matplotlib.pyplot as plt
from lib.databaseConnection import getDbConnection

def measureGenerationSpeed(
    tablesConfig,
    outputCsvPath,
    outputImagePath=None
):
    tableOrder = ["viewer", "movie", "cinema", "hall", "session", "viewer_profile", "favorite_movies", "movie_review", "ticket"]
    rowCounts = [100, 500, 1000, 5000, 10000]
    allResults = []
    existingIds = {}
    def generatePhone():
        country = random.randint(1, 999)
        number = random.randint(1000000, 9999999)
        countryStr = str(country)
        while len(countryStr) < 3:
            countryStr = "0" + countryStr
        numberStr = str(number)
        while len(numberStr) < 7:
            numberStr = "0" + numberStr
        return "+" + countryStr + numberStr
    def generateRating():
        ratings = ['G', 'PG', 'PG-13', 'R', 'NC-17']
        index = random.randint(0, len(ratings) - 1)
        return ratings[index]
    def generateGenre():
        genres = ['Action', 'Drama', 'Comedy', 'Horror', 'Sci-Fi']
        index = random.randint(0, len(genres) - 1)
        return genres[index]
    indexOrder = 0
    while indexOrder < len(tableOrder):
        table = tableOrder[indexOrder]
        if table in tablesConfig:
            with getDbConnection() as (conn, cur):
                cur.execute("DELETE FROM " + table)
                cur.execute(
                    "SELECT column_name, data_type, character_maximum_length, column_default, is_identity FROM information_schema.columns "
                    "WHERE table_name = %s",
                    (table,)
                )
                columnsInfo = cur.fetchall()
            for count in rowCounts:
                startTime = time.time()
                with getDbConnection() as (conn, cur):
                    if table == "viewer_profile":
                        cur.execute("SELECT viewer_id FROM viewer")
                        tempList = cur.fetchall()
                        viewersList = []
                        idx = 0
                        while idx < len(tempList):
                            viewersList.append(tempList[idx][0])
                            idx = idx + 1
                        existingIds['viewer'] = viewersList
                    if table == "favorite_movies" or table == "movie_review" or table == "ticket" or table == "session":
                        cur.execute("SELECT movie_id FROM movie")
                        tempList = cur.fetchall()
                        moviesList = []
                        idx = 0
                        while idx < len(tempList):
                            moviesList.append(tempList[idx][0])
                            idx = idx + 1
                        existingIds['movie'] = moviesList
                    if table == "favorite_movies" or table == "movie_review" or table == "ticket":
                        cur.execute("SELECT viewer_id FROM viewer")
                        tempList = cur.fetchall()
                        viewersList = []
                        idx = 0
                        while idx < len(tempList):
                            viewersList.append(tempList[idx][0])
                            idx = idx + 1
                        existingIds['viewer'] = viewersList
                    if table == "hall" or table == "session":
                        cur.execute("SELECT hall_id FROM hall")
                        tempList = cur.fetchall()
                        hallsList = []
                        idx = 0
                        while idx < len(tempList):
                            hallsList.append(tempList[idx][0])
                            idx = idx + 1
                        existingIds['hall'] = hallsList
                    if table == "session" or table == "hall":
                        cur.execute("SELECT cinema_id FROM cinema")
                        tempList = cur.fetchall()
                        cinemaList = []
                        idx = 0
                        while idx < len(tempList):
                            cinemaList.append(tempList[idx][0])
                            idx = idx + 1
                        existingIds['cinema'] = cinemaList
                    insertColumns = []
                    insertTypes = []
                    insertMaxLens = []
                    idx = 0
                    while idx < len(columnsInfo):
                        cName = columnsInfo[idx][0]
                        cType = columnsInfo[idx][1]
                        cLen = columnsInfo[idx][2]
                        cDefault = columnsInfo[idx][3]
                        cIdentity = columnsInfo[idx][4]
                        isSerial = False
                        if cDefault is not None:
                            if "nextval" in str(cDefault):
                                isSerial = True
                        if cIdentity is not None:
                            if str(cIdentity).upper() == "YES":
                                isSerial = True
                        if not isSerial:
                            insertColumns.append(cName)
                            insertTypes.append(cType)
                            insertMaxLens.append(cLen)
                        idx = idx + 1
                    i = 0
                    while i < count:
                        values = []
                        j = 0
                        while j < len(insertColumns):
                            name = insertColumns[j]
                            dtype = insertTypes[j]
                            maxLen = insertMaxLens[j]
                            if name == 'viewer_id' and table == 'viewer_profile':
                                if i < len(existingIds.get('viewer', [])):
                                    values.append(existingIds['viewer'][i])
                                else:
                                    j = j + 1
                                    continue
                            elif name == 'viewer_id' and (table == 'favorite_movies' or table == 'movie_review' or table == 'ticket'):
                                if len(existingIds.get('viewer', [])) > 0:
                                    rnd = random.randint(0, len(existingIds['viewer']) - 1)
                                    values.append(existingIds['viewer'][rnd])
                                else:
                                    values.append(None)
                            elif name == 'movie_id' and (table == 'favorite_movies' or table == 'movie_review' or table == 'session'):
                                if len(existingIds.get('movie', [])) > 0:
                                    rnd = random.randint(0, len(existingIds['movie']) - 1)
                                    values.append(existingIds['movie'][rnd])
                                else:
                                    values.append(None)
                            elif name == 'hall_id' and table == 'session':
                                if len(existingIds.get('hall', [])) > 0:
                                    rnd = random.randint(0, len(existingIds['hall']) - 1)
                                    values.append(existingIds['hall'][rnd])
                                else:
                                    values.append(None)
                            elif name == 'cinema_id' and table == 'hall':
                                if len(existingIds.get('cinema', [])) > 0:
                                    rnd = random.randint(0, len(existingIds['cinema']) - 1)
                                    values.append(existingIds['cinema'][rnd])
                                else:
                                    values.append(None)
                            elif name == 'rating':
                                values.append(generateRating())
                            elif name == 'genre':
                                values.append(generateGenre())
                            elif name == 'phone_number':
                                values.append(generatePhone())
                            elif 'char' in dtype or 'text' in dtype:
                                baseValue = 'val_' + table + '_' + str(i)
                                if maxLen is not None:
                                    if len(baseValue) > int(maxLen):
                                        baseValue = baseValue[:int(maxLen)]
                                values.append(baseValue)
                            elif 'int' in dtype:
                                values.append(random.randint(1, 1000))
                            elif 'numeric' in dtype:
                                cents = random.randint(0, 100000)
                                value = cents / 100.0
                                values.append(value)
                            elif dtype == 'date':
                                today = datetime.date.today()
                                values.append(today)
                            elif dtype.startswith('timestamp'):
                                now = datetime.datetime.now(datetime.timezone.utc)
                                values.append(now)
                            elif 'boolean' in dtype:
                                if random.randint(0, 1) == 0:
                                    values.append(False)
                                else:
                                    values.append(True)
                            else:
                                values.append(None)
                            j = j + 1
                        if len(values) == 0:
                            i = i + 1
                            continue
                        placeholders = []
                        k = 0
                        while k < len(insertColumns):
                            placeholders.append('%s')
                            k = k + 1
                        insertSql = "INSERT INTO " + table + " (" + ", ".join(insertColumns) + ") VALUES (" + ", ".join(placeholders) + ")"
                        cur.execute(insertSql, values)
                        i = i + 1
                elapsed = time.time() - startTime
                allResults.append((table, count, elapsed))
        indexOrder = indexOrder + 1
    if not os.path.isdir(os.path.dirname(outputCsvPath)) and os.path.dirname(outputCsvPath) != "":
        os.makedirs(os.path.dirname(outputCsvPath), exist_ok=True)
    with open(outputCsvPath, 'w', newline='', encoding='utf-8') as csvf:
        csvf.write('table,rows,time_seconds\n')
        idx = 0
        while idx < len(allResults):
            t = allResults[idx][0]
            r = allResults[idx][1]
            tm = allResults[idx][2]
            csvf.write(f"{t},{r},{tm:.6f}\n")
            idx = idx + 1
    if outputImagePath:
        outDir = os.path.dirname(outputImagePath)
        if outDir != "" and not os.path.isdir(outDir):
            os.makedirs(outDir, exist_ok=True)
        plt.figure(figsize=(10, 6))
        index = 0
        plottedTables = {}
        while index < len(allResults):
            table = allResults[index][0]
            if table in tablesConfig and table not in plottedTables:
                xs = []
                ys = []
                j = 0
                while j < len(allResults):
                    if allResults[j][0] == table:
                        xs.append(allResults[j][1])
                        ys.append(allResults[j][2])
                    j = j + 1
                plt.plot(xs, ys, marker='o', label=table)
                plottedTables[table] = True
            index = index + 1
        plt.title('Generation Speed')
        plt.xlabel('Rows')
        plt.ylabel('Time (s)')
        plt.grid(True)
        plt.legend()
        plt.savefig(outputImagePath)
        plt.close()
