import os
import sys
import csv
import psycopg2
import pandas as pd
import py7zr #для розпакування архівів
import requests #для завантаження архівів з даними про ЗНО/НМТ
import time


#docker-compose build --no-cache && docker-compose up -d --force-recreate
def createDataFrames():
    data21 = pd.read_csv('Odata2021File.csv', sep=";", decimal=",", low_memory=False)
    data19 = pd.read_csv('Odata2019File.csv', sep=";", decimal=",", encoding="Windows-1251", low_memory=False)
    df19 = pd.DataFrame(data19, columns=['OUTID', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME',
                                         'TERNAME', 'REGTYPENAME', 'TerTypeName', 'EONAME', 'EOTYPENAME', 'EORegName',
                                         'UkrTestStatus',
                                         'UkrBall100',
                                         'UkrBall12',
                                         'UkrBall',
                                         'histTestStatus',
                                         'histBall100',
                                         'histBall12',
                                         'histBall',
                                         'mathTestStatus',
                                         'mathBall100',
                                         'mathBall12',
                                         'mathBall',
                                         'physTestStatus',
                                         'physBall100',
                                         'physBall12',
                                         'physBall',
                                         'chemTestStatus',
                                         'chemBall100',
                                         'chemBall12',
                                         'chemBall',
                                         'bioTestStatus',
                                         'bioBall100',
                                         'bioBall12',
                                         'bioBall',
                                         'geoTestStatus',
                                         'geoBall100',
                                         'geoBall12',
                                         'geoBall',
                                         'engTestStatus',
                                         'engBall100',
                                         'engBall12',
                                         'engBall',
                                         'frTestStatus',
                                         'frBall100',
                                         'frBall12',
                                         'frBall',
                                         'deuTestStatus',
                                         'deuBall100',
                                         'deuBall12',
                                         'deuBall',
                                         'spTestStatus',
                                         'spBall100',
                                         'spBall12',
                                         'spBall',
                                         ])
    df21 = pd.DataFrame(data21, columns=['OUTID', 'Birth', 'SexTypeName', 'RegName', 'AREANAME',
                                         'TERNAME', 'RegTypeName', 'TerTypeName', 'EONAME', 'EOTypeName', 'EORegName',
                                         'UMLTestStatus',
                                         'UMLBall100',
                                         'UMLBall12',
                                         'UMLBall',
                                         'UkrTestStatus',
                                         'UkrBall100',
                                         'UkrBall12',
                                         'UkrBall',
                                         'HistTestStatus',
                                         'HistBall100',
                                         'HistBall12',
                                         'HistBall',
                                         'MathTestStatus',
                                         'MathBall100',
                                         'MathBall12',
                                         'MathBall',
                                         'PhysTestStatus',
                                         'PhysBall100',
                                         'PhysBall12',
                                         'PhysBall',
                                         'ChemTestStatus',
                                         'ChemBall100',
                                         'ChemBall12',
                                         'ChemBall',
                                         'BioTestStatus',
                                         'BioBall100',
                                         'BioBall12',
                                         'BioBall',
                                         'GeoTestStatus',
                                         'GeoBall100',
                                         'GeoBall12',
                                         'GeoBall',
                                         'EngTestStatus',
                                         'EngBall100',
                                         'EngBall12',
                                         'EngBall',
                                         'FrTestStatus',
                                         'FrBall100',
                                         'FrBall12',
                                         'FrBall',
                                         'DeuTestStatus',
                                         'DeuBall100',
                                         'DeuBall12',
                                         'DeuBall',
                                         'SpTestStatus',
                                         'SpBall100',
                                         'SpBall12',
                                         'SpBall',
                                         ])

    for col in df19.columns:
        if "Ball100" in col:
            df19[col] = df19[col].apply(pd.to_numeric)

    for col in df19.columns:
        if "Ball100" in col:
            df19[col] = df19[col].apply(pd.to_numeric)

    return df19, df21


def createConnection():
    for attempt in range(10):
        try:
            connection = psycopg2.connect(dbname='Klots_01_BD', user='Bohdan_Klots_01', password='root', host='db')
            print("Connection to database is successful")
            return connection
        except psycopg2.OperationalError:
            print("Connection failed. Restarting in 5 seconds...")
            time.sleep(5)

    print("Failed to connect. Try later :(")
    sys.exit()


def txtStopWatch(start_time):
    with open('stopwatch.txt', 'w', encoding='UTF-32') as f:
        minutes = int((time.time() - start_time)/60)
        seconds = int((time.time() - start_time) - minutes*60)
        f.write("Time of executing: {0}:{1}".format(minutes, seconds))
        f.close()


def download_7z():
    years = ["2019", "2021"]
    url19 = "https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2019.7z"
    url21= "https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2021.7z"
    urls = [url19, url21]

    for num in range(len(urls)):
        req = requests.get(urls[num], stream=True)
        if req.status_code == 200:
            filename = "ZNO" + years[num]
            with open(filename, 'wb') as out:
                out.write(req.content)
            with py7zr.SevenZipFile(filename, 'r') as archive:
                archive.extractall()

            if os.path.isfile("ZNO" + str(years[num])):
                os.remove("ZNO" + str(years[num]))
        else:
            print('Request failed: %d' % req.status_code)


def createTable():
    connection = createConnection()
    with connection:
        cursor = connection.cursor()

        query1 = """
        CREATE TABLE zno_records(
            Year INT,
            OutID VARCHAR(1000) NOT NULL,
            Birth CHAR(4) NOT NULL,
            SexTypeName CHAR(8) NOT NULL,
            Regname VARCHAR(1000) NOT NULL,
            AreaName VARCHAR(1000) NOT NULL,
            TerName VARCHAR(1000) NOT NULL,
            RegTypeName VARCHAR(1000) NOT NULL,
            TerTypeName VARCHAR(1000) NOT NULL,
            EOName VARCHAR(1000),
            EOTypeName VARCHAR(1000),
            EORegName VARCHAR(1000),
            UMLTestStatus VARCHAR(25),
            UMLBall100 DECIMAL,
            UMLBall12 DECIMAL,
            UMLBall DECIMAL,
            UkrTestStatus VARCHAR(25),
            UkrBall100 DECIMAL,
            UkrBall12 DECIMAL,
            UkrBall DECIMAL,
            HistTestStatus VARCHAR(25),
            HistBall100 DECIMAL,
            HistBall12 DECIMAL,
            HistBall DECIMAL,
            MathTestStatus VARCHAR(25),
            MathBall100 DECIMAL,
            MathBall12 DECIMAL,
            MathBall DECIMAL,
            PhysTestStatus VARCHAR(25),
            PhysBall100 DECIMAL,
            PhysBall12 DECIMAL,
            PhysBall DECIMAL,
            ChemTestStatus VARCHAR(25),
            ChemBall100 DECIMAL,
            ChemBall12 DECIMAL,
            ChemBall DECIMAL,
            BioTestStatus VARCHAR(25),
            BioBall100 DECIMAL,
            BioBall12 DECIMAL,
            BioBall DECIMAL,
            GeoTestStatus VARCHAR(25),
            GeoBall100 DECIMAL,
            GeoBall12 DECIMAL,
            GeoBall DECIMAL,
            EngTestStatus VARCHAR(25),
            EngBall100 DECIMAL,
            EngBall12 DECIMAL,
            EngBall DECIMAL,
            FrTestStaTus VARCHAR(25),
            FrBall100 DECIMAL,
            FrBall12 DECIMAL,
            FrBall DECIMAL,
            DeuTestStaTus VARCHAR(25),
            DeuBall100 DECIMAL,
            DeuBall12 DECIMAL,
            DeuBall DECIMAL,
            SpTestStaTus VARCHAR(25),
            SpBall100 DECIMAL,
            SpBall12 DECIMAL,
            SpBall DECIMAL 
        );
        """
        cursor.execute(query1)


def doesTableExist():
    connection = createConnection()
    cursor = connection.cursor()
    query = """SELECT COUNT(table_name) FROM information_schema.tables
            WHERE table_schema LIKE 'public' AND table_type LIKE 'BASE TABLE' AND table_name = 'zno_records'"""
    cursor.execute(query)
    result = cursor.fetchall()[0][0]
    if result == 1:
        return True
    return False


def loadDataIntoDB(df, year, connection):
    columns = [i[0].upper() + i[1:] for i in df.columns]
    values_string = '%s, ' * (len(columns)+1)
    values_string = values_string[:-2]
    columns = "year, " + ', '.join(columns)
    cash = []

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM zno_records WHERE year=" + str(year))
        num_of_records_to_ignore = cursor.fetchall()[0][0]

    except psycopg2.OperationalError:
        connection = createConnection()
        loadDataIntoDB(df, year, connection)

    counter = 0
    for row in df.values:
        row = list(row)
        row.insert(0, year)
        cash.append(row)
        query1 = "INSERT INTO zno_records(" + columns + ") VALUES( " + values_string + ");"
        if counter >= num_of_records_to_ignore:
            try:
                cursor = connection.cursor()
                cursor.execute(query1, row)
                if counter % 1000 == 0:
                    connection.commit()
                    cash = []
                    print("{0} rows inserted, time: {1}".format(counter, time.strftime("%H:%M:%S")))

            except psycopg2.OperationalError:
                print("Restoring connection...")
                connection = createConnection()
                cursor = connection.cursor()
                for el in cash:
                    cursor.execute(query1, el)
                cursor.execute(query1, row)
                if counter % 1000 == 0:
                    connection.commit()
                    cash = []
                    print("{0} rows inserted, time: {1}".format(counter, time.strftime("%H:%M:%S")))

        counter += 1

    connection.commit()
    print("{0} rows inserted, time: {1}".format(counter, time.strftime("%H:%M:%S")))
    print("Data of {0} year is successfully inserted.".format(connection))


def fetchResultsByRegion():
    query = "SELECT year, regname, MIN(PhysBall100) FROM zno_records WHERE physteststatus='Зараховано' GROUP BY regname, year;"
    connection = createConnection()
    cursor = connection.cursor()
    cursor.execute(query)
    records = cursor.fetchall()
    fieldnames = ['Year', 'Region', "PhysBall100"]
    rows = []

    for record in records:
        d = {'Year': record[0],
            'Region': record[1],
            "PhysBall100": record[2]}

        rows.append(d)

    with open('records.csv', 'w', encoding='UTF-32') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        f.close()


def main():
    start_time = time.time()

    # Підключаємось до БД
    connection = createConnection()

    # завантажуємо та розархівовуємо дані про ЗНО з сайту
    download_7z()

    # створюємо датафрейм для зручної праці з даними
    df19, df21 = createDataFrames()

    if doesTableExist():
        print("Table exists")
    else:
        createTable()
        print("Table doesn't exist. Creating table...")

    # Вставляємо дані в таблицю
    connection = createConnection()
    loadDataIntoDB(df19, 2019, connection)
    connection = createConnection()
    loadDataIntoDB(df21, 2021, connection)

    # Виконуємо запит згідно 12-го варіанту
    fetchResultsByRegion()

    #Записуємо час виконання програми в текстовий файл
    txtStopWatch(start_time)
    connection.close()
    print("--- %s seconds ---" % round(time.time() - start_time, 2))


main()