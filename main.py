import uuid
import zipfile
import os
import psycopg2
import pybase64
from psycopg2 import Error
import openpyxl
import logging
cycle
file_source = 'zipf/'
file_destination = 'Zipf2/'
get_files = os.listdir(file_source)

#this script infinity
#cycle, read zip files, search pdf files in zip and save in directories, search excel file in zip and save in directories
#you mission only create 4 directories and create database on postgres
def get_zip():
    while True:
        for g in os.listdir(file_source):
            os.replace(file_source + g, file_destination + g)
            archive = file_destination + g
            with zipfile.ZipFile(archive, 'r') as zip_files:
                for file in zip_files.namelist():
                    if file.endswith('pdf'):
                        uid = str(uuid.uuid4())
                        zip_files.extract(file, 'pdffiles')
                        os.renames('pdffiles/' + file, 'pdffiles/' + uid + ".pdf" )
                        print('файл= ' + uid, "обработан")
                        for excel in zip_files.namelist():
                            if excel.endswith('xlsx'):
                                xlfile = zip_files.open(excel)
                                book = openpyxl.open(xlfile, read_only=True)
                                sheet = book.active
                                for row in range(2, 3):
                                    kodterr = sheet[row][1].value
                                    snilspravo = sheet[row][2].value
                                    snilsymer = sheet[row][3].value
                                    namefiles = sheet[row][4].value
                                    idizve = sheet[row][5].value
                                    dataotp = sheet[row][6].value
                                    cmevid = sheet[row][7].value
                                    statuscmev = sheet[row][8].value
                                    statusdosva = sheet[row][9].value
                                    ecxel = kodterr, snilspravo, snilsymer, namefiles, idizve, dataotp, cmevid, statuscmev, statusdosva
                                    try:
                                        #connect ty database and insert data
                                        connection = psycopg2.connect(user="user",
                                                                      password="pass",
                                                                      host="ip",
                                                                      port="port",
                                                                      database="name database")

                                        cursor = connection.cursor()
                                        insert_query = """ INSERT INTO tabl (
                                        id,
                                        kodterr,
                                        snilspravo,
                                        snilsdeadzl,
                                        namefailpravo, 
                                        idizve,
                                        dateotp,
                                        idsmev,
                                        sdossmev,
                                        sdosepgy) 
                                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        record_insert = (
                                        uid, kodterr, snilspravo, snilsymer, namefiles, idizve, dataotp, cmevid,
                                        statuscmev, statusdosva)
                                        cursor.execute(insert_query, record_insert)
                                        connection.commit()
                                        print("1 запись успешно вставлена")
                                        cursor.execute("SELECT * from pfr")
                                        record = cursor.fetchall()
                                        print("Результат", record)
                                    except (Exception, Error) as error:
                                        print("Ошибка при работе с PostgreSQL", error)
                                        logging.critical('Ошибка при работе с PostgreSQL')
                                    finally:
                                        if connection:
                                            cursor.close()
                                            connection.close()
                                            print("Соединение с PostgreSQL закрыто")
                                            logging.info("Добавлена информация в БД")
                                            with open('pdffiles/' + uid + '.pdf', "rb") as f:
                                                bytess = f.read()
                                                encoded = pybase64.b64encode(bytess)
                                                base = encoded.decode('utf-8')
                                                #make xml files
                                            msg = ((
                                                       '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:xml="http://www.w3.org/XML/1998/namespace">'
                                                       '<soap:Header>'
                                                       '<InfoList>'
                                                       '<Info>'
                                                       f'<ID> {idizve} </ID>'
                                                       f'<CodeTerritori>{kodterr}</CodeTerritori>'
                                                       f'<SnilsAssignee>{snilspravo}</SnilsAssignee>'
                                                       f'<SnilsZL> {snilsymer} </SnilsZL>'
                                                       f'<NameFiles> {namefiles} </NameFiles>'
                                                       f'<Date> {dataotp} </Date>'
                                                       f'<IdSMEV> {cmevid} </IdSMEV>'
                                                       f'<StatusSmev> {statuscmev} </StatusSmev>'
                                                       f'<StatusEPGY> {statusdosva} </StatusEPGY>'
                                                       f'<Document> {base} </Document>'
                                                       '</Info>'
                                                       '</InfoList>'
                                                       '</soap:Header>'
                                                       '</soap:Envelope>'))
                                            message = bytes(msg, 'utf-8')
                                            with open('xmlfiles/' + uid + "xmlfile.xml", "w") as output:
                                                output.write(str(message.decode('utf-8')))
                                                logging.info('Создан xml файл снилс -' + str(snilspravo) + ' - Удачно')
                    if os.listdir('zipf'):
                        continue
                        print("Hello")
                        break
get_zip()