import requests
import re
from bs4 import BeautifulSoup
# import pymysql
import storage
import sys
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

def LINE():
    return sys._getframe(1).f_lineno

debug=config['app']['debug']
debug = config.getboolean('app','debug')
print("####",debug,"####")
if debug : print("debug is set to True")
else : print("debug is set to False")

# debug=True
debug=False
mydb = storage.connect()

# Open database connection
# prepare a cursor object using cursor() method
cursor = mydb.cursor()

def insertProgram(cols):
    if cols != False:
        sql = "INSERT IGNORE INTO programs (genre_id, name, created_at) VALUES (%s, %s, NOW())"
        if debug:
            print(LINE(), " : sql : ", sql, cols)
        try:
            cursor.execute(sql, cols)
            mydb.commit()
        except mydb.Error as err:
            print(err)

def get_genres():
    sql = "SELECT id,name,table_name FROM genres"
    cursor.execute(sql)
    rows = cursor.fetchall()
    # Build the nested object
    nested_object = {}
    for row in rows:
        genre_id, genre_name, table_name = row
        nested_object[table_name] = {
            "id": genre_id,
            "name": genre_name,
        }

    # Print the nested object
    print(nested_object)
    return nested_object

def getDirMixdropUrl(url):
    for table_name, genre_info in genres.items():
        print("Table Name:", table_name)
        genre_id = genre_info["id"]
        genre_name = genre_info["name"]
        print("Genre ID:", genre_id)
        print("Genre Name:", genre_name)
        print("-----------")

        url = domain + '/' + table_name
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "lxml")
        if debug : print("50 : url : " + url)

        eles = soup.findAll("li", attrs={"class": "wr-subject"})
        for ele in eles:
            name = ele.a.get_text().strip()
            name = re.sub(r'^\d{6}\s*', '', name)
            name = re.sub(r'\s*제\s?\d+회$', '', name)
            name = name.strip()
            print(name)
            insertProgram([genre_id, name])
    return

domain = "https://dongyoungsang.club"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
}

genres = get_genres()
getDirMixdropUrl(genres)

cursor.close()
mydb.close()
