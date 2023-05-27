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

def insertDb_genre(cols):
    if cols != False:
        sql = "INSERT IGNORE INTO genres ( name, table_name, created_at, updated_at ) VALUE ( %s, %s, now(), now() )"
        try:
            cursor.execute(sql, tuple(cols))
            mydb.commit()
            # return cursor.lastrowid
            # cursor.insert_id()
        except pymysql.Error as err:
            print(err)

url = "https://dongyoungsang.club"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
}

print(LINE(), " : url : " + url)
res = requests.get(url, headers=headers)
res.raise_for_status()
soup = BeautifulSoup(res.text, "lxml")
eles = soup.findAll("a", attrs={"class": "sharp none list-group-item border0"})
print(eles)
for ele in eles:
    name = ele.get_text()
    href = ele["href"]
    print(name, href[1:])
    insertDb_genre( [name, href[1:]] )
    # if keyw in name:
    #     url2 = ele.a["href"]
        # break

# disconnect from server
mydb.close()
