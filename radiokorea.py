import requests
import re
from bs4 import BeautifulSoup
# import pymysql
import storage

import datetime

# Open database connection
mydb = storage.connect()

# prepare a cursor object using cursor() method
cursor = mydb.cursor()

def initTable(cur):
    ## create table ktvprograms
    sql = """CREATE table IF NOT EXISTS `jobs` ( 
        `id` int(10) unsigned NOT NULL AUTO_INCREMENT, \
        `area` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
        `subject` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT '', \
        `link` char(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL, \
        `writer` char(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL, \
        `keyword` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
        `posted_at` datetime NULL DEFAULT CURRENT_TIMESTAMP, \
        `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP, \
        `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
        PRIMARY KEY (`id`), \
        UNIQUE KEY `unique` (`link`) \
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci """

    try:
        cur.execute(sql)
        mydb.commit()
    # except:
    except pymysql.Error as err:
        print (err)
        # print ("Error: unable to create table")
        # pass

    # sql = "SELECT * FROM tmp"
    # try:
    #     # Execute the SQL command
    #     cursor.execute(sql)
    #     # Fetch all the rows in a list of lists.
    #     results = cursor.fetchall()
    #     for row in results:
    #         id = row[0]
    #         # Now print fetched result
    #         print ("id = %d" % (id))
    # except:
    #     print ("Error: unable to fetch data")

    # disconnect from server

def insertDb(rows):
    for row in rows:
        sql = "INSERT IGNORE INTO jobs ( area, subject, link, writer, keyword, posted_at ) VALUE ( %s, %s, substring_index(%s, '&', 2), %s, %s, %s )"
        try:
            # Execute the SQL command
            # cursor.execute(sql, tuple(cols))
            cursor.execute(sql,(
                row["area"],
                row["subject"],
                row["link"],
                row["writer"],
                row["keyword"],
                row["pdate"],
                ))
            mydb.commit()
        except pymysql.Error as err:
            print (err)

def getLastdate(cur):
    sql = "SELECT ifnull(max(posted_at), now() - interval 7 day)  lastposted FROM jobs"
    try:
        # Execute the SQL command
        cursor.execute(sql)
        lastdate = cursor.fetchone()
        return lastdate[0]
    except:
        print ("Error: unable to fetch data")


def getData(link, headers, ldate):
    res = requests.get(link, headers=headers)
    res.raise_for_status()
    soup = BeautifulSoup(res.content, "html.parser")
    # print(soup.prettify())
    # return False
    data = []

    lines = soup.find("ul", attrs={"class":"board_list"}).findAll("li")
    # print(lines)

    # return True
    # soup1 = BeautifulSoup(tmp.text, "lxml")
    # lines = soup1.findAll("a", attrs={"class":"thumb"})
    for line in lines:
        # print(line.findChild()["href"].value)
        # link = line["href"].split("?")[1]
        try:
            # link = line["href"]
            plink = line.find("a", attrs={"class":"thumb"})["href"].split("?")[1]
            print("1", plink)
            subject = (line.find("div", attrs={"class":"subject"}).text).strip()
            area = (line.find("div", attrs={"class":"area"}).text).strip()
            writer = (line.find("div", attrs={"class":"writer"}).text).strip()
            pdatestr = (line.find("div", attrs={"class":"date"}).text).strip()
            dd = pdatestr.split('.')
            pdate = datetime.datetime(int('20'+dd[2]),int(dd[0]),int(dd[1]))
            posted_at = getExactPDate(plink)
            print("3", posted_at)
            pd = posted_at.strftime("%m-%d-%Y %H:%M:%S")
            print("4", pd)
            # print(subject,area,writer,pdate,plink, end="\n"*1)
            # print(subject, area, writer, pdate, plink)
            if(pdate < ldate):
                break

            ddict = {
                "area" : area,
                "subject" : subject,
                "writer" : writer,
                "pdate" : posted_at.strftime("%m-%d-%Y %H:%M:%S"),
                "link" : plink,
            }

            # print(ddict)
            data.append(ddict)
        except:
            print(line)
        print('---------------------------------------')
    # print(data)
    return data

def getExactPDate(link):
    dlink = domain+path+link
    print("2", dlink)
    pdate = ''

    dp = requests.get(dlink, headers=headers)
    dp.raise_for_status()
    soup2 = BeautifulSoup(dp.content, "html.parser")
    # print(soup2.prettify())
    #data = []
    words = soup2.find("div", attrs={"class":"date"}).text.split('|')
    return words[1].split(': ')[1].strip()

domain = "https://www.radiokorea.com/"
path = "bulletin/bbs/board.php?"
headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"}
def main():
    keywords = {
        "웹",
        "개발",
        "프로그",
        "web",
        "develop",
        "python",
        "golang",
        "django",
        "php"
    }
    args = "bo_table=c_jobs&sca=&sfl=wr_subject&stx="
    tail = "&sop=and"
    # lastupdated_at = datetime.datetime(2023,8,11)
    lastupdated_at = getLastdate(cursor)
    ddata = []

    for keyword in keywords:
        search = domain + path + args + keyword + tail
        print(search + "\n")
        ddd = getData(search, headers, lastupdated_at)
        # print(cols)
        for dd in ddd:
            dd["link"] = domain + path + dd["link"]
            dd["keyword"] = keyword
            print(dd)
            ddata.append(dd)
    
    # print(ddata)
    insertDb(ddata)

    # sql = "SELECT program_name, title, link, channel, updated_at FROM ktvprograms WHERE updated_at > now() - interval 7 day ORDER BY updated_at DESC"
    # try:
    #     # Execute the SQL command
    #     cursor.execute(sql)
    #     # Fetch all the rows in a list of lists.
    #     results = cursor.fetchall()
    #     for row in results:
    #         program_name = row[0]
    #         title = row[1]
    #         link = row[2]
    #         channel = row[3]
    #         updated_at = row[4]
    #         # Now print fetched result
    #         print ("%s > %s : %s at %s" % (program_name, title, link, updated_at))
    # except:
    #     print ("Error: unable to fetch data")

if __name__ == '__main__':
    # initTable(cursor)
    main()
# disconnect from server
mydb.close()
