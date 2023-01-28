import requests
import re
from bs4 import BeautifulSoup
# import pymysql
import storage

mydb = storage.connect()


# Open database connection
# prepare a cursor object using cursor() method
cursor = mydb.cursor()

# create table ktvprograms
sql = """CREATE table IF NOT EXISTS `ktvprograms` ( 
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT, \
    `program_name` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
    `title` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
    `link` char(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL, \
    `channel` char(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL, \
    `parent_id` int(10) unsigned , \
    `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP, \
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
    PRIMARY KEY (`id`), \
    UNIQUE KEY `unique` (`program_name`,`title`,`link`) \
) ENGINE=InnoDB AUTO_INCREMENT=3772 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci """

try:
    cursor.execute(sql)
    mydb.commit()
    # except:
except pymysql.Error as err:
    if debug : print("31 : ", err)
    # print ("Error: unable to create table")
    # pass

# create table ktvprograms
sql2 = """CREATE table IF NOT EXISTS `ktvprogram_parents` ( 
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT, \
    `program_name` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
    `title` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
    `link` char(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL, \
    `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP, \
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
    PRIMARY KEY (`id`), \
    UNIQUE KEY `unique` (`program_name`,`title`, `link`) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci """

try:
    cursor.execute(sql2)
    mydb.commit()
# except:
except pymysql.Error as err:
    if debug : print("31-1 : ", err)
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

def insertDb_parent(cols):
    if cols != False:
        sql = "INSERT IGNORE INTO ktvprogram_parents ( program_name, title, link, created_at ) VALUE ( %s, %s, %s, now() )"
        try:
            cursor.execute(sql, tuple(cols))
            mydb.commit()
            return cursor.lastrowid
            # cursor.insert_id()
        except pymysql.Error as err:
            print(err)

def insertDb(cols):
    if cols != False:
        sql = "INSERT IGNORE INTO ktvprograms ( parent_id, link, channel, created_at ) VALUE ( %s, %s, %s, now() )"
        try:
            cursor.execute(sql, tuple(cols))
            mydb.commit()
        except pymysql.Error as err:
            print(err)

def get_parent_id(program, title, link):
    sql = "SELECT id FROM ktvprogram_parents WHERE program_name=%s AND title=%s AND link=%s"
    cursor.execute(sql, tuple([program, title, link]))
    parent = cursor.fetchone()
    return parent[0]

def getDirMixdropUrl(prog, keyw, url):
    #url = "https://dongyoungsang.club/bbs/board.php?bo_table=en&sca=&sfl=wr_subject&sop=and&stx=" + keyw + "&page=1"
    url = url + keyw
    # print(url)
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "lxml")

    eles = soup.findAll("li", attrs={"class": "wr-subject"})
    # print(eles)
    for ele in eles:
        name = ele.a.get_text()
        # print(1, name)
        if keyw in name.lower():
            url2 = ele.a["href"]
            # break

            name = name.strip()
            res2 = requests.get(url2, headers=headers)
            soup2 = BeautifulSoup(res2.text, "lxml")

            tmp = soup2.find("div", id="bo_v_con")
            ele2 = tmp.find_next_sibling("div")

            url3 = domain + ele2.a["href"]
            res3 = requests.get(url3, headers=headers)
            soup3 = BeautifulSoup(res3.text, "lxml")
            parent_id = insertDb_parent([prog, name, url3])
            if(parent_id==0):
                parent_id = get_parent_id(prog, name, url3)

            for ch in channels:
                regs = re.compile(ch)
                tmp = soup3.find("a", text=regs)
                # print(2, tmp)
                # if tmp != None:
                #     break

                try:
                    furl = tmp["href"]
                    if ch=="GG" :
                        insertDb([parent_id, furl, ch])
                        continue
                    res4 = requests.get(furl, headers=headers)
                    soup4 = BeautifulSoup(res4.text, "lxml")
                    aa = soup4.find("embed")
                    rfurl = aa["src"]
                    res5 = requests.get(rfurl, headers=headers)
                    soup5 = BeautifulSoup(res5.text, "lxml")
                    print(prog, " > ", name, ": ", rfurl)
                    insertDb([parent_id, rfurl, ch])
                    # return [prog, name, rfurl, ch]
                except:
                    print(prog, " > ", name, ": ")

""

programs = {
    "en" :{
        # "도시어부3": "도시어부3",
        # "슬기로운 의사생활 시즌2": "슬기로운",
        # "골 때리는 그녀들": "때리는",
        # "나는 solo": "solo",
        # "주주총회": "주주총회",
        # "환승 연예2": "환승",
        # "씨름의 여왕": "씨름의 여왕",
        # "오은영의 금쪽 상담소": "오은영",
        # "돌싱글즈3": "돌싱글즈",
        # "꼬리에 꼬리를 무는 그날 이야기": "꼬리",
        # "씨름의 제왕": "씨름의 제왕",
        # "벌거벗은 세계사":"벌거벗은",
    },
    "dra":{
        # "슬기로운 의사생활 시즌2": "슬기로운",
        "재벌집 막내아들":"재벌집",
        # "스타트업":"스타트업"
        # "가우스전자":"가우스전자"
    }
}
channels = ["MIXDROP", "FLASHVID", "HLSPLAY", "SUPERVID", "GG"]
domain = "https://dongyoungsang.club"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"}
url_head = "https://dongyoungsang.club/bbs/board.php?bo_table="
url_tail = "&sca=&sfl=wr_subject&sop=and&stx="

for table_name in programs:
    print("135 : ", table_name)
    for program in programs[table_name]:
        url = url_head + table_name + url_tail
        # print(programs[table_name][program])
        # print(program)
        cols = getDirMixdropUrl(program, programs[table_name][program], url)


# for program in programs:
#     cols = getDirMixdropUrl(program, programs[program])
#     print(cols)
#     # if(cols):
#     #     insertDb(cols)

sql = "SELECT p.program_name, p.title, c.link, c.channel, c.updated_at FROM ktvprograms c, ktvprogram_parents p WHERE c.parent_id=p.id AND c.updated_at > now() - interval 7 day ORDER BY c.updated_at DESC"
try:
    # Execute the SQL command
    cursor.execute(sql)
    # Fetch all the rows in a list of lists.
    results = cursor.fetchall()
    for row in results:
        program_name = row[0]
        title = row[1]
        link = row[2]
        channel = row[3]
        updated_at = row[4]
        # Now print fetched result
        # print ("%s > %s : %s at %s" % (program_name, title, link, updated_at))
except:
    print("Error: unable to fetch data")

# disconnect from server
mydb.close()
