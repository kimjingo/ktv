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
    if debug : print(LINE(), " : ", err)
    # print ("Error: unable to create table")
    # pass

# create table requesed
sql3 = """CREATE table IF NOT EXISTS `genres` ( 
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT, \
    `name` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
    `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP, \
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
    PRIMARY KEY (`id`), \
    UNIQUE KEY `unique` (`name`) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci """
try:
    cursor.execute(sql3)
    mydb.commit()
except pymysql.Error as err:
    if debug : print(LINE(), " : ", err)

sql4 = """CREATE table IF NOT EXISTS `requests` ( 
    `id` int(10) unsigned NOT NULL AUTO_INCREMENT, \
    `genre_id` int(10) unsigned NOT NULL, \
    `program_name` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
    `keyword` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '', \
    `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP, \
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
    PRIMARY KEY (`id`), \
    FOREIGN KEY (`genre_id`) REFERENCES genres(`id`), \
    UNIQUE KEY `unique` (`genre_id`, `program_name`) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci """
try:
    cursor.execute(sql4)
    mydb.commit()
except pymysql.Error as err:
    if debug : print(LINE(), " : ", err)

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
        if debug : print(LINE(), " : sql : ", sql, cols)
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
    url1 = url + keyw
    res = requests.get(url1, headers=headers)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "lxml")
    if debug : print("50 : url1 : " + url1)

    eles = soup.findAll("li", attrs={"class": "wr-subject"})
    for ele in eles:
        name = ele.a.get_text()
        if keyw in name:
            url2 = ele.a["href"]
            break

    if debug : print("59 : url2 : ", url2)
    name = name.strip()
    log = name
    res2 = requests.get(url2, headers=headers)
    soup2 = BeautifulSoup(res2.text, "lxml")

    tmp = soup2.find("div", id="bo_v_con")
    ele2 = tmp.find_next_sibling("div")

    url3 = domain + ele2.a["href"]
    if debug : print("886 : final page including links : ", url3)
    res3 = requests.get(url3, headers=headers)
    soup3 = BeautifulSoup(res3.text, "lxml")
    parent_id = insertDb_parent([prog, name, url3])
    if(parent_id==0):
        parent_id = get_parent_id(prog, name, url3)
    if debug : print("986 : parent_id : ", parent_id)
    if debug : print(LINE(), " : soup3 : ", soup3)

    for ch in channels:
        if debug : print("118 : channel : ", ch)
        if ch=="MIXDROP" :
            log += " : "
        else :
            log += " , "
        log += ch
        regs = re.compile(ch)
        tmp = soup3.find("a", string=regs)
        if debug : print(LINE(), " : ttttttmp : ", tmp)
        # if tmp != None:
        #     break
        try:
            furl = tmp["href"]
            if debug : print(LINE(), " : furl : ", furl)
            if ch=="GG" :
                if debug : print(LINE(), " : GG : ", furl)
                insertDb([parent_id, furl, ch])
                continue
            if debug : print("79 : ", furl)
            res4 = requests.get(furl, headers=headers)
            soup4 = BeautifulSoup(res4.text, "lxml")
            aa = soup4.find("embed")
            rfurl = aa["src"]
            res5 = requests.get(rfurl, headers=headers)
            soup5 = BeautifulSoup(res5.text, "lxml")
            if debug : print("86 : success : ", parent_id, " > ",  rfurl, ": ", ch)
            log += " -> success"
            insertDb([parent_id, rfurl, ch])

            # return [prog, name, rfurl, ch]
        except:
            if debug : print("90 : fail : ", prog, " > ", name, ": ")
            log += " -> fail"

        # print("95 : ", status, " : ", prog, " > ", name, ": ", url2, " : ", ch)
    print(log)


programs = {
    "en" : {
        "영화가 좋다": "영화가 좋다",
        # "아이돌 스타 선수권대회": "아이돌 스타 선수권",
        # "스트릿 우먼 파이터":"스트릿 우먼 파이터",
        # "뭉쳐야 쏜다": "뭉쳐야",
        "유 퀴즈 온 더 블럭": "블럭",
        # "배달GAYO 신비한 레코드샵": "레코드샵",
        # "히든싱어":"히든싱어 6",
        "하트시그널":"하트시그널",
        #"전지적 참견 시점": "전지적",
        # "고등래퍼": "고등래퍼",
        "쇼미더머니": "쇼미더머니",
        # "싱어게인": "싱어게인",
        "씨름의 여왕": "씨름의 여왕",
        # "환승 연예2": "환승",
        "아는 형님": "아는 형님",
        # "미운 우리 새끼": "미운 우리 새끼",
        # "연애는 직진": "연애는 직진",
        "놀면 뭐하니": "놀면 뭐하니",
        "라디오 스타": "라디오스타",
        "코미디 빅리그": "빅리그",
        # "여자들의 은밀한 파티" :"여자들의 은밀한 파티",
        "나 혼자 산다": "나 혼자 산다 제",
        # "1호가 될 순 없어": "1호",
        "골 때리는 그녀들": "골 때리는 그녀들",
        # "골 때리는 외박": "골 때리는 외박",
        "나는 솔로": "나는 솔로",
        "나는 SOLO": "SOLO",
        # "꼬리에 꼬리를 무는 그날 이야기": "꼬리",
        # "돌싱글즈3": "돌싱글즈",
        "천하제일장사2": "천하제일장사2",
        "씨름의 제왕": "씨름의 제왕",
        "오은영": "오은영",
        # "벌거벗은 세계사":"벌거벗은",
        "맛있는 녀석들": "맛있는 녀석들"
    },
    "dra":{
        # # "슬기로운 의사생활 시즌2": "슬기로운",
        # # "옷소매 붉은 끝동":"옷소매",
        # "가우스전자":"가우스전자"
        # "재벌집 막내아들":"재벌집",
    }
}
channels = [
    # "MIXDROP", 
    # "FLASHVID", 
    # "HLSPLAY", 
    # "SUPERVID",
    "GG",
    "FILEMOON",
    "WATCHSB",
]
url_head = "https://dongyoungsang.club/bbs/board.php?bo_table="
url_tail = "&sca=&sfl=wr_subject&sop=and&stx="
# url = {
    # "table_name"https://dongyoungsang.club/bbs/board.php?bo_table=en&sca=&sfl=wr_subject&sop=and&stx=" + keyw
    # url = "https://dongyoungsang.club/bbs/board.php?bo_table=dra&sca=&sfl=wr_subject&sop=and&stx=" + keyw
# channels = ["SUPERVID"]
domain = "https://dongyoungsang.club"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
    }

for table_name in programs:
    if debug : print("135 : ", table_name)
    for program in programs[table_name]:
        url = url_head + table_name + url_tail
        # print(programs[table_name][program])
        # print(program)
        cols = getDirMixdropUrl(program, programs[table_name][program], url)
        # if debug : print("141 : ", cols)
        # if(cols):
        #     if debug : print("143 : ", cols)
            # insertDb(cols)

sql = "SELECT p.program_name, p.title, c.link, c.channel, c.updated_at FROM ktvprograms c, ktvprogram_parents p WHERE c.parent_id=p.id AND c.updated_at > now() - interval 7 day ORDER BY c.updated_at DESC"
try:
    # Execute the SQL command
    cursor.execute(sql)
    results = cursor.fetchall()
    # Fetch all the rows in a list of lists.
    for row in results:
        # program_name = row[0]
        title = row[1]
        link = row[2]
        channel = row[3]
        updated_at = row[4]
        # Now print fetched result
        # print ("%s > %s : %s at %s" % (program_name, title, link, updated_at))
except:
    if debug : print("161 : Error: unable to fetch data")

# disconnect from server
mydb.close()
