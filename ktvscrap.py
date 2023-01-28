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
    print(err)
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


def insertDb(cols):
    # print(cols)
    if cols != False:
        #     # cols.append(file)
        sql = "INSERT IGNORE INTO ktvprograms ( program_name, title, link, channel, created_at ) VALUE ( %s, %s, %s, %s, now() )"
        # sql = "INSERT IGNORE INTO ktvprograms ( program_name, title, link, channel, created_at ) VALUE ( '1호가 될 순 없어', '092020 1호가 될 순 없어 제18회', 'https://mixdrop.to/e/4ndodd97i4o3nv', 'MIXDROP', now() )"
        # VALUE ( %s, %s, %s, %s, now() )"""
        try:
            # Execute the SQL command
            # cursor.execute(sql, tuple(cols))
            cursor.execute(sql, tuple(cols))
            mydb.commit()
        except pymysql.Error as err:
            print(err)


def getDirMixdropUrl(prog, keyw):
    url = "https://dongyoungsang.club/bbs/board.php?bo_table=en&sca=&sfl=wr_subject&sop=and&stx=" + keyw
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "lxml")

    eles = soup.findAll("li", attrs={"class": "wr-subject"})
    for ele in eles:
        name = ele.a.get_text()
        if keyw in name:
            url2 = ele.a["href"]
            break

    name = name.strip()
    res2 = requests.get(url2, headers=headers)
    soup2 = BeautifulSoup(res2.text, "lxml")

    tmp = soup2.find("div", id="bo_v_con")
    ele2 = tmp.find_next_sibling("div")

    url3 = domain + ele2.a["href"]
    res3 = requests.get(url3, headers=headers)
    soup3 = BeautifulSoup(res3.text, "lxml")

    for ch in channels:
        regs = re.compile(ch)
        tmp = soup3.find("a", text=regs)
        if tmp != None:
            break
    try:
        furl = tmp["href"]
        res4 = requests.get(furl, headers=headers)
        soup4 = BeautifulSoup(res4.text, "lxml")
        aa = soup4.find("embed")
        rfurl = aa["src"]
        res5 = requests.get(rfurl, headers=headers)
        soup5 = BeautifulSoup(res5.text, "lxml")
        insertDb([prog, name, rfurl, ch])
        print(prog, " > ", name, ": ", rfurl)

        # return [prog, name, rfurl, ch]
    except:
        print(prog, " > ", name, ": ")


programs = {
    # "아이돌 스타 선수권대회": "아이돌 스타 선수권",
    "스트릿 우먼 파이터":"스트릿 우먼 파이터",
    "뭉쳐야 쏜다": "뭉쳐야",
    "유 퀴즈 온 더 블럭": "블럭",
    # "배달GAYO 신비한 레코드샵": "레코드샵",
    # "히든싱어":"히든싱어 6",
    # "쇼미더머니 9":"쇼미더머니 9",
    "전지적 참견 시점": "전지적",
    # "고등래퍼": "고등래퍼",
    # "싱어게인": "싱어게인",
    "환승 연예": "환승",
    "아는 형님": "아는 형님",
    "미운 우리 새끼": "미운 우리 새끼",
    "영화가 좋다": "영화가 좋다",
    "정글의 법칙": "정글의 법칙",
    "놀면 뭐하니": "놀면 뭐하니",
    "라디오 스타": "라디오스타",
    "코미디 빅리그": "빅리그",
    "환승 연예": "환승",
    # "여자들의 은밀한 파티" :"여자들의 은밀한 파티",
    "나 혼자 산다": "나 혼자 산다 제",
    "1호가 될 순 없어": "1호",
    "골 때리는 그녀들": "때리는",
    "오은영의 금쪽 상담소": "오은영",
    "맛있는 녀석들": "맛있는 녀석들"
}
channels = ["MIXDROP", "FLASHVID", "HLSPLAY", "SUPERVID"]
# channels = ["SUPERVID"]
domain = "https://dongyoungsang.club"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
    }

for program in programs:
    cols = getDirMixdropUrl(program, programs[program])
    # print(cols)
    # if(cols):
        # print(cols)
        # insertDb(cols)

sql = "SELECT program_name, title, link, channel, updated_at FROM ktvprograms WHERE updated_at > now() - interval 7 day ORDER BY updated_at DESC"
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
