import configparser
import pymysql

config = configparser.ConfigParser()
config.read('config.ini')

def connect():
    return pymysql.connect(
        host = config['mysqlDB']['host'],
        user = config['mysqlDB']['user'],
        passwd = config['mysqlDB']['pass'],
        db = config['mysqlDB']['db'],
        port = int(config['mysqlDB']['port'])
    )

if __name__ == '__main__':
    connect()