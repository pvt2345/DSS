import pymysql
from . import config
class DataAccess:
    def __init__(self):
        self.connect()

    def connect(self):
        self.connection = pymysql.connect(host=config.DATABASE_CONFIG['host'],
                                     db=config.DATABASE_CONFIG['dbname'],
                                     user=config.DATABASE_CONFIG['user'],
                                     password=config.DATABASE_CONFIG['password'],
                                     port=config.DATABASE_CONFIG['port'],
                                     charset='utf8mb4',
                                     cursorclass = pymysql.cursors.DictCursor)

    def GetData(self, query ):
        if(self.connection._closed):
            self.connect()
        cursor = self.connection.cursor()
        try:
            # connection.open()
            cursor.execute(query)
            data = cursor.fetchall()
            return data
        # except:
        #     return -1
        finally:
            self.connection.close()


    def ExecuteCommand(self, query : str):
        if(self.connection._closed):
            self.connect()
        cursor = self.connection.cursor()
        try:
            # connection.open()
            cursor.execute(query)
            self.connection.commit()
        finally:
            self.connection.close()


