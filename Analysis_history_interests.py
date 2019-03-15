from pymongo import MongoClient


class MongoDB:
    def __init__(self):
        self.db_host = ''
        self.db_name = ''
        self.db_port = 27017
        self.db_user = 'app'
        self.db_pwd = ''
        self.db_report_name = 'report'
        self.db_ads_name = 'ads'
        self.client = None
        self.db = None

    def mongodb_conn(self):
        client = MongoClient(host=self.db_host, port=self.db_port)
        db = client.get_database(self.db_name)
        db.authenticate(self.db_user.strip(), self.db_pwd.strip())
        self.client = client
        self.db = db

    def client_close(self):
        self.client.close()



