from python_utils.rest_connector import RestConnector

class AuthProxy():
    def __init__(self, host, email="", password="", cookie=""):
        self.conn = RestConnector(host)
        if cookie:
            self.conn.cookie = cookie
        else:
            self.conn.post("/authenticate",
                           {"email": email, "password": password})

    def getAccounts(self):
        h,r=self.conn.get("/accounts")
        return r

    def getUsers(self):
        h,r=self.conn.get("/users")
        return r

    def createAccount(self, name):
        h,r=self.conn.post("/accounts", {"name": name})
        return r

    def createUser(self, account_id, role, name, email, password):
        h,r=self.conn.post("/users", {"account_id": account_id, "role": role,
            "name": name, "email": email, "password": password})
        return r

    def getToken(self, id):
        h,r = self.conn.get("/users/%d/longtoken" % id)
        return r
