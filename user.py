class User:
    def __init__(self, username, password, fname, lname, status, active):
        self.username = username
        self.fname = fname
        self.lname = lname
        self.status = status
        self.active = active

    def to_dct(self):
        return {'username': self.username, 'fname': self.fname , 'lname': self.lname,
                'status': self.status, 'active': self.active}