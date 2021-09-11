
# Stores user metadata
class UserData:
    def __init__(self, email, password):
        self._email = email
        self._password = password

    def __str__(self):
        return(f'Email: {self._email}, Password: {self._password}')

# Stores user id and metadata class
class UserInfo:
    def __init__(self, user_id, user_data):
        self._user_id = user_id
        self._user_data = user_data

    def __str__(self):
        return(f'User ID: {self._user_id}, {self._user_data}')

    @property
    def user_id(self):
        return self._user_id

    @property
    def user_data(self):
        return self._user_data
