import math
import time
import random
import string


class InfoGenerator:
    _next_user = -1

    # Generates an integer User ID, the next in sequence
    # starts at 0
    @classmethod
    def generate_user_id(cls):
        cls._next_user += 1
        return cls._next_user

    # Generates a random email address
    @staticmethod
    def generate_email(RANDOM_STRING_LENGTH):
        user = ''.join(random.choices(string.ascii_lowercase,
                                      k=RANDOM_STRING_LENGTH))
        anchor = '@'
        domain = ''.join(random.choices(string.ascii_uppercase,
                                        k=RANDOM_STRING_LENGTH))
        dot_com = '.com'
        
        email = user + anchor + domain + dot_com
        return email

    # Generates a random password
    @staticmethod
    def generate_password(PASSWORD_LENGTH):
        password = ''.join(random.choices(string.ascii_letters + string.digits,
                                          k=PASSWORD_LENGTH))
        return password

    # Generates a random Node name
    @staticmethod
    def generate_node_name(RANDOM_STRING_LENGTH):
        prefix = 'node_'
        timestamp = str(math.floor(time.time() * 1000000))
        rand_string = ''.join(random.choices(string.ascii_uppercase + string.digits,
                                             k=RANDOM_STRING_LENGTH))
        suffix = rand_string + '.' + timestamp
        node_name = prefix + suffix
        return node_name
