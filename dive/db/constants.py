from enum import Enum

# User role
ADMIN = 0
USER = 1

# Users
class Role(Enum):
    ADMIN = 'admin'
    USER = 'user'

# User Status

class User_Status(Enum):
    NEW = 'new'
