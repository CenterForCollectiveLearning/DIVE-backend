from enum import Enum

# User role
ADMIN = 0
USER = 1

# Users
class Role(Enum):
    ADMIN = u'admin'
    USER = u'user'

# User Status
class User_Status(Enum):
    NEW = u'new'
