from dive.core import create_app
from dive.db.accounts import register_user
from dive.db.constants import Role

users = [
    {
        'username': 'admin',
        'password': 'dive',
        'email': 'usedive@gmail.com',
        'name': 'DIVE Admin',
        'role': Role.ADMIN.value
    },
    {
        'username': 'colgate',
        'password': 'data',
        'email': 'whoiskevinhu@gmail.com',
        'name': 'Colgate-Palmolive User',
        'role': Role.USER.value
    },
    {
        'username': 'diveuser',
        'password': '',
        'email': 'kzh@mit.edu',
        'name': 'DIVE Test User',
        'role': Role.USER.value
    },
]

from dive.core import create_app
app = create_app()

with app.app_context():
    for user in users:
        register_user(
            user['username'],
            user['email'],
            user['password'],
            user['name'],
            user['role']
        )
