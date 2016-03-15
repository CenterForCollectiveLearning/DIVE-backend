from dive.core import create_app
from dive.db.accounts import register_user
from dive.db.constants import Role

users = [
    {
        'username': 'admin',
        'password': 'b9f5bcd98fe1627e37cd87a27b4a7fd6',  # 'dive',
        'email': 'usedive@gmail.com',
        'name': 'DIVE Admin',
        'role': Role.ADMIN.value
    },
    {
        'username': 'colgate',
        'password': '8d777f385d3dfec8815d20f7496026dc',  # 'data',
        'email': 'whoiskevinhu@gmail.com',
        'name': 'Colgate-Palmolive User',
        'role': Role.USER.value
    },
    {
        'username': '""',
        'password': 'b9f5bcd98fe1627e37cd87a27b4a7fd6',  # ''
        'email': '',
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
