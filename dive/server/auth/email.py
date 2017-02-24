from flask_mail import Message
from dive.base.core import mail

import logging
logger = logging.getLogger(__name__)


def send_email(to, subject, html):
    msg = Message(subject, recipients=[ to ])

    msg.html = html
    mail.send(msg)
