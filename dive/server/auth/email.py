from flask_mail import Message
from dive.base.core import mail

import logging
logger = logging.getLogger(__name__)


def send_email(to, subject, template):
    msg = Message('Testing e-mail',
        recipients=[ 'whoiskevinhu@gmail.com'])
    msg.body = 'testing'
    msg.html = '<b>testing</b>'

    logger.info('Sending e-mail')
    mail.send(msg)
