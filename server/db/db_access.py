from db import db_session
from models import *
from app import logger

def row_to_dict(r):
    return {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}
#
#
# for attr, value in web_dict.items():
#     q = q.filter(getattr(myClass, attr).like("%%%s%%" % value))
#

# TODO Make these methods of a class?
# TODO keep the details of session, transaction and exception management as far as possible from the details of the program doing its work

# http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/
def get_projects(project_id=None, user_id=None, **kwargs):
    logger.info(project_id)

    # Ensure auth here
    if project_id:
        result = db_session.query(Project).filter(Project.id == project_id).first()
        return row_to_dict(result)
    else:
        result = db_session.query(Project).all()
    return [ row_to_dict(row) for row in result ]


def create_project(title='title'):
    new_project = Project(title=title)
    db_session.add(new_project)
    return db_session.commit()


def update_project(update_doc, projectID):
    db_session.query(Project).filter(Project.id == projectID).update(update_doc).delete()
    db_session.commit()


def delete_projects(project_ids = None):
    # Synchronize session?
    # How to deal with the filter object?
    for project_id in project_ids:
        db_session.query(Project).filter(Project.id == project_id).delete()
    return db_session.commit()
