from db import DBSession
from models import *
from app import logger

def row_to_dict(r):
    return {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}

# http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/
def get_projects(project_id=None, user_id=None):
    db_session = DBSession()
    logger.info(project_id)
    if project_id:
        result = db_session.query(Project).filter(Project.id == project_id).first()
        return row_to_dict(result)
    else:
        result = db_session.query(Project).all()
    return [ row_to_dict(row) for row in result ]

def create_project():
    db_session = DBSession()
    new_project = Project(title='Test Project')
    db_session.add(new_project)
    db_session.commit()
    db_session.remove()

def update_project(update_doc, projectID):
    session.query(Project).filter(Project.id == projectID).update(update_doc).delete()
    session.commit()

def delete_projects(delete_doc):
    # Synchronize session?
    # How to deal with the filter object?
    session.query(Project).filter(Project.id == projectID).delete()
    session.commit()
