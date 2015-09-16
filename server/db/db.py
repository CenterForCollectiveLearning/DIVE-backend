import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils import database_exists, create_database
from models import *


# Establish database session
engine = create_engine('postgresql+psycopg2://localhost:5432/dive', convert_unicode=True)
if not database_exists(engine.url):
    print "Creating database"
    # TODO Fix error to not call below if not created
    engine = create_database(engine.url)
DBSession = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

Base = declarative_base()
Base.query = DBSession.query_property()
def init_db():
    # Recreate each time for testing
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def populate_db():
    db_session = DBSession()
    objects = [
        Project(title='Test Project'),
        Dataset(file_name='test.csv')
    ]
    db_session.bulk_save_objects(objects)
    db_session.commit()
    db_session.remove()

# @app.teardown_appcontext
def shutdown_session(exception=None):
    DBSession.remove()

def row_to_dict(r):
    return {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}

# http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/

# def get_project():
#     row_to_dict(session.query(Project).first())
#
# def create_project():
#     new_project = Project(title='Test Project')
#     session.add(new_project)
#     session.commit()
#
# def update_project(update_doc, projectID):
#     session.query(Project).filter(Project.id == projectID).update(update_doc).delete()
#     session.commit()
#
# def delete_project(delete_doc):
#     # Synchronize session?
#     # How to deal with the filter object?
#     session.query(Project).filter(Project.id == projectID).delete()
#     session.commit()


# new_project = Project(title='Test Project')
# session.add(new_project)
# db_session.commit()
