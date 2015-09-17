import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy_utils import database_exists, create_database

# from app import app
from .models import *


# Establish database session
# engine = create_engine(app.config['DB_URI'], convert_unicode=True)
engine = create_engine('postgresql+psycopg2://localhost:5432/dive', convert_unicode=True)

if not database_exists(engine.url):
    app.logger.info("Database doesn't exist, creating now.")
    engine = create_database(engine.url)

DBSession = scoped_session(sessionmaker(autocommit=False,
                                        autoflush=False,
                                        bind=engine))
db_session = DBSession()  # Starting with module-wide session
Base.query = DBSession.query_property()

# Base.metadata.drop_all(bind=engine)
# Base.metadata.create_all(bind=engine)

def init_db():
    app.logger.info("Initializing DB")
    # Recreate each time for testing
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def populate_db():
    app.logger.info("Populating DB")
    db_session = DBSession()
    objects = [
        Project(title='Test Project'),
        Dataset(file_name='test.csv')
    ]
    db_session.bulk_save_objects(objects)
    db_session.commit()
    db_session.remove()


# @app.teardown_appcontext
# def shutdown_session(exception=None):
#     app.logger.info("Tearing down DB")
#     DBSession.remove()
