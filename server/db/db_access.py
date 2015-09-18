from models import *
from app import db, logger

def row_to_dict(r):
    return {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}

################
# Projects
################
def get_project(project_id):
    project = Project.query.get_or_404(int(project_id))
    return row_to_dict(project)

# https://github.com/sloria/PythonORMSleepy/blob/master/sleepy/api_sqlalchemy.py
def delete_project(project_id):
    project = Project.query.get_or_404(int(project_id))
    db.session.delete(project)
    db.session.commit()
    return project

# TODO General create function?
def get_projects(**kwargs):
    # TODO Ensure auth here
    if kwargs.get(project_id):
        logger.info("Requested project given project_id: %s", project_id)
        result = Project.query.filter(Project.id == project_id).first()
        if result:
            return row_to_dict(result)
        else:
            return {}, 404
    # TODO Filter by user_id
    else:
        result = Project.query.all()
    return [ row_to_dict(row) for row in result ]


def create_project(**kwargs):
    new_project = Project(title=title)
    db.add(new_project)
    return db.commit()


def update_project(update_doc, projectID):
    db.query(Project).filter(Project.id == projectID).update(update_doc).delete()
    db.commit()


def delete_projects(project_ids = None):
    # Synchronize session?
    # How to deal with the filter object?
    for project_id in project_ids:
        db.query(Project).filter(Project.id == project_id).delete()
    return db.commit()

model_from_name = {
    'Project': Project,
    'Dataset_Properties': Dataset_Properties,
}

def get_objects(project_id, model_name, **kwargs):
    model = model_from_name[model_name]
    model.query.filter_by(**kwargs).all()

# TODO Upsert?

def update_objects(project_id, model_name, **kwargs):
    model = model_from_name[model_name]
    model.query.filter_by(**kwargs).all()

def insert_objects(project_id, model_name, **kwargs):
    model = model_from_name[model_name]
    model.query.filter_by(**kwargs).all()

# Cascade
def delete_objects(project_ids = None):
    # Synchronize session?
    # How to deal with the filter object?

    model = model_from_name[model_name]

    for project_id in project_ids:
        model.query.filter_by(**kwargs).delete()
    db.session.commit()

################
# Datasets
################
def get_datasets(**kwargs):
    return

################
# Dataset Properties
################

################
# Field Properties
################

################
# Specifications
################

################
# Exported Specifications
################

################
# Users
################
