from app import db

from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from constants import Role, User_Status


class Project(db.Model):
    __tablename__ = 'project'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(250))
    # creationDate = db.Column(Date, index=True)
    # updateDate = db.Column(Date, index=True)


# Distinguish from Dataset_Properties?
class Dataset(db.Model):
    __tablename__ = 'dataset'
    id = db.Column(db.Integer, primary_key=True)

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Dataset_Properties(db.Model):
    __tablename__ = 'dataset_properties'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(250))  # convert_unicode?
    path = db.Column(db.String(250))
    file_name = db.Column(db.String(250))
    file_type = db.Column(db.String(250))
    rows = db.Column(db.String(250))
    cols = db.Column(db.String(250))
    field_names = db.Column(JSONB)
    field_types = db.Column(JSONB)
    field_accessors = db.Column(JSONB)
    is_time_series = db.Column(db.Boolean())
    structure = db.Enum(['wide', 'long'])

    fields_properties = db.relationship('Field_Properties',
        backref="dataset",
        cascade="all, delete-orphan",
        lazy='dynamic')  # Get all field properties

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))
    dataset = db.relationship(Dataset)

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Field_Properties(db.Model):
    __tablename__ = 'field_properties'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(250))  # Have these here, vs. in dataset_properties?
    type = db.Column(db.String(250))  #
    index = db.Column(db.Integer)
    normality = db.Column(JSONB)
    is_unique = db.Column(db.Boolean())
    child = db.Column(db.String(250))
    is_child = db.Column(db.Boolean())
    unique_values = db.Column(JSONB)
    stats = db.Column(JSONB)

    dataset_properties_id = db.Column(db.Integer, db.ForeignKey('dataset_properties.id'))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Specification(db.Model):
    __tablename__ = 'specification'
    id = db.Column(db.Integer, primary_key=True)
    generating_prodecure = db.Column(db.String(250))
    type_structure = db.Column(db.String(250))
    viz_type = db.Column(db.String(250))  # TODO Enum?
    args = db.Column(JSONB)
    meta = db.Column(JSONB)
    score = db.Column(JSONB)
    stats = db.Column(JSONB)

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Exported_Specification(db.Model):
    __tablename__ = 'exported_specification'
    id = db.Column(db.Integer, primary_key=True)
    conditionals = db.Column(JSONB)
    config = db.Column(JSONB)

    specification_id = db.Column(db.Integer, db.ForeignKey('specification.id'))
    specification = db.relationship(Specification)

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class User(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), unique=True)
    email = db.Column(db.String(120), unique=True)
    password = db.Column(db.String(120))
    role = db.Column(db.SmallInteger, default=Role.USER.value)
    status = db.Column(db.SmallInteger, default=User_Status.NEW.value)
