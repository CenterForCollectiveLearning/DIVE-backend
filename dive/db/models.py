from dive.core import db
from datetime import datetime
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from constants import Role, User_Status


class Project(db.Model):
    __tablename__ = 'project'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.Unicode(250))
    description = db.Column(db.Unicode(2000))
    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class Dataset(db.Model):
    '''
    The dataset is the core entity of any access to data.
    The dataset keeps an in-memory representation of the data model
    (including all dimensions and measures) which can be used to
    generate necessary queries.
    '''
    __tablename__ = 'dataset'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.Unicode(250))
    description = db.Column(db.Unicode())
    creation_date = db.Column(db.DateTime, default=datetime.utcnow)
    update_date = db.Column(db.DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)
    # Store dataset here here?
    data = db.Column(JSONB)

    path = db.Column(db.Unicode(250))
    file_name = db.Column(db.Unicode(250))
    file_type = db.Column(db.Unicode(250))

    dataset_properties = db.relationship('Dataset_Properties', uselist=False, backref="dataset")

    fields_properties = db.relationship('Field_Properties',
        backref="dataset",
        cascade="all, delete-orphan",
        lazy='dynamic')  # Get all field properties

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


# TODO Decide between a separate table and more fields on Dataset
class Dataset_Properties(db.Model):
    __tablename__ = 'dataset_properties'
    id = db.Column(db.Integer, primary_key=True)
    n_rows = db.Column(db.Unicode(250))
    n_cols = db.Column(db.Unicode(250))
    field_names = db.Column(JSONB)
    field_types = db.Column(JSONB)
    field_accessors = db.Column(JSONB)
    is_time_series = db.Column(db.Boolean())
    structure = db.Enum(['wide', 'long'])

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Field_Properties(db.Model):
    __tablename__ = 'field_properties'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(250))  # Have these here, vs. in dataset_properties?
    type = db.Column(db.Unicode(250))  #
    index = db.Column(db.Integer)
    normality = db.Column(JSONB)
    is_unique = db.Column(db.Boolean())
    child = db.Column(db.Unicode(250))
    is_child = db.Column(db.Boolean())
    unique_values = db.Column(JSONB)
    stats = db.Column(JSONB)

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Specification(db.Model):
    __tablename__ = 'specification'
    id = db.Column(db.Integer, primary_key=True)
    generating_prodecure = db.Column(db.Unicode(250))
    type_structure = db.Column(db.Unicode(250))
    viz_type = db.Column(db.Unicode(250))  # TODO Enum?
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


class Group(db.Model):
    __tablename__ = 'group'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(50), unique=True)
    email = db.Column(db.Unicode(120), unique=True)
    password = db.Column(db.Unicode(120))
    role = db.Column(db.SmallInteger, default=Role.USER.value)
    status = db.Column(db.SmallInteger, default=User_Status.NEW.value)


class User(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(50), unique=True)
    email = db.Column(db.Unicode(120), unique=True)
    password = db.Column(db.Unicode(120))
    role = db.Column(db.SmallInteger, default=Role.USER.value)
    status = db.Column(db.SmallInteger, default=User_Status.NEW.value)
