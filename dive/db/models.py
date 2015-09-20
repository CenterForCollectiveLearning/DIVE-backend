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

    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    users = db.relationship("User")
    # TODO Define relationships for other one-to-manys?

    def __repr__(self):
        return "<Project - ID: %s, Title: %s>" % (self.id, self.title)

# TODO Use mixins and custom base classes to support dataset -> postgres?
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
    type = db.Column(db.Unicode(250))
    orig_type = db.Column(db.Unicode(250))

    # One-to-one with dataset_properties
    dataset_properties = db.relationship('Dataset_Properties', uselist=False, backref="dataset")

    # One-to-many with field_properties
    fields_properties = db.relationship('Field_Properties',
        backref="dataset",
        cascade="all, delete-orphan",
        lazy='dynamic')  # Get all field properties

    # Many-to-one with project
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
    structure = db.Enum(['wide', 'long'])
    is_time_series = db.Column(db.Boolean())


    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Field_Properties(db.Model):
    __tablename__ = 'field_properties'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(250))  # Have these here, vs. in dataset_properties?
    type = db.Column(db.Unicode(250))
    index = db.Column(db.Integer)  # TODO Tie this down with a foreign key?
    normality = db.Column(JSONB)
    is_unique = db.Column(db.Boolean())
    child = db.Column(db.Unicode(250))
    is_child = db.Column(db.Boolean())
    unique_values = db.Column(JSONB)
    stats = db.Column(JSONB)

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)

# TODO Make this not dataset-specific?
class Spec(db.Model):
    '''
    Many-to-one with Dataset
    '''
    __tablename__ = 'spec'
    id = db.Column(db.Integer, primary_key=True)
    generating_procedure = db.Column(db.Unicode(250))
    type_structure = db.Column(db.Unicode(250))
    viz_type = db.Column(db.Unicode(250))  # TODO Enum?
    args = db.Column(JSONB)
    meta = db.Column(JSONB)
    score = db.Column(JSONB)

    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'))
    dataset = db.relationship(Dataset)

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Exported_Spec(db.Model):
    '''
    Many-to-one with Specification
    '''
    __tablename__ = 'exported_spec'
    id = db.Column(db.Integer, primary_key=True)
    conditionals = db.Column(JSONB)
    config = db.Column(JSONB)

    spec_id = db.Column(db.Integer, db.ForeignKey('spec.id'))
    spec = db.relationship(Spec)

    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    project = db.relationship(Project)


class Group(db.Model):
    '''
    One-to-many with User
    '''
    __tablename__ = 'group'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(50), unique=True)
    email = db.Column(db.Unicode(120), unique=True)
    password = db.Column(db.Unicode(120))
    role = db.Column(db.SmallInteger, default=Role.USER.value)
    status = db.Column(db.SmallInteger, default=User_Status.NEW.value)


class User(db.Model):
    '''
    Many-to-one with Group
    '''
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Unicode(50), unique=True)
    email = db.Column(db.Unicode(120), unique=True)
    password = db.Column(db.Unicode(120))
    role = db.Column(db.SmallInteger, default=Role.USER.value)
    status = db.Column(db.SmallInteger, default=User_Status.NEW.value)
