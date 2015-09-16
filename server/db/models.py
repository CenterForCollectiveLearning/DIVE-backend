from sqlalchemy import Column, ForeignKey, Integer, String, PickleType, Boolean, Enum, SmallInteger
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from constants import Role, User_Status


class Project(Base):
    __tablename__ = 'project'
    id = Column(Integer, primary_key=True)
    title = Column(String(250))
    creationDate = Column(Date, index=True)
    updateDate = Column(Date, index=True)


# Distinguish from Dataset_Properties?
class Dataset(Base):
    __tablename__ = 'dataset'
    id = Column(Integer, primary_key=True)

    project_id = Column(Integer, ForeignKey('project.id'))
    project = relationship(Project)


class Dataset_Properties(Base):
    __tablename__ = 'dataset_properties'
    id = Column(Integer, primary_key=True)
    title = Column(String(250))  # convert_unicode?
    path = Column(String(250))
    file_name = Column(String(250))
    file_type = Column(String(250))
    rows = Column(String(250))
    cols = Column(String(250))
    field_names = JSONB
    field_types = JSONB
    field_accessors = JSONB
    is_time_series = Boolean()
    structure = Enum(['wide', 'long'])

    fields_properties = relationship(Field_Properties)  # Get all field properties

    dataset_id = Column(Integer, ForeignKey('dataset.id'))
    dataset = relationship(Dataset)

    project_id = Column(Integer, ForeignKey('project.id'))
    project = relationship(Project)


class Field_Properties(Base):
    __tablename__ = 'field_properties'
    id = Column(Integer, primary_key=True)
    name = Column(String(250))  # Have these here, vs. in dataset_properties?
    type = Column(String(250))  #
    index = Column(Integer)
    normality = JSONB
    is_unique = Boolean()
    child = Column(String(250))
    is_child = Boolean()
    unique_values = ARRAY
    stats = JSONB

    dataset_id = Column(Integer, ForeignKey('dataset.id'))
    dataset = relationship(Dataset)

    project_id = Column(Integer, ForeignKey('project.id'))
    project = relationship(Project)


class Specification(Base):
    __tablename__ = 'specification'
    id = Column(Integer, primary_key=True)
    generating_prodecure = Column(String(250))
    type_structure = Column(String(250))
    viz_type = Column(String(250))  # TODO Enum?
    args = JSONB
    meta = JSONB
    score = JSONB
    stats = JSONB

    project_id = Column(Integer, ForeignKey('project.id'))
    project = relationship(Project)


class Exported_Specification(Base):
    __tablename__ = 'exported_specification'
    id = Column(Integer, primary_key=True)
    conditionals = JSONB
    config = JSONB

    specification_id = Column(Integer, ForeignKey('specification.id'))
    specification = relationship(Specification)

    project_id = Column(Integer, ForeignKey('project.id'))
    project = relationship(Project)


class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True)
    email = Column(String(120), unique=True)
    password = Column(String(120))
    role = Column(SmallInteger, default=Role.USER.value)
    status = Column(SmallInteger, default=User_Status.NEW.value)
