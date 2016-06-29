from enum import Enum
from flask_restful import reqparse

projectIDParser = reqparse.RequestParser()

class ContentType(Enum):
    VISUALIZATION = 'VISUALIZATION'
    CORRELATION = 'CORRELATION'
    REGRESSION = 'REGRESSION'
