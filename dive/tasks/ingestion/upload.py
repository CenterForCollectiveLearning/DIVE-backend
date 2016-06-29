'''
Functions for reading, sampling, and detecting types of datasets

No manipulation or calculation, only description
'''

import os
import re
import csv
import xlrd
import json
import codecs
import pandas as pd
from werkzeug.utils import secure_filename
from flask import current_app

from messytables.error import ReadError
from messytables import any_tableset, headers_guess

import logging
logger = logging.getLogger(__name__)

from dive.db import db_access
from dive.task_core import celery, task_app
from dive.data.access import get_data
from dive.data.in_memory_data import InMemoryData as IMD


def upload_file(project_id, file):
    '''
    1. Save file in uploads/project_id directory
    2. If excel or json, also save CSV versions
    3. If all steps are successful, save file location in project data collection

    file_name = foo.csv
    file_title = foo
    '''
    file_name = secure_filename(file.filename)

    # TODO Create file_type enum
    file_title, file_type = file_name.rsplit('.', 1)
    path = os.path.join(current_app.config['UPLOAD_DIR'], project_id, file_name)

    # Ensure project directory exists
    project_dir = os.path.join(current_app.config['UPLOAD_DIR'], project_id)
    if not os.path.isdir(project_dir):
        os.mkdir(os.path.join(project_dir))

    if file_type in ['csv', 'tsv', 'txt', 'json'] or file_type.startswith('xls'):
        try:
            file.save(path)
        except IOError:
            logger.error('Error saving file with path %s', path, exc_info=True)

    datasets = save_dataset(project_id, file_title, file_name, file_type, path)
    return datasets


def get_offset_and_headers(f):
    f.seek(0)
    table_set = any_tableset(f)
    row_set = table_set.tables[0]

    offset, headers = headers_guess(row_set.sample)
    return offset, headers


def get_dialect(f):
    '''
    TODO Use file extension as an indication?
    TODO list of delimiters
    '''
    DELIMITERS = ''.join([',', ';', '|', '$', ';', ' ', ' | ', '\t'])
    f.seek(0)
    sample = f.readline()

    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(sample)

    result = {
        'delimiter': dialect.delimiter,
        'doublequote': dialect.doublequote,
        'escapechar': dialect.escapechar,
        'lineterminator': dialect.lineterminator,
        'quotechar': dialect.quotechar,
    }
    return result


def save_dataset(project_id, file_title, file_name, file_type, path):
    file_docs = []
    if file_type in ['csv', 'tsv', 'txt'] :
        file_doc = {
            'file_title': file_title,
            'file_name': file_name,
            'type': file_type,
            'path': path
        }
        file_docs.append(file_doc)

    elif file_type.startswith('xls'):
        file_docs = save_excel_to_csv(project_id, file_title, file_name, path)

    elif file_type == 'json':
        file_doc = save_json_to_csv(project_id, file_title, file_name, path)
        file_docs.append(file_doc)

    datasets = []
    for file_doc in file_docs:
        path = file_doc['path']

        # Get pre-read dataset properties (all data needed to correctly read)
        # Insert into database
        with open(path, 'rb') as f:
            dialect = get_dialect(f)
            try:
                offset, headers = get_offset_and_headers(f)
            except ReadError:
                logger.error('Error getting offset for %s', file_name, exc_info=True)
                offset = 0
                continue

        with task_app.app_context():
            dataset = db_access.insert_dataset(project_id,
                path = path,
                dialect = dialect,
                offset = offset,
                title = file_doc['file_title'],
                file_name = file_doc['file_name'],
                type = file_doc['type']
            )
            datasets.append(dataset)

    return datasets


def save_excel_to_csv(project_id, file_title, file_name, path):
    book = xlrd.open_workbook(path)
    sheet_names = book.sheet_names()

    file_docs = []
    for sheet_name in sheet_names:
        sheet = book.sheet_by_name(sheet_name)

        if sheet.nrows == 0: continue

        csv_file_title = file_name + "_" + sheet_name
        csv_file_name = csv_file_title + ".csv"
        csv_path = os.path.join(current_app.config['UPLOAD_DIR'], str(project_id), csv_file_name)

        csv_file = open(csv_path, 'wb')
        wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        for rn in xrange(sheet.nrows) :
            wr.writerow([ unicode(v).encode('utf-8') for v in sheet.row_values(rn) ])
        csv_file.close()
        file_doc = {
            'file_title': csv_file_title,
            'file_name': csv_file_name,
            'path': csv_path,
            'type': 'csv',
            'orig_type': 'xls'
        }
        file_docs.append(file_doc)
    return file_docs


def save_json_to_csv(project_id, file_title, file_name, path):
    f = open(path, 'rU')
    json_data = json.load(f)

    orig_type = file_name.rsplit('.', 1)[1]
    csv_file_title = file_title
    csv_file_name = csv_file_title + ".csv"
    csv_path = os.path.join(current_app.config['UPLOAD_DIR'], project_id, csv_file_name)

    csv_file = open(csv_path, 'wb')
    wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)

    header = json_data[0].keys()

    wr.writerow([v.encode('utf-8') for v in header])
    for i in range(len(json_data)) :
        row = []
        for field in header :
            row.append(json_data[i][field])
        wr.writerow([unicode(v).encode('utf-8') for v in row])
    csv_file.close()
    file_doc = {
        'title': csv_file_title,
        'file_name': csv_file_name,
        'path': csv_path,
        'type': 'csv',
        'orig_type': 'json'
    }
    return file_doc
