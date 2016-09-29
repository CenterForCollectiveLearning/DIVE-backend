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

from dive.base.core import s3_bucket
from dive.base.db import db_access
from dive.worker.core import celery, task_app
from dive.base.data.access import get_data
from dive.base.data.in_memory_data import InMemoryData as IMD

import boto3
# import boto.s3
# from boto.s3.cors import CORSConfiguration
# from boto.exception import S3ResponseError

import logging
logger = logging.getLogger(__name__)


def save_fileobj(fileobj, project_id, file_name):
    s3 = boto3.client('s3',
        aws_access_key_id=current_app.config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=current_app.config['AWS_SECRET_ACCESS_KEY'],
        region_name=current_app.config['AWS_REGION']
    )
    try:
        url = s3.generate_presigned_url(
            'get_object',
            Params = {
                'Bucket': current_app.config['AWS_DATA_BUCKET'],
                'Key': file_name
            },
            ExpiresIn = 86400
        )
        s3.upload_fileobj(
            fileobj,
            current_app.config['AWS_DATA_BUCKET'],
            "%s/%s" % (project_id, file_name)
        )
    except Exception as e:
        logger.error(e, exc_info=True)
    return url


def upload_file(project_id, file_obj):
    '''
    1. Save file in uploads/project_id directory
    2. If excel or json, also save CSV versions
    3. If all steps are successful, save file location in project data collection

    file_name = foo.csv
    file_title = foo
    '''
    file_name = secure_filename(file_obj.filename)
    file_title, file_type = file_name.rsplit('.', 1)

    dialect = get_dialect(file_obj)
    file_obj.read(0)
    url = save_fileobj(file_obj, project_id, file_name)

    if current_app.config['STORAGE_TYPE'] == 'file':
        path = os.path.join(current_app.config['STORAGE_PATH'], project_id, file_name)
    elif current_app.config['STORAGE_TYPE'] == 's3':
        path = 'https://s3.amazonaws.com/%s/%s/%s' % (current_app.config['AWS_DATA_BUCKET'], project_id, file_name)

    # print df
    # file_obj.close()
    # s3_conn = boto.connect_s3(
    #     current_app.config['AWS_ACCESS_KEY'],
    #     current_app.config['AWS_SECRET_KEY'],
    #     host=current_app.config['AWS_HOST']
    # )
    # s3 = boto3.resource('s3')
    # for bucket in s3.buckets.all():
    #     print(bucket.name)

    # bucket = s3_conn.get_bucket(current_app.config['AWS_DATA_BUCKET'])
    # # CREATE PROJECT ID DIR IF NEEDED
    # print bucket
    # # generate_s3_upload_policy(bucket)
    #
    # # TODO Create file_type enum
    # file_title, file_type = file_name.rsplit('.', 1)
    # key = boto.s3.key.Key(bucket, file_name)
    # key.send_file(
    #     file,
    #     chunked_transfer=False
    # )
    # print 'Sent file'
    # path = os.path.join(current_app.config['STORAGE_PATH'], project_id, file_name)
    #
    # # Ensure project directory exists
    # project_dir = os.path.join(current_app.config['STORAGE_PATH'], project_id)
    # if not os.path.isdir(project_dir):
    #     os.mkdir(os.path.join(project_dir))
    #
    # if file_type in ['csv', 'tsv', 'txt', 'json'] or file_type.startswith('xls'):
    #     try:
    #         file.save(path)
    #     except IOError:
    #         logger.error('Error saving file with path %s', path, exc_info=True)
    #

    datasets = save_dataset_to_db(
        project_id,
        file_obj,
        dialect,
        file_title,
        file_name,
        file_type,
        path,
        current_app.config['STORAGE_TYPE']
    )
    file_obj.close()
    return datasets


def get_dialect(file_obj, sample_size=1024*1024):
    '''
    TODO Use file extension as an indication?
    TODO list of delimiters
    '''
    DELIMITERS = ''.join([',', ';', '|', '$', ';', ' ', ' | ', '\t'])

    try:
        sample = file_obj.read(sample_size)
    except StopIteration:
        sample = file_obj.readline()
    file_obj.seek(0)

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


def save_dataset_to_db(project_id, file_obj, dialect, file_title, file_name, file_type, path, storage_type):
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
        # dialect = get_dialect(file_obj)

        with current_app.app_context():
            dataset = db_access.insert_dataset(project_id,
                path = path,
                dialect = dialect,
                offset = None,
                title = file_doc['file_title'],
                file_name = file_doc['file_name'],
                type = file_doc['type'],
                storage_type = storage_type
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
        csv_path = os.path.join(current_app.config['STORAGE_PATH'], str(project_id), csv_file_name)

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
    csv_path = os.path.join(current_app.config['STORAGE_PATH'], project_id, csv_file_name)

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
