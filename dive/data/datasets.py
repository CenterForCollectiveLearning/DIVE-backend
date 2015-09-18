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
from bson.objectid import ObjectId
from flask import current_app

from dive.db import db_access
from dive.data import DataType
from dive.data.access import get_data
from dive.data.in_memory_data import InMemoryData as IMD
from dive.data.dataset_properties import get_dataset_properties, compute_dataset_properties
from dive.data.type_detection import get_column_types, detect_time_series

def get_dataset_sample(dataset_id, project_id, start=0, inc=1000):
    end = start + inc  # Upper bound excluded
    df = get_data(dataset_id=dataset_id, project_id=project_id)
    df = df.fillna('')
    sample = map(list, df.iloc[start:end].values)

    result = get_dataset_properties(dataset_id, project_id)
    result['sample'] = sample
    return result


def upload_file(project_id, file):
    '''
    Dataflow:
    1. Save file in uploads/project_id directory
    2. Compute properties
    3. If all steps are successful, save file location in project data collection
    4. Return sample

    TODO: Separate these different functions?
    '''
    full_file_name = secure_filename(file.filename)
    file_name, file_type = full_file_name.rsplit('.', 1)
    path = os.path.join(current_app.config['UPLOAD_FOLDER'], project_id, full_file_name)

    # Ensure project directory exists
    project_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], project_id)
    if not os.path.isdir(project_dir):
        os.mkdir(os.path.join(project_dir))

    datasets = []

    if file_type in ['csv', 'tsv', 'txt'] or file_type.startswith('xls'):
        try:
            file.save(path)
        except IOError, e:
            print str(e)

    # Flat files
    if file_type in ['csv', 'tsv', 'txt'] :
        dataset = db_access.insert_dataset(project_id, path=path, file_name=full_file_name)
        dataset_id = dataset['id']
        dataset_doc = compute_dataset_properties(dataset_id, project_id, path=path)

        dataset_doc.update({
            'title' : file_name,
            'filename' : full_file_name,
            'dataset_id' : dataset_id,
        })
        datasets.append(dataset_doc)

    # Excel files
    elif file_type.startswith('xls') :
        book = xlrd.open_workbook(path)
        sheet_names = book.sheet_names()

        for sheet_name in sheet_names:
            sheet = book.sheet_by_name(sheet_name)

            # Don't save empty sheets
            if sheet.nrows == 0:
                continue

            csv_file_name = file_name + "_" + sheet_name + ".csv"
            csv_path = os.path.join(config['UPLOAD_FOLDER'], project_id, csv_file_name)

            csv_file = open(csv_path, 'wb')
            wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            for rn in xrange(sheet.nrows) :
                wr.writerow([ unicode(v).encode('utf-8') for v in sheet.row_values(rn) ])
            csv_file.close()

            dataset = db_access.insert_dataset(project_id, path=csv_path, file_name=csv_file_name)
            dataset_id = dataset['id']
            # TODO
            data_doc = compute_dataset_properties(dataset_id, project_id, path=csv_path)
            data_doc.update({
                'title' : csv_file_name.rsplit('.', 1)[0],
                'filename' : csv_file_name,
                'dataset_id' : dataset_id
            })

            datasets.append(data_doc)

    elif file_type == 'json' :
        f = open(path, 'rU')
        json_data = json.load(f)

        csv_path = path + ".csv"
        csv_file_name = file_name + ".csv"

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

        dataset_id = db_access.insert_dataset(project_id, path=csv_path, file_name=csv_file_name)

        result = compute_dataset_properties(dataset_id, project_id, path=json_path)
        result.update({
            'title' : csv_file_name.rsplit('.', 1)[0],
            'filename' : csv_file_name,
            'dataset_id' : dataset_id,
        })
        datasets.append(result)
    return datasets
