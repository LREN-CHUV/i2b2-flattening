#!/usr/bin/env python3.5

import logging
import argparse
from pandas import DataFrame

import i2b2_connection

SUBJECT_CODE_COLUMN = 'subject_code'


def get_volume(i2b2_conn, region, subject, dataset_prefix=''):
    region = dataset_prefix + region
    patient_num = i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_num).filter_by(
        patient_ide=subject).first()
    return float(i2b2_conn.db_session.query(i2b2_conn.ObservationFact.nval_num).filter_by(
        concept_cd=region, patient_num=patient_num).first()[0])


def main(i2b2_url, output_file, dataset_prefix='', volumes_list_path=None):

    logging.info("START")

    volumes_list_path = volumes_list_path if volumes_list_path else './data/default_volumes_list'

    with open(volumes_list_path, 'r') as volumes_list_file:
        volumes_list = [v.rstrip()
                        if len(v.rstrip()) <= 50 - len(dataset_prefix) else v.rstrip()[:47-len(dataset_prefix)] + '...'
                        for v in volumes_list_file.readlines()]

    i2b2_conn = i2b2_connection.Connection(i2b2_url)

    headers = list()
    headers.append(SUBJECT_CODE_COLUMN)

    for results in i2b2_conn.db_session.query(i2b2_conn.ConceptDimension.concept_cd):
        feature = results[0][4:]
        if feature in volumes_list:
            headers.append(feature)

    df = DataFrame(columns=headers)

    subjects = list()
    for results in i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_ide):
        subjects.append(results[0])

    df[SUBJECT_CODE_COLUMN] = subjects

    for index, row in df.iterrows():
        for h in headers[1:]:
            logging.info("processing %s" % row[SUBJECT_CODE_COLUMN])
            df.loc[index, h] = get_volume(i2b2_conn, h, row[SUBJECT_CODE_COLUMN], dataset_prefix)

    i2b2_conn.close()

    df.to_csv(output_file, index=False)

    logging.info("DONE")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("i2b2_url")
    args_parser.add_argument("output_file")
    args_parser.add_argument("--dataset_prefix")
    args_parser.add_argument("--volumes_list")
    args = args_parser.parse_args()
    main(args.i2b2_url, args.output_file, dataset_prefix=args.dataset_prefix, volumes_list_path=args.volumes_list)
