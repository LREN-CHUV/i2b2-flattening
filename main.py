#!/usr/bin/env python3.5

import logging
import argparse
from pandas import DataFrame

import i2b2_connection


def get_volume(i2b2_conn, region, subject, dataset_prefix=''):
    region = dataset_prefix + region
    patient_num = i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_num).filter_by(
        patient_ide=subject).first()
    return float(i2b2_conn.db_session.query(i2b2_conn.ObservationFact.nval_num).filter_by(
        concept_cd=region, patient_num=patient_num).first()[0])


def main(i2b2_url, output_file, dataset_prefix=''):

    logging.info("START")

    i2b2_conn = i2b2_connection.Connection(i2b2_url)

    headers = list()
    headers.append('subject_code')

    for results in i2b2_conn.db_session.query(i2b2_conn.ConceptDimension.concept_cd):
        feature = results[0][4:]
        if feature[-5:] == '(cm3)':
            headers.append(feature)

    df = DataFrame(columns=headers)

    subjects = list()
    for results in i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_ide):
        subjects.append(results[0])

    df['subject_code'] = subjects

    for index, row in df.iterrows():
        for h in headers[1:]:
            df.loc[index, h] = get_volume(i2b2_conn, h, row['subject_code'], dataset_prefix)

    i2b2_conn.close()

    df.to_csv(output_file, index=False)

    logging.info("DONE")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("i2b2_url")
    args_parser.add_argument("output_file")
    args_parser.add_argument("--dataset_prefix")
    args = args_parser.parse_args()
    main(args.i2b2_url, args.output_file, args.dataset_prefix)
