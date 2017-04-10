#!/usr/bin/env python3.5

import logging
import argparse
from pandas import DataFrame

import i2b2_connection


def main(i2b2_url, output_file):

    logging.info("START")

    i2b2_conn = i2b2_connection.Connection(i2b2_url)

    headers = list()
    headers.append('subject_code')

    for results in i2b2_conn.db_session.query(i2b2_conn.ConceptDimension.concept_cd):
        feature = results[0][4:]
        if feature[-6:] != '_names':
            headers.append(feature)

    df = DataFrame(columns=headers)

    subjects = list()
    for results in i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_ide):
        subjects.append(results[0])

    df['subject_code'] = subjects

    i2b2_conn.close()

    df.to_csv(output_file, index=False)

    logging.info("DONE")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("i2b2_url")
    args_parser.add_argument("output_file")
    args = args_parser.parse_args()
    main(args.i2b2_url, args.output_file)
