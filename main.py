#!/usr/bin/env python3.5

import logging
import argparse
from pandas import DataFrame

import i2b2_connection


def main(i2b2_url, output_file):

    logging.info("START")

    i2b2_conn = i2b2_connection.Connection(i2b2_url)

    headers = []
    scans = []
    subjects = []

    for results in i2b2_conn.db_session.query(
            i2b2_conn.ConceptDimension.concept_cd):
        headers.append(results[0])

    for results in i2b2_conn.db_session.query(
            i2b2_conn.EncounterMapping.encounter_ide,
            i2b2_conn.EncounterMapping.patient_ide):
        scans.append(results[0])
        subjects.append(results[1])

    df = DataFrame({'subject_code': subjects, 'scan_code': scans})

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
