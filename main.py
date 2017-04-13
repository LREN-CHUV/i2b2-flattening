#!/usr/bin/env python3.5

import logging
import argparse
from pandas import DataFrame

import i2b2_connection

SUBJECT_CODE_COLUMN = 'subject_code'
DEFAULT_VOLUMES_LIST = './data/default_volumes_list'
VOLUMES_POSTFIX = "_volume(cm3)"


def get_baseline(query_results):
    try:
        # TODO: compare exam dates and/or patient age at visit time to return the baseline value
        return query_results[0]
    except IndexError:
        return None


def get_volume(i2b2_conn, concept_cd, subject):
    patient_num = i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_num).filter_by(
        patient_ide=subject).first()
    all_values = i2b2_conn.db_session.query(
        i2b2_conn.ObservationFact.nval_num, i2b2_conn.ObservationFact.encounter_num).\
        filter_by(concept_cd=concept_cd, patient_num=patient_num).all()
    value = get_baseline(all_values)
    try:
        return float(value[0])
    except (IndexError, TypeError):
        return None


def main(i2b2_url, output_file, dataset_prefix='', volumes_list_path=None):

    logging.info("START")

    dataset_prefix = dataset_prefix + ":" if dataset_prefix[-1] != ":" else dataset_prefix
    volumes_list_path = volumes_list_path if volumes_list_path else DEFAULT_VOLUMES_LIST

    logging.info("Reading volumes list from %s..." % volumes_list_path)
    with open(volumes_list_path, 'r') as volumes_list_file:
        volumes_list = [v.rstrip() for v in volumes_list_file.readlines()]

    logging.info("Connecting to I2B2 database...")
    i2b2_conn = i2b2_connection.Connection(i2b2_url)

    headers = list()
    headers.append(SUBJECT_CODE_COLUMN)

    logging.info("Generating columns (one by brain region)...")
    concept_dict = dict()
    for vol in volumes_list:
        concept_cd = dataset_prefix + vol.lower().replace(' ', '_') + VOLUMES_POSTFIX
        concept_name = i2b2_conn.db_session.query(i2b2_conn.ConceptDimension.name_char).filter_by(
            concept_cd=concept_cd).first()[0]
        headers.append(concept_name)
        concept_dict[concept_name] = concept_cd
    df = DataFrame(columns=headers)

    logging.info("Generating rows (one by subject)...")
    subjects = [subj[0] for subj in i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_ide)]
    df[SUBJECT_CODE_COLUMN] = subjects

    logging.info("Filling table with data...")
    for index, row in df.iterrows():
        subject = row[SUBJECT_CODE_COLUMN]
        logging.info("Filling row for %s" % subject)
        for h in headers[1:]:
            df.loc[index, h] = get_volume(i2b2_conn, concept_dict[h], subject)

    i2b2_conn.close()
    logging.info("I2B2 database connection closed")

    logging.info("Exporting data to %s CSV file..." % output_file)
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
