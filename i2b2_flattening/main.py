#!/usr/bin/env python3.5

import argparse
import logging
import os
import sys

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from i2b2_flattening import i2b2_connection
from i2b2_flattening import db_helpers
from pandas import DataFrame


######################################################################################################################
# CONSTANTS
######################################################################################################################

SUBJECT_COL_NAME = 'Subject Code'
AGE_YEARS_COL_NAME = "Subject Age Years"
AGE_MONTHS_COL_NAME = "Subject Age Months"
SEX_COL_NAME = "Gender"
DIAG_COL_NAME = "Diagnosis Categories"

DIAGNOSIS_TIMEFRAME_YEAR = 10
SCORES_TIMEFRAME_YEAR = 10

DEFAULT_VOLUMES_LIST = './data/default_volumes_list'
DEFAULT_SCORES_LIST = './data/default_scores_list'

DIAGNOSIS_CONCEPT_CD = "diag_category"
PROTOCOL_CONCEPT_CD = "protocol_name"

VOLUMES_CONCEPT_POSTFIX = "_volume(cm3)"


######################################################################################################################
# FUNCTIONS
######################################################################################################################

def main(i2b2_url, output_file, dataset_prefix='', volumes_list_path=None, scores_list_path=None):

    logging.info("START")

    dataset_prefix = dataset_prefix + ":" if dataset_prefix[-1] != ":" else dataset_prefix
    volumes_list_path = volumes_list_path if volumes_list_path else DEFAULT_VOLUMES_LIST
    scores_list_path = scores_list_path if scores_list_path else DEFAULT_SCORES_LIST

    logging.info("Reading volumes list from %s..." % volumes_list_path)
    with open(volumes_list_path, 'r') as volumes_list_file:
        volumes_list = [v.rstrip() for v in volumes_list_file.readlines()]

    logging.info("Reading scores list from %s..." % scores_list_path)
    with open(scores_list_path, 'r') as scores_list_file:
        scores_list = [s.rstrip() for s in scores_list_file.readlines()]

    logging.info("Connecting to I2B2 database...")
    i2b2_conn = i2b2_connection.Connection(i2b2_url)

    headers = list()

    logging.info("Generating extra_vars columns (demographics, diag, etc)...")
    extra_vars = list()
    extra_vars.append(SUBJECT_COL_NAME)
    extra_vars.append(AGE_YEARS_COL_NAME)
    extra_vars.append(AGE_MONTHS_COL_NAME)
    extra_vars.append(SEX_COL_NAME)
    extra_vars.append(DIAG_COL_NAME)
    headers.extend(extra_vars)

    logging.info("Generating volumes columns (one by brain region)...")
    concept_dict = dict()
    for vol in volumes_list:
        concept_cd = dataset_prefix + vol.lower().replace(' ', '_') + VOLUMES_CONCEPT_POSTFIX
        concept_name = db_helpers.get_concept_name(i2b2_conn, concept_cd)
        headers.append(concept_name)
        concept_dict[concept_name] = concept_cd
    df = DataFrame(columns=headers)

    logging.info("Generating scores columns (one by score)...")
    for score in scores_list:
        concept_cd = dataset_prefix + score
        concept_name = db_helpers.get_concept_name(i2b2_conn, concept_cd)
        headers.append(concept_name)
        concept_dict[concept_name] = concept_cd

    logging.info("Generating rows (one by subject)...")
    subjects = [subj[0] for subj in i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_ide)]
    df[SUBJECT_COL_NAME] = subjects

    logging.info("Filling table with data...")
    mri_test_concept = dataset_prefix + PROTOCOL_CONCEPT_CD
    for index, row in df.iterrows():
        subject = row[SUBJECT_COL_NAME]
        logging.info("Filling row for %s" % subject)

        logging.info("-> extra-vars")
        encounter_num = db_helpers.get_baseline_visit_with_features(i2b2_conn, subject, mri_test_concept)
        mri_age = db_helpers.get_age(i2b2_conn, encounter_num)
        try:
            age_years = int(mri_age)
        except TypeError:
            age_years = None
        try:
            age_months = int(12 * (mri_age - age_years))
        except TypeError:
            age_months = None
        df.loc[index, SEX_COL_NAME] = db_helpers.get_sex(i2b2_conn, subject)
        df.loc[index, AGE_YEARS_COL_NAME] = age_years
        df.loc[index, AGE_MONTHS_COL_NAME] = age_months
        df.loc[index, DIAG_COL_NAME] = db_helpers.get_diag(i2b2_conn, dataset_prefix, subject, mri_age,
                                                           time_frame=DIAGNOSIS_TIMEFRAME_YEAR,
                                                           diag_cd=DIAGNOSIS_CONCEPT_CD)

        logging.info("-> volumes")
        start_column = len(extra_vars)
        end_column = len(extra_vars) + len(volumes_list)
        for h in headers[start_column:end_column]:
            df.loc[index, h] = db_helpers.get_volume_at_baseline(i2b2_conn, concept_dict[h], encounter_num)

        logging.info("-> scores")
        start_column = end_column
        for h in headers[start_column:]:
            df.loc[index, h] = db_helpers.get_score(
                i2b2_conn, concept_dict[h], dataset_prefix, subject, mri_age, SCORES_TIMEFRAME_YEAR)

    i2b2_conn.close()
    logging.info("I2B2 database connection closed")

    logging.info("Exporting data to %s CSV file..." % output_file)
    df.to_csv(output_file, index=False)

    logging.info("DONE")


######################################################################################################################
# ENTRY POINT
######################################################################################################################

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("input_folder")
    args_parser.add_argument("output_folder")
    args_parser.add_argument("i2b2_url")
    args_parser.add_argument("--dataset_prefix")
    args_parser.add_argument("--output_file", default='flattening.csv')
    args_parser.add_argument("--volumes_list_file")
    args_parser.add_argument("--scores_list_file")
    args = args_parser.parse_args()

    if args.volumes_list_file:
        volumes_path = os.path.join(args.input_folder, args.volumes_list_file)
    else:
        volumes_path = DEFAULT_VOLUMES_LIST
    if args.scores_list_file:
        scores_path = os.path.join(args.input_folder, args.scores_list_file)
    else:
        scores_path = DEFAULT_SCORES_LIST
    output_path = os.path.join(args.output_folder, args.output_file)

    main(args.i2b2_url, output_path,
         dataset_prefix=args.dataset_prefix, volumes_list_path=volumes_path, scores_list_path=scores_path)
