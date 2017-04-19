#!/usr/bin/env python3.5

import argparse
import logging
import os
import sys
from math import fabs

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from i2b2_flattening import i2b2_connection
from pandas import DataFrame

######################################################################################################################
# CONSTANTS
######################################################################################################################

SUBJECT_CODE_COLUMN = 'subject_code'
DEFAULT_VOLUMES_LIST = './data/default_volumes_list'
DEFAULT_SCORES_LIST = './data/default_scores_list'
VOLUMES_POSTFIX = "_volume(cm3)"

AGE_COL_NAME = "age"
SEX_COL_NAME = "sex"
DIAG_COL_NAME = "diag_category"

DIAG_CD = "diag_category"
MRI_TEST_CONCEPT = "protocol_name"

DIAG_CAT_TIMEFRAME_YEAR = 10
SCORES_TIMEFRAME_YEAR = 2


######################################################################################################################
# FUNCTIONS
######################################################################################################################

def get_baseline_visit_with_features(i2b2_conn, subject, mri_test_concept):
    patient_num = i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_num). \
        filter_by(patient_ide=subject).first()
    visits = [int(v[0]) for v in
              i2b2_conn.db_session.query(i2b2_conn.ObservationFact.encounter_num).filter_by(
                  patient_num=patient_num, concept_cd=mri_test_concept).distinct().all()]
    visit_bl = None
    age_bl = None
    for visit in visits:
        age = i2b2_conn.db_session.query(i2b2_conn.VisitDimension.patient_age).\
            filter_by(encounter_num=visit).one_or_none()
        if not age_bl or age < age_bl:
            age_bl = age
            visit_bl = visit
    return visit_bl


def get_sex(i2b2_conn, subject):
    try:
        patient_num = i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_num).\
            filter_by(patient_ide=subject).first()
        return str(i2b2_conn.db_session.query(i2b2_conn.PatientDimension.sex_cd).
                   filter_by(patient_num=patient_num).one_or_none()[0])
    except TypeError:
        return None


def get_volume_at_baseline(i2b2_conn, concept_cd, encounter_num):
    try:
        return float(i2b2_conn.db_session.query(i2b2_conn.ObservationFact.nval_num).
                     filter_by(concept_cd=concept_cd, encounter_num=encounter_num).one_or_none()[0])
    except TypeError:
        return None


def get_age(i2b2_conn, encounter_num):
    try:
        return float(i2b2_conn.db_session.query(i2b2_conn.VisitDimension.patient_age).
                     filter_by(encounter_num=encounter_num).one_or_none()[0])
    except TypeError:
        return None


def get_diag(i2b2_conn, dataset_prefix, subject, mri_age, time_frame=None):
    if not mri_age:
        return None
    concept_cd = dataset_prefix + DIAG_CD
    patient_num = int(i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_num).
                      filter_by(patient_ide=subject).first()[0])
    tuples = i2b2_conn.db_session.query(i2b2_conn.ObservationFact.tval_char, i2b2_conn.ObservationFact.encounter_num).\
        filter_by(patient_num=patient_num, concept_cd=concept_cd).all()
    value = None
    delta = None
    for t in tuples:
        encounter_num = int(t[1])
        age = float(i2b2_conn.db_session.query(i2b2_conn.VisitDimension.patient_age).
                    filter_by(encounter_num=encounter_num).one_or_none()[0])
        if age and (not delta or fabs(mri_age - age) < delta):
            new_delta = fabs(mri_age - age)
            if new_delta < time_frame / 2:
                delta = new_delta
                value = str(t[0])
    return value


def get_score(i2b2_conn, concept_cd, dataset_prefix, subject, mri_age, time_frame):
    if not mri_age:
        return None

    patient_num = int(i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_num).
                      filter_by(patient_ide=subject).first()[0])
    tuples = i2b2_conn.db_session.query(i2b2_conn.ObservationFact.nval_num, i2b2_conn.ObservationFact.encounter_num).\
        filter_by(patient_num=patient_num, concept_cd=concept_cd).all()
    value = None
    delta = None
    for t in tuples:
        encounter_num = int(t[1])
        age = float(i2b2_conn.db_session.query(i2b2_conn.VisitDimension.patient_age).
                    filter_by(encounter_num=encounter_num).one_or_none()[0])
        if age and (not delta or fabs(mri_age - age) < delta):
            new_delta = fabs(mri_age - age)
            if new_delta < time_frame / 2:
                delta = new_delta
                value = float(t[0])
    return value


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
    extra_vars.append(SUBJECT_CODE_COLUMN)
    extra_vars.append(AGE_COL_NAME)
    extra_vars.append(SEX_COL_NAME)
    extra_vars.append(DIAG_COL_NAME)
    headers.extend(extra_vars)

    logging.info("Generating volumes columns (one by brain region)...")
    concept_dict = dict()
    for vol in volumes_list:
        concept_cd = dataset_prefix + vol.lower().replace(' ', '_') + VOLUMES_POSTFIX
        try:
            concept_name = i2b2_conn.db_session.query(i2b2_conn.ConceptDimension.name_char).filter_by(
                concept_cd=concept_cd).first()[0]
        except TypeError:
            logging.warning("Cannot find name_char for %s" % concept_cd)
            concept_name = concept_cd
        headers.append(concept_name)
        concept_dict[concept_name] = concept_cd
    df = DataFrame(columns=headers)

    logging.info("Generating scores columns (one by score)...")
    for score in scores_list:
        concept_cd = dataset_prefix + score
        try:
            concept_name = i2b2_conn.db_session.query(i2b2_conn.ConceptDimension.name_char).filter_by(
                concept_cd=concept_cd).first()[0]
        except TypeError:
            logging.warning("Cannot find name_char for %s" % concept_cd)
            concept_name = concept_cd
        headers.append(concept_name)
        concept_dict[concept_name] = concept_cd

    logging.info("Generating rows (one by subject)...")
    subjects = [subj[0] for subj in i2b2_conn.db_session.query(i2b2_conn.PatientMapping.patient_ide)]
    df[SUBJECT_CODE_COLUMN] = subjects

    logging.info("Filling table with data...")
    mri_test_concept = dataset_prefix + MRI_TEST_CONCEPT
    for index, row in df.iterrows():
        subject = row[SUBJECT_CODE_COLUMN]
        logging.info("Filling row for %s" % subject)

        logging.info("-> extra-vars")
        encounter_num = get_baseline_visit_with_features(i2b2_conn, subject, mri_test_concept)
        mri_age = get_age(i2b2_conn, encounter_num)
        df.loc[index, SEX_COL_NAME] = get_sex(i2b2_conn, subject)
        df.loc[index, AGE_COL_NAME] = mri_age
        df.loc[index, DIAG_COL_NAME] = get_diag(i2b2_conn, dataset_prefix, subject, mri_age, DIAG_CAT_TIMEFRAME_YEAR)

        logging.info("-> volumes")
        start_column = len(extra_vars)
        end_column = len(extra_vars) + len(volumes_list)
        for h in headers[start_column:end_column]:
            df.loc[index, h] = get_volume_at_baseline(i2b2_conn, concept_dict[h], encounter_num)

        logging.info("-> scores")
        start_column = end_column
        for h in headers[start_column:]:
            df.loc[index, h] = get_score(
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
    args_parser.add_argument("output_folder")
    args_parser.add_argument("i2b2_url")
    args_parser.add_argument("--dataset_prefix")
    args_parser.add_argument("--output_file", default='flattening.csv')
    args_parser.add_argument("--volumes_list")
    args_parser.add_argument("--scores_list")
    args = args_parser.parse_args()

    output_path = os.path.join(args.output_folder, args.output_file)
    main(args.i2b2_url, output_path,
         dataset_prefix=args.dataset_prefix, volumes_list_path=args.volumes_list, scores_list_path=args.scores_list)
