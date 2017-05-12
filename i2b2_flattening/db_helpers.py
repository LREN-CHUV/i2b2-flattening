import logging
from math import fabs


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
            filter_by(encounter_num=visit).one_or_none()[0]
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


def get_diag(i2b2_conn, dataset_prefix, subject, mri_age, time_frame=None, diag_cd=''):
    if not mri_age:
        return None
    concept_cd = dataset_prefix + diag_cd
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


def get_concept_name(i2b2_conn, concept_cd):
    try:
        return i2b2_conn.db_session.query(i2b2_conn.ConceptDimension.name_char).filter_by(
            concept_cd=concept_cd).first()[0]
    except TypeError:
        logging.warning("Cannot find name_char for %s" % concept_cd)
        return concept_cd
