[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/LREN-CHUV/i2b2-flattening/blob/master/LICENSE)
[![CHUV](https://img.shields.io/badge/CHUV-LREN-AF4C64.svg)](https://www.unil.ch/lren/en/home.html)

# I2B2 to Flat CSV

## Introduction

This is a tool to generate a flat CSV file containing brain features read from an I2B2 DB.

## Usage

main.py [-h] [--dataset_prefix DATASET_PREFIX] i2b2_url output_file

where:
  * 'i2b2_url' is the URL of the I2B2 database
  * 'output_file' is the path to the CSV output file
  * '--dataset_prefix' (optional) is a dataset prefix used on the concept_cd
  * '--volumes_list' (optional) is a file containing the list of brain regions volumes to extract

Example: `./main.py postgresql://postgres:postgres@localhost:5432/postgres /home/mirco/Bureau/test.csv --dataset_prefix='clm:'`
