# I2B2 to Flat CSV

## Introduction

This is a tool to generate a flat CSV file containing brain features read from an I2B2 DB.

## Usage

main.py [-h] [--dataset_prefix DATASET_PREFIX] i2b2_url output_file

where:
  * 'i2b2_url' is the URL of the I2B2 database
  * 'output_file' is the path to the CSV output file
  * '--dataset_prefix' (optional) is a dataset prefix used on the concept_cd

Example: `./main.py postgresql://postgres:postgres@localhost:5432/postgres /home/mirco/Bureau/test.csv --dataset_prefix='clm:'`
