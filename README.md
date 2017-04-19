[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/LREN-CHUV/i2b2-flattening/blob/master/LICENSE)
[![CHUV](https://img.shields.io/badge/CHUV-LREN-AF4C64.svg)](https://www.unil.ch/lren/en/home.html)

# I2B2 to Flat CSV

## Introduction

This is a tool to generate a flat CSV file containing brain features read from an I2B2 DB.

## Usage

Run: `docker run --rm -v <output_folder>:/output_folder hbpmip/i2b2-flattening <db_url>
--dataset_prefix <dataset_prefix>
--output_file <output_file>
--volumes_list <volumes_list>
--scores_list <scores_list>`
