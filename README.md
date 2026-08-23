# STEDI Human Balance Analytics

This project builds a data lakehouse solution for STEDI Human Balance Analytics using AWS Glue, Amazon S3, Athena, and PySpark.

## Landing Zone

- customer_landing: 956 rows
- accelerometer_landing: 81,273 rows
- step_trainer_landing: 28,680 rows

## Trusted Zone

- customer_trusted: 482 rows
- accelerometer_trusted: 40,981 rows
- step_trainer_trusted: 14,460 rows

## Curated Zone

- customers_curated: 482 rows
- machine_learning_curated: 43,681 rows

## Project Files

The repository contains:
- SQL DDL scripts for the landing-zone tables
- AWS Glue/PySpark ETL scripts
- Athena query-result screenshots demonstrating the required row counts and consent filtering

## Screenshots

Athena screenshots are stored in the `screenshots` folder.
