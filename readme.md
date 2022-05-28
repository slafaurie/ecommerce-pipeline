# End-to-End E-commerce pipeline

This project goal is to build a data pipeline that takes production tables and outputs a curated table suitable for business consumption. This project demonstrates skills in Python, OOP, and Airflow. 


## Highlights
- ETL OOP Framework inspired by [DMOT](https://towardsdatascience.com/dmot-a-design-pattern-for-etl-data-model-orchestrator-transformer-c0d7baacb8c7)
- Input data is Olist E-commerce data available in Kaggle [link](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv)
- Airflow as Orchestration service deployed using Docker
- Use of AWS S3 Service to prep data
- Multiple layers (raw, transient, curated) suitable for data governance

## Next Improvements
- Testing, I know. Will add it. 
     DAG integrity (no dag import errors)
- Store curated table into DWH (e.g BigQuery)
- Datamart layer to support reporting. Will be exclusively done in SQL.
- CI (Github Actions)
    - Infra build
    - Container spinning
    - run tests
- Terraform scripts to build the required infra.

## How to run

TBD, but essentially, you'd need to have an AWS account with read access to S3 and Docker to run the containers. 

## Details

TBD