# End-to-End E-commerce pipeline

Build a data pipeline that take normalized production tables and outputs a curated table suitable for business consumption available.
This project mimic a real world scenario where the pipeline runs daily and process a batch of data from operational sources, process it and then loads the data into a Data Warehouse. 

The following skills are demonstrated in this project:
- Python Pandas
- Python OOP design inspired by [DMOT](https://towardsdatascience.com/dmot-a-design-pattern-for-etl-data-model-orchestrator-transformer-c0d7baacb8c7)
- Airflow: Custom Operators, build task factory for scalable scheduling, complex workflows such as Branching and inter-dags dependencies. 
- SQL DDL and DML to build set up the DWH
- Software development best practices: 
    - CI/CD using Github Actions and Git for version history
    - Docker and Docker compose for environment reproducibility.


![Original scheme](static/olist-scheme.png)

![Curated Dag](statitc/curated-orders-dag.png)


## Data
- Input data is Olist E-commerce data available in Kaggle [link](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv)


## How to run

Assuming docker and docker-compose is installed:
- Clone the repo
- Create a folder called "data" and within it, another one called "kaggle". Pase the 
- Download the csv data from the link above
- set the dir to airflow-env and spin up the containers: docker-compose up -d. 
- Trigger the prep dag first to make some prep work required to mimic the batch pipeline. 
- Activate the curated and datamart dags. The scheduler will trigger the dags by itself!

