# Mastering the Entire Data Stack with Databricks

## Welcome to the Repository of the Project!

This repository serves as a comprehensive guide to implementing advanced data engineering and machine learning techniques using Databricks, with a primary focus on the Brazilian e-commerce data from Olist. Explore various concepts, from building a robust data lake architecture to experimenting with MLflow for model management and deployment.

## Summary of Contents:

1. **Data Lake Architecture:**
   - Explore the intricacies of building a data lake architecture tailored to handle large-scale data processing tasks efficiently.

2. **How to Create the Ingestion in Bronze and Silver:**
   - Dive into detailed guides on setting up ingestion pipelines in both the Bronze and Silver layers of the data lake, ensuring seamless data processing from raw to refined stages.

3. **Central Feature Store Architecture:**
   - Understand the architecture and implementation details of a central feature store, a critical component for organizing and accessing model features efficiently.

4. **How to Create New Features:**
   - Learn the step-by-step process of creating new features, including SQL query creation, YAML rule definitions for feature validation, and notebook parameter updates.

5. **Improvements:**
   - Explore potential enhancements and optimizations for the existing architecture and workflows, along with ongoing development updates.

This repository is a continuous work in progress, aimed at providing comprehensive insights and practical guidance for leveraging Databricks and Olist dataset effectively in real-world scenarios. Stay tuned for more updates and contributions!



### Data Lake Architecture

![Data Lake Schema](https://github.com/jooaobrum/databricks-ml-lakehouse/blob/main/img/data-lake-schema.png)

- Landing Zone (Raw Data Storage): This is the initial layer where raw data from various sources, in this case, an e-commerce dataset from Kaggle, is stored in blob storage. The data in this layer is in its original format and is not transformed.
- Bronze Layer: In this layer, data is ingested from the landing zone using batch processing, specifically in delta format. The purpose of this layer is to replicate the data from the landing zone, facilitating querying and avoiding the need to interact directly with the original CSV tables.
- Silver Layer: Data from the bronze layer is ingested into the silver layer also using batch processing. Here, the data is normalized, and columns are renamed according to a predefined pattern. Additionally, relevant business columns are created. However, since the dataset is already well-organized, not many transformations are needed in this case.
- Gold Layer: This layer is intended for aggregated tables and requires modeling for use. However, in this implementation, the ingestion into the gold layer is skipped as it requires further modeling and aggregation steps.

### How to Create the Ingestion in Bronze and Silver

#### Bronze
1. **Upload File to Landing Zone:**
   - Place the new file in the landing zone directory (`/mnt/landing_zone/olist/`). Ensure that the file is in CSV format and contains data relevant to your Olist dataset.

2. **Create the Schema:**
   - Create the schema in `ingestion/bronze/olist/schemas` containing the types and columns of the raw file.

3. **Update Notebook Parameters:**
   - Before running the notebook, ensure that you update the notebook parameters:
     - **Task Key:** Set the `task_key` parameter to match the key associated with the new file. This key should be unique and match the name of the raw table.

4. **Execute or Schedule the Notebook:**
   - **Read Schema:** The notebook reads the schema for the new file from the corresponding JSON file located in the `schemas/` directory.
   - **Read Raw File:** It reads the new file from the landing zone directory using the specified file format, schema, and reading options.
   - **Transform Ingested Data:** The notebook transforms the ingested data by adding metadata such as the ingestor file, task key, and timestamp.
   - **Save as Delta Table in Bronze Schema:** Finally, it saves the transformed data as a Delta table in the Bronze schema. If the table already exists, it performs an overwrite operation to ensure that the table is up to date with the latest data.




#### Silver
1. **Create the SQL Transformation File:**
   - Create the SQL transformation file in `ingestion/silver/olist/silver_transformation` that will store all the transformations related to the business.

2. **Update Notebook Parameters:**
   - **Task Key:** Set the `task_key` parameter to match the key associated with the data you want to ingest into the Silver layer. This key should correspond to the table name in the Bronze layer.

3. **Execute the Notebook:**
   - **Reading Bronze Table:** The notebook reads the data from the specified table in the Bronze layer (`bronze.{ref_name}_{task_key}`).
   - **Normalization & Transformation:** It applies normalization and transformation operations on the data using a SQL query defined in a separate file located at       `silver_transformation/{task_key}.sql`.
   - **Saving as Delta Table in Silver Schema:** The transformed data is saved as a Delta table in the Silver schema (`silver.{ref_name}_{task_key}`). If the table already exists, it performs an overwrite operation to ensure that the table is up to date with the latest data.




### Central Feature Store Architecture

![Data Lake Schema](https://github.com/jooaobrum/databricks-ml-lakehouse/blob/main/img/feature-store-schema.png)


- **Feature Generator:** This component is responsible for processing parameterized SQL queries to calculate base and aggregated features over a defined period. It extracts relevant data points (features) from the silver database and prepares them for model training.

- **Aggregator (optional):** The Aggregator component operates after the Feature Generator and utilizes temporal windows to perform complex calculations on the generated features. It aggregates historical features and prepares them for storage.

- **Storage in Databricks:** The resulting features are efficiently stored in Databricks, leveraging the platform's native API for versioning and data organization. Databricks provides a reliable storage solution for features, ensuring accessibility and scalability.

- **Feature Quality Assurance:** The architecture includes a feature quality assurance step that incorporates tools like Pandas Profiling and Great Expectations. These tools ensure data quality and consistency by generating detailed statistics and applying predefined rules to validate each batch of data.

- **Orchestration with Databricks Workflow:** The complete job execution is orchestrated by Databricks Workflow, providing flexibility to define parameters, batch start and end dates, time intervals between features, tasks to be executed, and window size vectors for


### How to Create New Features

1. Create a new parametrized SQL query to calculate the base and aggregated features (optional) in `ingestions/feature_store/olist/silver_transformation` directory.
2. Create a new YAML file that contains the rules to validate the new features in the `validators/expectations_yml` directory.
3. Update the parameters of the ingestion notebook:
   - **Task Key:** Set the `task_key` parameter to match the key associated with the feature store.
   - **Start Date (`dt_start`):** Set the start date from which to generate feature store data.
   - **Stop Date (`dt_stop`):** Set the stop date until which to generate feature store data.
   - **Step:** Specify the step size for generating dates (e.g., daily, weekly).
   - **Window Size:** Specify the window size for aggregating features.
   - **Primary Keys:** Specify the primary keys of the feature store.
   - **Description:** Specify the description of the feature store.
4. Update the parameters of the validation notebook:
   - **Start Date (`dt_start`):** Set the start date from which to generate feature store data.
   - **Stop Date (`dt_stop`):** Set the stop date until which to generate feature store data.
   - **Task Key:** Set the `task_key` parameter to match the key associated with the feature store.
5. Schedule/deploy the notebook using the dependency ingestion -> validation.



### Improvements

This project is just a way to study all Databricks features. So, there are some points of improvements:

- Use configuration files to store all parameters from the notebook; it should help to version all the parameters in code.
- Use code to deploy all workflows and version them in code.
- Implement a CI structure to test codes before merging them into the main branch.


STILL UNDER CONSTRUCTION
