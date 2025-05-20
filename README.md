

## BigData_IU_2025: Codebase Documentation

This document provides a detailed overview of the directory structure and file purposes within the **BigData_IU_2025** project. The project implements a complete data processing pipeline from data collection to analysis and visualization, designed to run on a CentOS 7.9.2009 Linux distribution.

### Repository Structure Overview

```
BigData_IU_2025/
├── data/
├── hive/
├── logs/
├── models/
├── output/
│   ├── chart/
│   ├── benchmark/
│   └── src/
├── scripts/
├── sql/
├── secrets/
├── main.sh
├── README.md
└── requirements.txt
```

---

### Folder and File Descriptions

#### 1. `data/` Folder
*   **Purpose**: This directory serves as the primary storage for both raw input data and intermediate data generated during the pipeline's execution.
*   **Contents**:
    *   `US_Accidents_March23.csv`: This is the main raw dataset for the project. It is downloaded from Kaggle as the initial step of the pipeline, orchestrated by the `scripts/01_data_collection.sh` script.
    *   `train_data_reg.json`, `train_data_cls.json`, `test_data_reg.json`, `test_data_cls.json`: These JSON files store preprocessed dataframes. They represent the data after machine learning preprocessing steps have been applied and before the ML models are trained. Saving this data allows for skipping the computationally intensive preprocessing stage if model training needs to be rerun or experimented with, thus speeding up development iterations.

#### 2. `hive/` Folder
*   **Purpose**: Contains Hive Query Language (HQL) scripts used to define and populate external Hive tables. These tables are primarily used for generating business insights from the processed data.
*   **Contents**:
    *   `qN_description.hql`: This folder contains seven HQL files, following the naming convention `qN_description.hql`, where `N` is a number from 1 to 7 representing a specific insight, and `description` provides a brief summary of the insight. For example, `q1_accidents_per_state.hql` would be the script to generate a table showing accident counts per state. These scripts are executed by `scripts/export_insights.sh`.

#### 3. `logs/` Folder
*   **Purpose**: This directory stores log files generated during the execution of various pipeline scripts.
*   **Contents**: Log files primarily capture output from scripts, including standard output, error messages, and specific logs from tools like Beeline (for Hive interactions). These logs are crucial for debugging, monitoring pipeline progress, and auditing execution.

#### 4. `models/` Folder
*   **Purpose**: This directory is dedicated to storing trained machine learning models.
*   **Contents**: Serialized model files. After hyperparameter tuning (e.g., using grid search) and training, the best performing models for different tasks (e.g., regression, classification) are saved here. This allows for their later use in prediction or further analysis without retraining.

#### 5. `output/` Folder
*   **Purpose**: This directory acts as a repository for various outputs generated throughout the pipeline, including insight results, model predictions, evaluations, and benchmark data.
*   **Contents**:
    *   `q1.csv` - `q7.csv`: These CSV files contain the results of the analytical insights generated from Hive. Each file corresponds to an HQL query in the `hive/` folder. The process involves:
        1.  Executing the respective `hive/qN_description.hql` script.
        2.  The results are typically written to a temporary location in HDFS (e.g., `/user/team5/project/warehouse/hive_output/qN`).
        3.  These distributed files/partitions are then merged and copied to the local `output/` directory using the `hadoop fs -getmerge` command, as orchestrated by the `scripts/export_insights.sh` script.
    *   Model Prediction Files:
        *   `LinearRegression_reg_predictions.csv`
        *   `LogisticRegression_cls_predictions.csv`
        *   `GBTRegressor_reg_predictions.csv`
        *   `RandomForestClassifier_cls_predictions.csv`
        These CSV files store the predictions made by the trained machine learning models on the respective test datasets (`test_data_reg.json` and `test_data_cls.json` from the `data/` folder).
    *   Model Evaluation CSV Files:
        *   `classification_evaluation.csv`: Contains performance metrics (e.g., accuracy, precision, recall, F1-score) comparing different classification models.
        *   `regression_evaluation.csv`: Contains performance metrics (e.g., MSE, MAE, R-squared) comparing different regression models.
    *   `chart/` (Sub-folder):
        *   **Purpose**: Stores visual representations of the insights.
        *   **Contents**: Image files (e.g., `.png`, `.jpg`) that are charts and graphs. These visualizations are typically generated from a tool like Apache Superset, explaining the results of each of the 7 insights. These images were manually uploaded after dashboard creation.
    *   `benchmark/` (Sub-folder):
        *   `results.csv`: This CSV file stores the performance metrics collected from running the pipeline benchmarks. The benchmarking process is defined and executed by the `scripts/run_benchmark.sh` script.
    *   `src/` (Sub-folder):
        *   **Purpose**: Contains auto-generated Java source files and Avro schema files.
        *   **Contents**: `.java` and `.avsc` files for each table. These are generated by Apache Sqoop during the data import process from PostgreSQL to HDFS when using the Avro file format (as specified in `scripts/export_avro.sh`). The `.avsc` files define the schema of the Avro data, and the `.java` files are the corresponding Java classes Sqoop generates to represent table records.

#### 6. `scripts/` Folder
*   **Purpose**: This central directory houses all the Bash and Python scripts that automate and execute the various stages of the data pipeline.
*   **Contents**:
    *   `01_data_collection.sh`: A Bash script responsible for the first stage of the pipeline. It downloads the raw dataset (`US_Accidents_March23.csv`) from Kaggle and saves it into the `data/` directory.
    *   `02_data_storage.sh`: A Bash script that handles the data storage and initial processing stage. It typically activates the Python virtual environment and then executes `build_projectdb.py` to load data into the PostgreSQL database and set up the schema.
    *   `03_stage2.sh`: A Bash script that orchestrates the data processing stage. It sequentially executes `export_avro.sh` (to move data from PostgreSQL to HDFS), `create_hive_tables.sh` (to define Hive tables over the HDFS data), and `export_insights.sh` (to generate insights from Hive).
    *   `04_stage3.sh`: A Bash script that initiates the machine learning pipeline. It primarily runs the `run.py` Python script, which handles ML model training, evaluation, and prediction.
    *   `build_projectdb.py`: A Python script (likely using `psycopg2`) that forms the core of the data loading and RDBMS setup. It reads data from `data/US_Accidents_March23.csv` and populates the PostgreSQL database. This script orchestrates the execution of SQL scripts for schema creation and data normalization:
        *   Uses `sql/create_staging.sql` to create an initial staging table.
        *   Uses `sql/create_tables.sql` to define the final normalized relational schema (5 tables) with indexes, primary keys, and foreign keys.
        *   Uses `sql/import_data.sql` to perform data transformation, normalization (to 3NF), and splitting data from the staging table into the 5 target tables.
    *   `create_hive_tables.sh`: A Bash script responsible for creating Hive tables. It reads the Avro files (located in HDFS, previously exported by `export_avro.sh`) and defines corresponding Hive schemas, potentially including partitioning and bucketing strategies as defined in `sql/db.hql`.
    *   `export_avro.sh`: A Bash script that uses Apache Sqoop to export data from all tables in the PostgreSQL project database (`team5_projectdb`) to HDFS.
        *   **Functionality**:
            1.  **Environment Setup**: Defines HDFS warehouse path (`WAREHOUSE`), local output directory for generated sources (`OUT_DIR`), and reads the PostgreSQL password from `secrets/.psql.pass`.
            2.  **Cleanup**: Removes any existing `OUT_DIR` locally and the `WAREHOUSE` directory in HDFS to ensure a clean run.
            3.  **Directory Creation**: Re-creates the local `OUT_DIR`.
            4.  **Sqoop Import**: Executes `sqoop import-all-tables` with the following key configurations:
                *   Connects to `jdbc:postgresql://hadoop-04.uni.innopolis.ru/team5_projectdb` with username `team5`.
                *   Uses `--compress` and `--compression-codec bzip2` to store data in HDFS as compressed bzip2 files.
                *   Uses `--as-avrodatafile` to specify Avro as the storage format.
                *   Specifies the HDFS target directory as `$WAREHOUSE/avro`.
                *   Handles NULL values explicitly using `--null-string '\\N'` and `--null-non-string '\\N'`.
                *   Uses a single mapper (`--num-mappers 1`) for the import.
                *   Specifies `$OUT_DIR/src` as the output directory for generated Java source files (`--outdir`).
            5.  **Timing**: Measures and prints the duration of the import process.
        *   **Output**: Data from PostgreSQL tables stored as Avro files in HDFS under `$WAREHOUSE/avro/` and Java source files in `output/src/`.
    *   `export_insights.sh`: A Bash script that automates the generation and export of analytical insights. It iterates through all `*.hql` files in the `hive/` directory, executes them using a Hive client (e.g., Beeline), and saves their results as CSV files into the `output/` directory. This involves querying Hive tables and then transferring results from HDFS to the local filesystem, often using `hadoop fs -getmerge`.
    *   `run_benchmark.sh`: A Bash script designed to execute performance benchmarks for various parts of the pipeline. The results of these benchmarks are typically logged and saved to `output/benchmark/results.csv`.
    *   `run.py`: The main Python script for the machine learning pipeline. It handles:
        *   Loading preprocessed data (e.g., from `data/*.json` files).
        *   Setting up ML pipelines (e.g., using scikit-learn `Pipeline` objects).
        *   Training various ML models (e.g., Linear Regression, Logistic Regression, GBT Regressor, Random Forest Classifier).
        *   Evaluating model performance using appropriate metrics.
        *   Saving the trained models to the `models/` directory.
        *   Generating predictions and saving them to `output/model_predictions.csv` files.
    *   `Untitled.ipynb`: A Jupyter Notebook file. This was likely used for initial exploration, experimentation, and development of the machine learning tasks. The refined and operationalized ML code from this notebook was subsequently refactored into the `run.py` script for robust pipeline execution.

#### 7. `sql/` Folder
*   **Purpose**: Contains SQL and HQL scripts used for database schema definition, data manipulation in PostgreSQL, and Hive table definitions.
*   **Contents**:
    *   `create_staging.sql`: An SQL script containing DDL statements to create the initial staging table in PostgreSQL. This table temporarily holds all raw data ingested directly from `US_Accidents_March23.csv` before any significant preprocessing or normalization.
    *   `create_tables.sql`: An SQL script with DDL statements to create the final, normalized relational schema in PostgreSQL. This typically includes creating five target tables: `accidents`, `twilight`, `road_features`, `locations`, and `weather`. It also defines primary keys, foreign keys, and indexes to ensure data integrity and query performance.
    *   `db.hql`: An HQL script containing DDL statements for creating the primary external Hive tables. These tables are defined over the Avro data residing in HDFS (exported by `export_avro.sh`). This script also likely includes definitions for partitioning and bucketing strategies for large tables to optimize query performance in Hive.
    *   `import_data.sql`: An SQL script containing DML and potentially DDL statements that implement the main logic for transforming and moving data from the PostgreSQL staging table into the five normalized target tables. This script is responsible for data cleaning, normalization to 3rd Normal Form (3NF), and splitting the data appropriately.

#### 8. `secrets/` Folder
*   **Purpose**: Stores sensitive configuration files, primarily credentials for accessing databases and services. This folder should be (and is mentioned to be) included in the `.gitignore` file to prevent accidental commits of sensitive information to version control.
*   **Contents**:
    *   `.psql.pass`: A plain text file containing the password for accessing the PostgreSQL server (`team5_projectdb`).
    *   `.hive.pass`: A plain text file containing the password for accessing the Hive server (if authentication is configured similarly).

#### 9. Root Directory Files

*   `main.sh`:
    *   **Purpose**: The main entry point for executing the entire data pipeline.
    *   **Functionality**: This Bash script orchestrates the execution of the individual stage scripts (`01_data_collection.sh`, `02_data_storage.sh`, `03_stage2.sh`, `04_stage3.sh`) in the correct order.
    *   **Usage**:
        *   `bash main.sh`: Runs the complete pipeline.
        *   `bash main.sh true`: Runs the complete pipeline and includes the execution of `scripts/run_benchmark.sh` to collect and save performance benchmark data.
*   `README.md`:
    *   **Purpose**: The primary documentation file for the project. It provides an overview, setup instructions, pipeline stage descriptions, and usage guidelines. (This document you are reading is an expansion of what would typically be in `README.md`).
*   `requirements.txt`:
    *   **Purpose**: Lists all Python package dependencies required to run the Python scripts within the project (e.g., `build_projectdb.py`, `run.py`).
    *   **Usage**: Used with `pip install -r requirements.txt` to set up the Python environment.

---