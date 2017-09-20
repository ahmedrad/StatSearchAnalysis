# StatSearchAnalysis
## Requirement:
Answer three main questions provided raw crawler data at:
`https://stat-ds-test.s3.amazonaws.com/getstat_com_serp_report_201707.csv.gz`

Questions:
1- Which URL has the most ranks below 10 across all keywords over the period?

2- Provide the set of keywords (keyword information) where the rank 1 URL changes the most over the period. A change, for the purpose of this question, is when a given keyword's rank 1 URL is different from the previous day's URL.

3- We would like to understand how similar the results returned for the same keyword, market, and location are across devices. For the set of keywords, markets, and locations that have data for both desktop and smartphone devices, please devise a measure of difference to indicate how similar these datasets are.

## Setup:
* 3 different pipelines, each pipeline answers one question that way the pipelines are independant and can fail and be retried separately
* Initial pipeline is an extract step to download the csv, this is the only common dependancy.
* Every other pipeline is split into a compute step and a load step, makes for some separation of concerns in the code. Computes are usually a lot more expensive than loading as well so separating them means their failures can be dealt with separately
* The compute step generates the required dataset and saves it as a partitioned parquet file
* The load step loads the parquet file and does minimal represenation manipulation (like sorting) and saves the final result as a csv in
* all pipeline output files are saved to the `out` folder
* to answer the first question a csv file in `under10RankingCount_perUrl_forAllTimePeriod.csv` where the occurrence of any ranking under 10 per URL for the entire dataset is calculated
* to answer the second question a csv file in `topUrlChangesCount_perKeywordInfo_forAllTimePeriod.csv` where the changes in the first ranked URL from one day to the next is calculated per keyword Info (normally the ceiling of this data should be 31 which means that the top ranked URL is changing every day but some keyword Info are showing up to 73 occurrences, the raw data from the crawler is showing that these keywords have multiple readings per day which their handling needs to be discussed as part of the assignment requirements)
* to answer the third question a csv file in `out/deviceRankingDifference_perDay.csv` where the average absolute difference between desktop and smartphone ranking for a given keyword info is calculated per day. The standard deviation is calculated as well. This measure of difference is very simple but very intuitive as it immediately identifies how a user would observe the difference in ranks between devices. Other methods can be used as well for testing the statistical signifiance of the difference between rankings across devices but their need should be discussed further as they're not as straight forward

## Dependencies and Setting up:
### Pythnon 3.6 and iPython:
* Install through Anaconda at `https://www.anaconda.com/download/#download`
* pip should be available as a command line tool after successful installation

### Spark:
* Java requirement:
`Download and install it from oracle.com`

* Install Spark:
```
brew update
brew install scala
brew install apache-spark
```

* Running pyspark in the console should provide a command line interface to spark after a successful installation

### Airflow
* installation:
`pip install airflow`
* airflow commandline tool should be available after successful installation
* default directly for airflow will go in `~/airflow`
* may require installation of other dependencies like mysql or sqlite

### StatSearchAnalysis:
* Clone repo into the airflow folder at `~/airflow`
* Copy the assignment.py DAG file into `~/airflow/dags` (create directory if not already there)
* cd into StatSearchAnalysis and run `pip install -r requirements.txt` to install application dependencies

## Running Pipelines:
* Initializing airflow:
`airflow initdb`
* Running airflow webserver (nice to have):
`airflow webserver`
* Running pipelines:
`airflow backfill assignment -s 2017-09-18` (the date can be changed to the day of execution)
* output can be found at:
`~/airflow/StatSearchAnalysis/out`
* Progress and task tree views can be seen at the airflow webserver:
`http://localhost:8080`
* to clear and re-run:
`airflow clear assignment`
then run backfill commmand again
* to reset airflowdb and start over:
`airflow resetdb`
`airflow initdb`

## Potential improvements:
* The folder structure of the repo entangled in airflow can definitely be improved
* For every task spark is being initialized locally in the executor, this setup will have to change depending on that production spark setup is being used in StatSearch
* This setup is using airflow's sequential executor which is not a production setup and doesn't allow for parallel execution of pipelines
* The written output csvs are repartiioned into one partition which is inefficient, A potential better solution would be for the load step to load the parquet file directly into a DB for visualization and analysis
* Some tests could be written with mock data to ensure logic is sound

