# CoinBase-ETL-Project
### The Architecture of the project

![my project architecture](https://user-images.githubusercontent.com/122371233/228370238-07bb576d-fc5f-4c48-88ba-9b7509e50edf.png)

In this project, the data utilized was not deemed critical as the analysis and information required for the project is readily available on various websites. The data was primarily used for explanatory purposes and to allow me to test what I have learned about ETL pipelines.  
  
*Note: this is my first pipeline project.*
### Making-Process
- CoinMarket API request
- Docker container is used to run airflow
  - Configuring template for Docker image
  - Installing required dependecies
- DAG (consists of 3 tasks)
  - Extract: made a request to the api to extract the data and then placed the raw json data to Amazon S3.
  - Transform: did some data cleaning and transformation and put it in temprorary s3 bucket
  - Load: placed the transformed data to Google BigQuery
- Tableau data analysis
