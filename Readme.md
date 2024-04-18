Project Ongoing: 
Collect forumla1 racing data from https://ergast.com/mrd/ and implement a medalian architecture using databricks and Azure data factory. to ingest process and present data in cloud for analysis and possible model training

features:
- Data is stored in azure blob storage. The storage is divded into raw, processed and presentation containers
- Seevice principal with Storage Blob Data Contributor role to access and write data to the storage account
- The access is managed using OAuth Client Secret Authentication model. The client ID, Tentant ID, Client secret is stored in Azure Key vault and Databcricks secret scope is used to access it
- Data files is CSV and JSON formats are ingested to in parque files in the processed zone. Data was partitioned by race year to improve reading time for future use. Basic data cleaning transformations such fixing ambigious column names, adding ingetion timestamps were done before ingestion
- Schema was enforced for the data moving to processed zone
- From the proccessed folder, tables were joined and aggregation operations were done to create views for presentation layer. This data is stored in presentation layer and will be consumed by a dashboard to reporting and analysis

\# TODO:
- Implement azure data factory pipelines to automate and schedule the ingestion process
- Use tableau/power BI to connect to the data in the presentation layer
- Implement notifaction system to notify via email if an ingestion is missed or ingestion or if file was processed
