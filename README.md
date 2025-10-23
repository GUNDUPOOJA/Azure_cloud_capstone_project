  **Azure Project using Databricks and Data Factory**
**USE CASE 1**
  - There is a third party service which drops orders.csv every day into a **landing zone (ADLS Gen2)**
  - our requirement is as soon as the file arrives in the landing folder - we need to apply some checks
  - The checks are
    1. There should not be any duplicates ( no duplicate order_id) 
    2. Check for valid order status
  - If both the above conditions are true, then move the file to staging folder else move it to discarded folder
  - In future, if valid order status list is changed, we should be able to dynamically incorporate the changes.

**Implementation**
- we would have storage in ADLS Gen2 -> Data factory (trigger)-> Databricks Notebook (runs spark code)
- ADF is a storage event trigger as soon as the file arrives, it triggers the pipeline
- DF will then trigger databricks notebook which will have your code to perform those 2 checks - then we move the file to staging or discarded folder

1. storage account
2. Databricks workspace
3. Data factory - 3 linked services
4. Azure SQL DB - to store valid_order_status table
5. Create interactive cluster in Databricks


**Building project pipeline resource creation**
Let us create the resources
1. Storage account - trendytechsaproject
- In this storage account -create sales container - create 3 folders - landing, staging and discarded
2. create a workspace with the name - trendytech-sales-databricks-ws
3. create a Data Factory service - trendytechsalesdfproject

- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/e4631820-e45c-4c4e-8ad7-b7ffa8835030" />

- we need to connect Data factory to two components
  1. ADLS Gen2
  2. Databricks
 
- we will try to use Key vault to store any passwords/secrets keys
  
1. we need to create linked service for ADLS Gen2
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/cf809d4a-8001-4769-a4ae-36b550330261" />

2. we need to create linked service to Databricks
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/0444a753-3344-4259-a0ee-4ef6d3ec566c" />
- If you want the ADF to connect to databricks we need a token - Get the access token from databricks workspace 
- Databricks provides a token,so that any external service can connect
- storing the token directly isn't a good practice, store in keyvault
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/221f8a80-6687-4a36-9a16-0f67ffbf1445" />

- create a keyvault service(name - trendytechsalesprojectkv) and store this token credential over there
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/2d019b24-e195-4af8-bdf8-34edef0e2cbc" />
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/0b5c8fdf-f34f-462e-8fe3-3c8e4af2d3c1" />

3. we need to create linked service to key vault
- we need to allow ADF in access policies to create linked service to key vault
- Go to key vault - access policies - create new access policy
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/e335db19-5ead-4b15-baaf-083ddb827ee5" />
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/74951a13-ff2c-45fe-829c-cf1a90162acc" />

we created 3 linked services so far..

**Creating Azure SQL DB - Lookup Table**
- we have a list of order_status we need to validate, this list is dynamic, we might have to keep this in a lookup in a database, whenever the list of order status is changed, we again verify against the db
- Azure SQL db - To keep list of valid status in a lookup
- Azure sql db name - trendytechsqldbproject
- Azure sql db server name - trendytechsqlserverprojectt
- once the db is ready, we need to create a table( table_name : valid_order_status)
- create table valid_order_status(status_name varchar(50))
- insert into valid_order_status values('ON_HOLD'),('PAYMENT_REVIEW'),('PROCESSING'),('CLOSED'),('SUSPECTED_FRAUD'),('COMPLETE'),('PENDING'),('CANCELLED'),('PENDING_PAYMENT')
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/679741d8-8cee-492c-aaf6-d2abd5805561" />

**Developing Logic using Interactive Databricks cluster**
- Lets create an interactive cluster in databricks, later will schedule this notebook through our job cluster.
- Storage event trigger in ADF will trigger the pipeline means it will execute the databricks notebook
- we need to create a **mount point** to read data from storage account
  
- `dbutils.fs.mount(source = 'wasbs://sales@trendytechsaproject.blob.core.windows.net', mount_point = '/mnt/sales',
extra_configs = {'accesskey from ADF'}`

- Get the extra configs from -> Storage account -> access keys
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/0323781c-50db-4620-9ed1-d9908b0aa08b" />

- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/9c163596-e97b-42ed-995f-cdb4d41bb127" />
- upload orders.csv file in landing folder of ADLS
- check the mount point
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/f5cda8da-bbeb-4d71-ad61-9920450a874d" />
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/4c662964-de3f-42d6-9bc2-c43bc22bcafc" />
- `dbutils.notebook.exit('{"errorFlg": "true", "errorMsg": "Orderid is repeated"}')`

In our databricks Notebook so far we did
1. we create a mountpoint
2. we wrote the spark code to read orders.csv in a dataframe and apply the first validation.. i.e order_id should not repeat
4. if everything is fine, we are creating an orders table.
5. we need to apply second validation - order_status is valid or not - To do this we need connectivity to Azure SQL DB from our databricks notebook
we need few details :
`dbserver : 'trendytechsqlserverprojectt'
dbport :'1433'
dbname : 'trendytechsqldbproject'
dbusername : 'ttsqluser'
dbpassword : 'sql-password'
`

- `connectionUrl = 'jdbc:sqlserver://{}.database.windows.net:{}; database = {} ; user={}; '.format(dbServer, dbPort, dbName, dbUser)`

- `dbPassword = dbutils.secrets.get(scope = databricksScope, key = 'sql-password')`
  
- `connectionProperties = {
      'password' : dbPassword,
      'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver}`
  
- `ValidstatusDF = spark.read.jdbc(url = connectionUrl, table = 'db0.valid_order_status' properties = connectionProperties`
  
- `display(ValidstatusDF )`

- `ValidstatusDF.CreateOrReplaceTempview("valid_status")`

- we have stored db password in keyvault, but databricks can't access key vault directly, its a third party service - it will retrieve it using secret scope
- databricks -> secret scope -> connects to key vault
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/95052586-dbb8-4065-b902-f839d0dc4108" />
- Get the key vault uri and resource ID from key vault properties 
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/82c787cd-4350-4d29-aa16-476f1225c0c4" />

- we were able to connect to sql db and read the table into a df
- `invalid_rows_df = spark.sql(select * from orders where order_status not in (select * from order_status))`
- Refer sales_notebook databricks ( attached in this repository)

## Creation of Azure Data Factory pipeline with Storage Event Trigger
As soon as the file lands in sales container landing folder - execute this notebook 
- Create a data factory pipeline
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/47912c09-dfc8-44d1-840a-a793fa824c80" />
- Add notebook path as well
- Now, add a trigger
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/a940d78b-e84a-440b-80ca-0ffd6d12558d" />
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/26523561-fd68-4f76-ae28-033f0815c2d6" />
- The code will get executed as part of the notebook, and the created cluster will be destroyed later.

- In databricks we have job cluster, interactive cluster, resource pool(few machines will come as part of the cluster)
- If we have a pool, the time to provision a cluster will become less

**Summary of use case 1:**
- We basically first created a storage account, datafactory with 3 linked services, databricks, azure sql db to store lookup table
- then we developed the databricks notebook code with interactive cluster, after the code was developed we terminated the cluster and then we created a df pipeline (storage event trigger) as soon as file arrives in landing folder it will trigger the pipeline.
  
**USE CASE 2:Parameterized approach to dynamically read file names**
- **Problem statement:**
- Right now, our solution caters only to orders.csv, we have hardcoded it, what if the problem says it can be any file which is in landing folder.
- Previously, we have created a trigger with hardcoded values, but we need to create parameters for our pipeline
- First trigger should dynamically read the filename - this internally should be passed to the pipeline - it should be passed to the databricks notebook
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/41fd32de-35f1-490b-964b-9a663e360cae" />
- we want to retrieve the exact file name - trigger has the way we can say @triggerBody().filename
**File name flows from trigger -> pipeline -> Databricks**
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/7df93a73-6709-45c9-9d27-1fe8d7c1c3d4" />
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/620e1ef2-2b62-4727-a1fa-8ef737684282" />

- <p> when triggering the pipeline, sometimes we face quota exceed error as it creates new cluster on the go and takes time to provision resources, instead of new job cluster in Databricks linkedservice change the option to existing cluster - so that it uses existing cluster. </p>

**Making the pipeline generic - Generic Mount code | Secured storage account key**
- <p> we made the filename dynamic but the problem we were facing is - the code for mounting works for the first time and from second time we need to remove it, we have to generalize it and make sure it works all the time.</p>
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/09ae99b0-3059-424b-9751-6e4add386dd4" />

- while mounting in extra_configs section - account key is hardcoded, keep it in keyvault

### Ingesting data from AWS S3 to Azure
- so far we did
- 1. dynamic filename
  2. For mounting we have made generic code
  3. storage account key is secure
- We have orders.csv(orders data) - we already processed this file
- order_items (Amazon s3 in JSON Format)
- customers (will be published by an agency in Azure SQL DB )
  
- Get order_items from AWS S3 to ADLS Gen2 (using ADF)
- create bucket - trendytechsalesproject
- upload order_items.json file in order_items folder
- <img width="300" height="300" alt="image" src="https://github.com/user-attachments/assets/d37356db-2a66-40ec-b45f-e95d3e4e7bf0" />

- IAM dashboard - create access key - save access key and secret access key and store it in Key vault
- Go to ADF and create a linked service for AWS S3
- As soon as orders file arrives in landing folder of ADLS Gen2, additionally we need to get order_items file from AWS S3 and bring to ADLS Gen2 and execute databricks notebook

- we would have customers data in Azure SQL DB - we need to take the file from here
- 


































  
