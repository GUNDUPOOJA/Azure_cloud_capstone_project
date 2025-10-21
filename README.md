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

In our databricks Notebook so far we did
1. we create a mountpoint
2. we wrote the spark code to read orders.csv in a dataframe and apply the first validation.. i.e order_id should not repeat
3. if everything is fine, we are creating an orders table.








































  
