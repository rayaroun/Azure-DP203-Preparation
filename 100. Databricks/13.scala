// Create a new data lake storage account
// Upload a file and give the required permissions
// We will need to create a new cluster with the credential passthrough feature
// Assign the Reader role and Storage Blob Reader Role to the Azure Admin user
// Also remember to give ACL permissions to the Azure Admin user for the container 
// and the file via Azure Storage Explorer

val df = spark.read.format("csv").option("header","true").load("abfss://databricks@newdatalake1000.dfs.core.windows.net/Log.csv")

display(df)