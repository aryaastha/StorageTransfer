# StorageTransfer
You can now get your data transferred from any source to any sink .

# Features
Data can be tranformed to another form. The following tranformation processes are supported. The config is passed as json file.

## Processes
### Mapping 
Combining two or more source fields to form another sink field.

### Aggregations 
Performing aggregations to get a sink field on the basis of operation performed on another field. 

### Pivot 
For examples where you want top K values for a particular index, you can pivot the data.

### Derivation 
You can derive another field using the user defined functions. 

# How to deploy
1. Transfer the jar to the required box.
2. Using the spark-submit command, submit the job passing the config file as command line argument. (SourceFile.properties in resources) 
which is mentioning the paths for the config files for sources and sink as well as preprocess.json

Current implementation ave Hive and Hbase as datasources. 

# About {preprocess.json}
There are two types of mapping, simpleColumn and customColumn.
simpleColumn has a string value passed by the config file, custom column is a value derived from data(Used in hbase sink)

