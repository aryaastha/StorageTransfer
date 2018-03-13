# StorageTransfer
This is a service that can tranferred from any source to any sink.

# Features
Data can be tranformed to another form. The following tranformation processes are supported. 
The config is passed as json file.

## Processes
### Mapping 
Combining two or more source fields to form another sink field. There are two types of mapping, simpleColumn and customColumn.
simpleColumn has a string value passed by the config file, custom column is a value derived from data(Used in hbase sink)


### Aggregations 
Performing aggregations to get a sink field on the basis of operation performed on another source field. 

### Pivot 
You can transfer the data for a particular key from multiple rows to single row, pivoting on one or more columns. 

### Derivation 
You can derive another field using the user defined functions. 

# How to deploy
1. Transfer the jar to the required box.
2. Using the spark-submit command, submit the job passing the config file as command line argument. (SourceFile.properties in resources) 
This config file mentions the sourcetype, sinkType and the paths for the json-config files for source, sink and process

Current implementation have Hive and Hbase as datasources. 

# About {process.json}
process.json contains all the transformation information i.e the list of the processes to be applied on the data from source. 

