## Q. How to ensure that items are sampled in IID fashion.
To ensure that items are sampled in an IID (Independent and Identically Distributed) fashion the code picks out the sampled rows based on the weights calculated independently for each row.

## Q. What happens when mentioned constraints cannot be met
A value error stating "Given constraints cannot be met for the given dataset" will be raised by the code

## Q. When the datalake is large, how will my sampler perform
Since the code is calculating weights for each row, the time will increase proportionally with the size of dataset. A solution to this can be parallel processing using MapReduce like implementation.

## Q. Choices made to make code maintainable and scalable
Code is divided into functions to make it more understandable and when needed only needed function can be modified keeping the rest of the functionality same.

## Q. What sort of tests make sense to include? What edge cases user might face, does my code handle those ?  
Most of the edge cases are handled in the code, User might give input ratios which dont add up to 1 and it will throw an error. If the mentioned ratio of rows are not present in the DataLake, it will thow a value error.

## Q. Suppose the datalake had millions of records instead of thousands what programming language, database and cloud storage will be optimal. Would you change how the metadata is stored ? How would you modify your sampler to work with the new datalake ?
Python or Scala will be a good option to perform this task. We can use database technologies like Cassandra or MongoDB as these databases are designed for horizontal scalability and can handle large amounts of data. They are suitable for scenarios where you need to distribute data across multiple nodes. For Cloud storage, we can use AWS S3 or Azure Blob. 
When dealing with a larger datalake, modifications to the sampler may be needed for performance and efficiency:
Parallelization: Parallelizing the sampling process to handle a large volume of data efficiently. Frameworks like Apache Spark can assist with distributed processing.
Batch Processing: Instead of processing the entire datalake in a single pass, consider a batch processing approach to handle data in smaller chunks.
Data Partitioning: Leverage any existing data partitioning in your storage solution to reduce the amount of data that needs to be processed during sampling.
