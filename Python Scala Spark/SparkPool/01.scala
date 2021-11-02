// Lab - Spark Pool - Starting out with Notebooks

// Spark - Resilient Distributed Dataset
// We are creating a simple array of values
// Here val is used to represent an immutable data set
// If you want a mutable data set then you need to use the var construct

val data = Array(1, 2, 3, 4, 5)

// The parallelize method of the Spark context will create an RDD
val dist = sc.parallelize(data)

// To get the count of values in the RDD
dist.count()

// If you want to get the elements of the RDD
dist.collect()