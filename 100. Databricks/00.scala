// Lab - Simple notebook
val data = Array(1, 2, 3, 4, 5)

// The parallelize method of the Spark context will create an RDD
val dist = sc.parallelize(data)

// To get the count of values in the RDD
dist.count()

// If you want to get the elements of the RDD
dist.collect()