// Spark - Dataset and Dataframe
// Here we are seeing how to convert an existing RDD to a DataFrame
val data = Array(1, 2, 3, 4, 5)

val dist = sc.parallelize(data)

// Here we are converting our RDD to a DataFrame
val df=dist.toDF()

// Another example using a List
val customers:List[String]=List("UserA","UserB","UserC","UserD")
val customersRDD=sc.parallelize(customers)
val customersdf=customersRDD.toDF()

// To see the data in a Data Frame
customersdf.show()