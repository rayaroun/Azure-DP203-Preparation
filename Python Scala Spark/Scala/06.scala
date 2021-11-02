// Scala - List collection

  var numbers:List[Int]=List(10,20,20,40)
  println("The head of the list is " + numbers.head)
  numbers.foreach{println}

  var customers:List[String]=List("UserA","UserB","UserC","UserD")
  println("The position of UserB in the list is " + customers.indexOf("UserB"))
  println("The value of the customer at index 2 is " + customers(2))
  customers.foreach{println}
