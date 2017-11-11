package cn.gxufe.spark.scala.oop

object OopHelloworld extends App {

  val person = Person("zhangsan",30)

  println(person)

  val p = new Person()
  println(p)


  var map  = Map(("a"->1),("b"->2))
  val res  = map.getOrElse("c",3)

}

object Person {
  def apply(name: String, age: Int): Person =
  {
    println("apply -- > ")
    new Person( name, age )
  }
}


class Person(var name:String,var age:Int){

  println(name+","+age)


  def this(){
    this("zhangsan",89)
  }

}