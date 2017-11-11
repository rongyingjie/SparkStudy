package cn.gxufe.spark.scala.oop


object ClazzDemo {
  def main(args: Array[String]): Unit = {

    ExPersion.sayHello()

    print(ExPersion.name)


  }

}

trait Flyable{
  def fly(): Unit ={
    println("I can fly")
  }

  def fight(): String
}

abstract class Animal {
  def run(): Int
  val name: String
}

class Human(var age:Int,var n:String) extends Animal with Flyable {

  val name : String = n

  //打印几次"ABC"?
  val t1,t2,(a, b, c) = {
    println("ABC------")
    (1,2,3)
  }

  println(a)
  println(t1._3 + "-----")

  //在Scala中重写一个非抽象方法必须用override修饰
  override def fight(): String = {
    "fight with 棒子"
  }
  //在子类中重写超类的抽象方法时，不需要使用override关键字，写了也可以
  def run(): Int = {
    1
  }
}

object Human {

  def sayHuman() = {
    println("sayHuman")
  }

}

object ExPersion extends Human(20,"lisi") {

  def sayHello() = {
    println("sayHello")
  }

}
