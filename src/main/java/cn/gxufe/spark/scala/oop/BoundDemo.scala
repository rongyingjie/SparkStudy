package cn.gxufe.spark.scala.oop

object BoundDemo {

  def main(args: Array[String]): Unit = {

    val upperBoundDemo = new UpperBoundDemo[Integer](10)

    upperBoundDemo.sayHello( x => {
      String.valueOf(x)
    } )

    val lowerBoundDemo = new LowerBoundDemo[Object](new Object)

    lowerBoundDemo.sayHello( t => {
      t.toString
    } )

  }
}

class UpperBoundDemo[T <: Number](var t:T) // 泛型上界
{
  def sayHello( f:T => String ): Unit ={
    println( "value = " + f.apply(t) )
  }

}

class LowerBoundDemo[ T >: Number ](var t:T)
{

  def sayHello(  f:T => String ):Unit = {
    println( f.apply( t ) )
  }

}