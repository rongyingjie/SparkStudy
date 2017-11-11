package cn.gxufe.spark.scala.oop


object Helloworld {


  def main(args: Array[String]): Unit = {

    val startService: Int => Int = {
      x => {
        x + 5
      }
    }

    def ff(x:Int):(Int,Int) = {
      (x + 11,x)
    }



    m1( startService,10,10 )



    val ss= List(1, 3, 5, "seven") map {
      case i:Int => i+1
      case a:String => a+"_new"
    }

    print(ss)
  }

  def m1( f: (Int => Int) , x: Int,y : Int ): Int = {
    f(x) + y
  }


  def m2(f: (Int, Int) => Int) : Int = {
    f(2, 6)
  }


  def p(name:String):String =  name match {
    case "a" =>{
      "aa"
    }
    case "b" => {
      "bb"
    }
    case _ => {
      name
    }
  }


}
