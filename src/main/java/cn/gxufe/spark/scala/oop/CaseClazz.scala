package cn.gxufe.spark.scala.oop

object CaseClazz {

  def main(args: Array[String]): Unit = {

    val democlass = new Democlass("zhangsan",30)

    val name = "list"

    val res = name match {
      case "list" => {
        1
      }
      case "zhangsan" => {
        2
      }
      case _ => {
        3
      }
    }

    println(res)

  }
}


case class Democlass(var name:String,var age:Int)