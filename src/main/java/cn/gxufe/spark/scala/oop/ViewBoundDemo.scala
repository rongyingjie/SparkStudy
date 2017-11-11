package cn.gxufe.spark.scala.oop


object ViewBoundDemo {

  def main(args: Array[String]): Unit = {

    // 第一种实现方式
    //    implicit val studentToOrder = (s:Student) => new Ordered[Student] {
    //      override def compare(that:Student)  = {
    //        that.score - s.score
    //      }
    //    }

    // 第二种实现方式
    //    implicit val studentToOrder = new Ordering[Student]{
    //      override def compare(x: Student, y: Student) = {
    //        x.score - y.score
    //      }
    //    }

    // 第三种实现方式
    implicit object StudentVal extends StudentToOrder

    val ordered = new OrderScore[Student]
    val s1 = new Student(90,"zhangsan")
    val s2 = new Student(89,"lisi")
    val res = ordered.compare(s1,s2)
    println(res.name)

    val studentOrderObj = new StudentOrderObj[Student]
    val s = studentOrderObj.compare(s1,s2)
    println(s.name)

    val studentContext =  new StudentContext[Student]

    val ss = studentContext.compare(s1,s2)
    println(ss.name)
  }

}

class StudentToOrder extends Ordering[Student]{
  override def compare(x: Student, y: Student) = {
    x.score - y.score
  }
}

class Student(var score:Int,var name:String)

class OrderScore[ T <% Ordered[T] ]
{
  def compare(t1:T,t2:T):T = {
    if(t1 > t2){
      t2
    }else{
      t1
    }
  }
}

class StudentOrderObj[T]
{
  def compare(t1:T,t2:T)(implicit  ord : Ordering[T]):T = {
    if(ord.lt(t1,t2)){
      t1
    }else{
      t2
    }
  }
}

class StudentContext[T:Ordering] {
  def compare(t1:T,t2:T):T = {
    val ord = implicitly[Ordering[T]]
    if(ord.lt(t1,t2)){
      t1
    }else{
      t2
    }
  }
}

