/**
  * Created by malagusundaram on 3/7/17.
  */
object array_sum {

  def main(args: Array[String]): Unit = {
    var list=Array(1,3,6,9,10)

    var sum=0

    for (x<-list) {

      println(x)
    }

    for (x<-0 to (list.length-1)){
      sum +=list(x)
    }
    println("The sum is :" +sum)

    var max=list(0)
    for (x<-1 to (list.length-1)){
      if(list(x)>max)
        max=list(x)

    }
    println("The max is:" +max)


  }

}
