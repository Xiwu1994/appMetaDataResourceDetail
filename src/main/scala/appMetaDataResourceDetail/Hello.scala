package appMetaDataResourceDetail


import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

/**
  * Created by liebaomac on 17/11/8.
  */
object Hello {

  def main(args: Array[String]): Unit = {
       val columns = Seq("a","b","c","b") // column
       val data = Seq(1,2,3,1) // line

       val res = columns.zip(data)


        def getValuesMap(data:Seq[Int],names:Seq[String]): Map[String,Int]  = {
             val map = new HashMap[String,Int]()
             for( i <- 0 until names.length){
                 map.put(names.apply(i) , data.apply(i))
             }
            map.toMap
        }

    def getValuesMap1(data:Seq[Int],names:Seq[String]): Map[String,Int]  = {
      val rs = names.zip(data)

       val map = rs.groupBy(_._1).mapValues(_.map(_._2))

       println(map)

     //  map
      Map.empty

    }

       println(getValuesMap1(data, columns))
  }

}
