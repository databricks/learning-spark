import org.apache.spark._
import play.api.libs.json._
import play.api.libs.functional.syntax._

case class Person(name: String, lovesPandas: Boolean)
implicit val personReads = Json.format[Person]

val text = """{"name":"Sparky The Bear", "lovesPandas":true}"""

val input = sc.parallelize(List(text))
val parsed = input.map(Json.parse(_))
val result = parsed.flatMap(record => {    
    personReads.reads(record).asOpt
})
result.filter(_.lovesPandas).map(Json.toJson(_)).saveAsTextFile("files/out/pandainfo.json")




