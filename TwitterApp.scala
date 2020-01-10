package com.elh.twitter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import scopt.OptionParser
import scala.io.Source
import com.elh.twitter.model.Configuration
import com.elh.twitter.model.MyJsonProtocol._
import spray.json._

object TwitterApp{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val parser: OptionParser[Configuration] = new scopt.OptionParser[Configuration]("App Twitter Streaming") {
      opt[String]('f', "pathConfig") required() action
        {
          (x, c) => c.copy(pathConfig = x)
        } text "conf is the configuration file"
    }

    parser.parse(args, Configuration()) match {
      case Some(config) => {
        val jsonContent = Source.fromFile(config.pathConfig).mkString

        val conf = jsonContent.parseJson.convertTo[Configuration]

        //Les informations d’authentification de Twitter
        System.setProperty("twitter4j.oauth.consumerKey", conf.apiKey)
        System.setProperty("twitter4j.oauth.consumerSecret", conf.apiKeySecret)
        System.setProperty("twitter4j.oauth.accessToken", conf.accessToken)
        System.setProperty("twitter4j.oauth.accessTokenSecret", conf.accessTokenSecret)

        val ssc = new StreamingContext("local[*]", "TestAPITweets", Seconds(1))

        val filters = Array("Colombes","Paris")

        val tweets = TwitterUtils.createStream(ssc, None, filters)

        val statuses = tweets.map(status => status.getText())
        statuses.print()

        ssc.start()
        ssc.awaitTermination()

      }
      case _ => println("Bad arguments")
    }



  }

}


##Créer un package model et copiez cette classe dedans
package com.elh.twitter.model
import spray.json._
​
case class Configuration(pathConfig:String = "" , apiKey: String="", apiKeySecret:String="", accessToken: String="", accessTokenSecret:String="")
​
object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val confFormat = jsonFormat5(Configuration)
}
