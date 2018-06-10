package Config

import com.typesafe.config.{Config, ConfigFactory}

/**
  * User: Russell
  * Date: 2018-06-10
  * Time: 10:47
  */
object Confighelper {

    private lazy  val config: Config = ConfigFactory.load()

     val log_path: String = config.getString("log_input_path")




}
