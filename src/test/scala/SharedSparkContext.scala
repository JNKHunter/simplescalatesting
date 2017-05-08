import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by John on 5/8/17.
  */
trait SharedSparkContext extends BeforeAndAfterAll { self:Suite =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  var conf = new SparkConf(false)

  override def beforeAll(): Unit = {
    _sc = new SparkContext("local[2]", "test", conf)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    /*LocalSparkContext.stop(_sc)
    _sc = null*/
    super.afterAll()
  }



}