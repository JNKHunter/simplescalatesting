import java.io.File

import SimpleScalaSpark._
import org.scalatest.FunSuite

/**
  * Created by John on 5/8/17.
  */
class SOSuite extends FunSuite with SharedSparkContext {

  def filePath = {
    val resource = this.getClass.getClassLoader.getResource("data.dat")
    if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions")
    new File(resource.toURI).getPath
  }

  test("create data rdd") {
      val dataRdd = sc.textFile(filePath)
      assert(dataRdd.count() == 10)
  }
}
