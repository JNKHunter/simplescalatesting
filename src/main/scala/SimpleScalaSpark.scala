import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by John on 5/7/17.
  */
case class Posting(postingType: Int,
                   id: Int,
                   acceptedAnswer: Option[Int],
                   parentId: Option[Int],
                   score: Int,
                   tags: Option[String]) extends Serializable

object SimpleScalaSpark {

/*  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SimpleScalaTesting")
  val sc: SparkContext = new SparkContext(conf)

  def filePath = {
    val resource = this.getClass.getClassLoader.getResource("data.dat")
    if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions")
    new File(resource.toURI).getPath
  }*/

  def rawPostings(lines: RDD[String]) : RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(
        postingType = arr(0).toInt,
        id = arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId =      if (arr(3) == "") None else Some(arr(3).toInt),
        score = arr(4).toInt,
        tags = if(arr.length >= 6) Some(arr(5).intern()) else None
      )
  })
  
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    val questions = postings.filter(p => p.postingType == 1).map(q => (q.id, q))
    val answers = postings.filter(p => p.postingType == 2).map(a => (a.parentId.get, a))
    questions.join(answers).groupByKey()
  }

  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

    def answerHighScore(as: Array[Posting]): Int = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    grouped.map(group => {

      val answers = group._2.map(_._2).toArray
      val questions = group._2.map(_._1).toArray
      val highest = answerHighScore(answers)
      var i = 0
      var question:Posting = null

      while (i < questions.length) {
        question = questions(i)
        i += 1
      }
      (question, highest)
    })
  }
/* TODO: Once unit testing is set up, remove this
  def main(args: Array[String]): Unit = {
    val dataRdd = sc.textFile(filePath)
    val postings = rawPostings(dataRdd)
    val grouped = groupedPostings(postings)
    val scored = scoredPostings(grouped)

    scored.foreach(score => println(score))
  }*/

}
