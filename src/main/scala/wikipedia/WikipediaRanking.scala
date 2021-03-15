package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class WikipediaArticle(title: String, text: String) {
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking extends WikipediaRankingInterface {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setAppName("Wikipedia").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)

  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines)
    .map((line: String) => WikipediaData.parse(line))

  val ss: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

  import ss.implicits._


  // Returns the number of articles on which the language `lang` occurs.
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.filter((line: WikipediaArticle) => line.mentionsLanguage(lang)).count().toInt
  }

  // (1) Use `occurrencesOfLang` to compute the ranking of the languages
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map((lang: String) => (lang, occurrencesOfLang(lang, rdd))).sortBy(-_._2)
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd.flatMap((line: WikipediaArticle) => {
      langs.filter((lang: String) => line.mentionsLanguage(lang))
        .map((lang: String) => (lang, line))
    }).groupByKey()
  }

  // (2) Compute the language ranking again, but now using the inverted index.
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.mapValues((line: Iterable[WikipediaArticle]) => line.size)
      .sortBy(-_._2)
      .collect()
      .toList
  }

  // (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val rddResult = rdd.flatMap((line: WikipediaArticle) => {
      langs.filter((lang: String) => line.mentionsLanguage(lang))
        .map((lang: String) => (lang, 1))
    }).reduceByKey(_ + _)
      .sortBy(-_._2)
      .cache()

    val langsDF = rddResult.toDF("lang", "count")

    // Register the DataFrame as a temporary view
    langsDF.createOrReplaceTempView("langs")

    ss.sql("SELECT * FROM langs").show()

    rddResult.collect().toList
  }

  def main(args: Array[String]): Unit = {

    // Languages ranked according to (1)
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    // Inverted index mapping languages to wikipedia pages
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    // Languages ranked according to (2), using the inverted index
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    // Languages ranked according to (3)
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    // Output the speed of each ranking
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
