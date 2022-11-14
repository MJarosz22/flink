import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.conditions.Context
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, File, Stats}
import util.{CommitGeoParser, CommitParser, Protocol}

import java.util.{Date, SimpleTimeZone}
import java.text.SimpleDateFormat

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    question_nine(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input.filter(f => f.stats.isDefined).filter(f => f.stats.get.additions >= 20)
      .map(f => (f.sha))
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input.flatMap(c => c.files)
      .filter(f => f.filename.isDefined)
      .filter(f => f.deletions > 30)
      .map(f => f.filename.get)
  }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.flatMap(c => c.files)
      .filter(f => f.filename.isDefined)
      .filter(f => getExtension(f) == ".scala" || getExtension(f) == ".java")
      .map(f => (getExtension(f).slice(1, getExtension(f).size), 1))
      .keyBy(x => x._1)
      .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
      .sum(1)
  }

  def getExtension(file: Protocol.File) :String = {
    '.' + file.filename.get.split('.')(file.filename.get.split('.').length-1)
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(
      input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    input.flatMap(c => c.files)
      .filter(f => f.filename.isDefined && f.status.isDefined)
      .filter(f => getExtension(f) == ".js" || getExtension(f) == ".py")
      .map(f=>(getExtension(f), f.status.get, f.changes))
      .keyBy(x => (x._1,x._2))
      .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
      .sum(2);


  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .assignAscendingTimestamps(c => c.commit.committer.date.getTime)
      .map(c => (c.commit.committer.date, 1))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .sum(1)
      .map(x => (getDay(x._1), x._2))

  }

  def getDay (time: Date): String = {
    val day = new SimpleDateFormat("dd-MM-yyyy")
    val timeZone = new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC")
    day.setTimeZone(timeZone)
    day.format(time).toString
  }
  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] =  {
    input
      .assignAscendingTimestamps(c => c.commit.committer.date.getTime)
      .filter(c => c.stats.isDefined)
      .map(c=>(getType(c), 1))
      .keyBy(x => x._1)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .sum(1)
  }

  def getType(commit: Commit): String = {
    if(commit.stats.get.total > 20)
      return "large"
    "small"
  }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(
      commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    commitStream
      .assignAscendingTimestamps(c => c.commit.committer.date.getTime)
      .keyBy(c => c.url.split('/')(4) + "/" +c.url.split('/')(5))
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new MyProcessWindowFunction())
      .filter(x => x.amountOfCommits > 20 && x.amountOfCommitters <=2)
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[Commit, CommitSummary, String, TimeWindow] {

    def process(key: String, context: Context, input: Iterable[Commit], out: Collector[CommitSummary]) = {
      var minDate = new Date()
      var amountOfCommits = 0
      var committers: List[String] = List[String]()
      var totalChanges = 0
      for (in <- input) {
        if(in.commit.committer.date.getTime < minDate.getTime)
          minDate = in.commit.committer.date
        amountOfCommits += 1
        committers = in.commit.committer.name :: committers
        totalChanges += in.stats.getOrElse(Stats(0,0,0)).total
      }
      val topCommitter = committers.groupBy(c => c).map(x => (x._1, x._2.size)).maxBy(_._2)._1;
      var amountOfCommitters = committers.distinct.size
      val cs = new CommitSummary(key, getDay(minDate), amountOfCommits, amountOfCommitters, totalChanges, topCommitter)
      out.collect(cs)
    }
  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(
      commitStream: DataStream[Commit],
      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)]
   = {
      val joinedStream = commitStream
        .join(geoStream).where(c => c.sha).equalTo(c =>c.sha)
        .window(EventTimeSessionWindows.withGap(Time.minutes(90)))
        .apply((e1, e2) => (e1, e2.continent))
        .assignAscendingTimestamps(c => c._1.commit.committer.date.getTime)
        .map(x => (x._1.files, x._2))
        .map(c => (c._1.filter(f => getExtension(f)==".java"), c._2))
        .map(c => (c._1.map(f => f.changes), c._2))
        .map(c => (c._1.sum, c._2))
        .keyBy(c => c._2)
        .window(TumblingEventTimeWindows.of(Time.days(7)))
        .sum(0)
        .filter(x => x._1 >0)
        .map(x => (x._2, x._1))

    joinedStream
  }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(
      inputStream: DataStream[Commit])//: DataStream[(String, String)]
   = {




    val stream: DataStream[(String,File,Long)] = inputStream
      .flatMap(x => x.files.map(f => (x.url.split('/')(5), f, x.commit.committer.date.getTime)))


    val pattern =
      Pattern.begin[(String, File, Long)]("start")
        .followedBy("middle": String)
        .oneOrMore
        .where(
          (value, ctx) => {
            lazy val prevFile = ctx.getEventsForPattern("start").map(_._2.sha.get)
            lazy val currFile = value._2.sha.get
            prevFile.toList.contains(currFile)
          }
        )

    val patternStream =  CEP.pattern(stream, pattern )

    patternStream
  }


  object Solution {
    def favoritePeerList(students: List[(String, List[String])]): List[(String, String)] = {
      students.map(x => (x._1, getMostPaired(x._2)))
    }

    def getMostPaired(names: List[String]): String = {
      names
        .map(x => (x, 1))
        .groupBy(x => (x._1))
        .map(x => (x._1, x._2.size))
        .sortBy(x => x._2)
        .foldRight(List(): List[(String, Int)])(foo: ((String, Int), List[(String, Int)]) => List[(String, Int)])
        .map(x => x._1).toList(0)
    }

    def foo(x: (String, Int), acc: List[(String, Int)]): List[(String, Int)] = {
      x._2 match{

      }
      if (acc.size == 0)
        return List(x)
      if (acc(0)._2 < x._2)
        return List(x)
      return acc
    }
  }

  object Solution {
    def partition(pivot: Int, list: List[Int]): (List[Int], List[Int]) = {
      list.foldRight((List(): List[Int], List(): List[Int]))(foo(pivot))

    }

    def foo(pivot: Int): (Int, (List[Int], List[Int])) => (List[Int], List[Int]) = {
      return ((x: Int, acc: (List[Int], List[Int])) => ({
        if (x < pivot)
           (x :: acc._1, acc._2)
        (acc._1, x :: acc._2)
      }))
    }
  }

  object Solution {
    def subtract(x: Map[String, Int], y: Map[String, Int]): Map[String, Int] = {
      x.toList.toMap

    }
  }
}
