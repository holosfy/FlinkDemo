package com.holos.dataset

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",9999)
    //val text = env.fromElements("Who's there?","I think hear them. Stand,Ho! Who's there?")

    /**
      * \w：用于匹配字母，数字或下划线字符；
      * \W：用于匹配所有与\w不匹配的字符；
      */
    val word = text.flatMap(_.toLowerCase.split("\\W+"))
    word.print()

    val counts = text.flatMap{_.toLowerCase.split("\\W+") filter {_.nonEmpty}}
      .map((_,1))
      .keyBy(0)
      .sum(1)

    counts.print()
    env.execute("Window Stream WordCount")

  }
}
