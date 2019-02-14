package com.spark.test.dao

import com.spark.test.HbaseUtils
import com.spark.test.domain.CourseClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CourseClickCountDAO {
  val table_name = "course_click_count"
  val cf = "info"
  val qualifier = "click_count"

  /**
    * save data to Hbase
    * @param list
    */
  def save(list: ListBuffer[CourseClickCount]) :Unit ={
    val table= HbaseUtils.getInstance().getTable(table_name)

    for(ele <- list){
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifier),ele.click_count)
    }
  }

  def count(day_course:String) :Long ={
    val table = HbaseUtils.getInstance().getTable(table_name)
    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes(),qualifier.getBytes())

    if(value==null){
      0l
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8",8))
    list.append(CourseClickCount("20181111_9",9))
    list.append(CourseClickCount("20181111_8",100))

    println(count("20171111_8")+":"+ count("20181111_8"))

  }

}
