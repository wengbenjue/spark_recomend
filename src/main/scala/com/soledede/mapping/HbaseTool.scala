package com.soledede.cf.mapping

/**
 * Created by wengbenjue on 2014/9/15.
 */

import java.io.{ObjectInputStream, ByteArrayInputStream}

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable
import scala.util.parsing.json.JSONArray


object HbaseTool {

  val table = new mutable.HashMap[String,HTable]()
  var conf = HBaseConfiguration.create()

  def setConf(c:Configuration)={
    conf = c
  }

  def getTable(tableName:String):HTable={

    table.getOrElse(tableName,{
      println("----new connection ----")
      val tbl = new HTable(conf, tableName)
      table(tableName)= tbl
      tbl
    })
  }

  def createTable(tableName:String,columnFamily:String): Unit ={
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(columnFamily.getBytes()))
      admin.createTable(tableDesc)
    }
  }


  def getSingleValue(tableName:String,rowKey:String,family:String,qualifiers:String):JSONArray={
    val table_t =getTable(tableName)
    val row1 =  new Get(Bytes.toBytes(rowKey))
    val HBaseRow = table_t.get(row1)
    var obh:AnyRef  = null
      //new JSONArray[List[String]]
    //var bv = new Array[Byte](0)
   // var bv = new String
    if(HBaseRow != null && !HBaseRow.isEmpty){
     val b = new ByteArrayInputStream(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifiers)))
      val o = new ObjectInputStream(b)
      obh = o.readObject()
       o.close()
      //bv = new String(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifiers)),"UTF-8")
      }
      obh.asInstanceOf[JSONArray]
  }

  def getValue(tableName:String,rowKey:String,family:String,qualifiers:Array[String]):Array[(String,String)]={
    var result:AnyRef = null
    val table_t =getTable(tableName)
    val row1 =  new Get(Bytes.toBytes(rowKey))
    val HBaseRow = table_t.get(row1)
    if(HBaseRow != null && !HBaseRow.isEmpty){
      result = qualifiers.map(c=>{
        (tableName+"."+c, Bytes.toString(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(c))))
      })
    }
    else{
      result=qualifiers.map(c=>{
        (tableName+"."+c,"null")  })
    }
    result.asInstanceOf[Array[(String,String)]]
  }

  def getMapSingleValue(tableName:String,rowKey:String,family:String,qualifiers:String):String={
    var result:AnyRef = null
    val table_t =getTable(tableName)
    val row1 =  new Get(Bytes.toBytes(rowKey))
    val HBaseRow = table_t.get(row1)
    if(HBaseRow != null && !HBaseRow.isEmpty) {
      result = Bytes.toString(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifiers)))
    }
    result.asInstanceOf[String]
  }

  def putValue(tableName:String,rowKey:String, family:String,qualifierValue:Array[(String,String)]) {
    val table =getTable(tableName)
    val new_row  = new Put(Bytes.toBytes(rowKey))
    qualifierValue.map(x=>{
      var column = x._1
      val value = x._2
      val tt = column.split("\\.")
      if (tt.length == 2) column=tt(1)
      if(!(value.isEmpty))
        new_row.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
    })
    table.put(new_row)
  }

  def putSingleValue(tableName:String,rowKey:String, family:String,qualifier:String,cValue:String) {
    val table =getTable(tableName)
    val new_row  = new Put(Bytes.toBytes(rowKey))
      var column = qualifier
      val value = cValue
      val tt = column.split("\\.")
      if (tt.length == 2) column=tt(1)
      if(!(value.isEmpty))
        new_row.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(new_row)
  }

  val family = "F"
}