/********************************************************************************
  * Created by Siqi Zhou on 31/07/2019
  ********************************************************************************/
package reg.spark

import java.io.{BufferedReader, ByteArrayOutputStream, File, FileReader, InputStreamReader}
import java.net.URI
import java.util
import javax.imageio.ImageIO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.domain
import org.apache.hadoop.util.domainRunner
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.ZooKeeperConnectionException
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.client.TableDescriptor
import org.apache.hadoop.hbase.client.TableDescriptorBuilder
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import collection.mutable._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

object fileSpark {

  def main(args: Array[String]): Unit = {
    //refactor driver
    val setMaster : String = "yarn"
    val spark : SparkSession = SparkSession.builder().appName(applicationName).master(setMaster).getOrCreate()

    /*****Read data from HDFS*****/
    val dataPath : String = args(0)

    val xDf : DataFrame = readdata(dataPath,spark)
    xDf.show(1)
    println("xDf count:" + xDf.count())




    /*****UDF*****/
//    val yDetectionUDF : UserDefinedFunction = udf(yDetection(_ : String, _ : Configuration) : Unit)
//    spark.udf.register("yDetectionFunction", yDetectionUDF)
//    xDf.withColumn("file_contents", "yDetectionFunction")
    /*****Map*****/
    import spark.implicits._
    xDf.selectExpr("file_contents").repartition(10).foreachPartition(
      partition => {
        val listBuffer : ListBuffer[Boolean] = new ListBuffer[Boolean]
        /*
        Create ur hbase connection / table object
         */
        val conf : Configuration = new Configuration()
        conf.addResource(HBaseConfiguration.create())
        if(!HBaseConfiguration.create.get("zookeeper.znode.parent").contains("unsecure")){
          try{
            login(conf, username + "@NA.gmail.COM", "/home/" + username + "/.keytab/" + username + ".keytab")
          }
          catch {
            case e: Exception => println("exception caught: " + e)
              println("Secure log in failure. Check if keytab file is in place...")
          }
        }
        val conn : Connection = ConnectionFactory.createConnection(conf)

        partition.foreach(
          row => {
            println("Come into Map")
            val flag : Boolean = yDetection(row.getAs[String]("file_contents"), conn, maxline18, maxline17, maxpoint, hbaseNamespace, dOffset, historyDays, alltogether, hdfsDataPath)
            listBuffer.append(flag)
          })
        //listBuffer.toIterator
      }
      /*
      table.close()
       */
    )
    println("finish map")

  }

  def login(conf : Configuration, kerberosPrincipal : String, kerberosKeytab : String): Configuration = synchronized{
    conf.set("hbase.myclient.keytab", kerberosKeytab)
    conf.set("hbase.myclient.principal", kerberosPrincipal)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab)

    conf
  }

  /*****Read data from HDFS*****/
  def ls(path : String) : List[String] = {
    val hadoop : FileSystem = {
      val conf : Configuration = new Configuration()
      //conf.set( "fs.defaultFS", "hdfs://localhost:9000" )
      FileSystem.get(conf)
    }
    val files = hadoop.listFiles(new Path(path),false)
    val filenames = ListBuffer[String]()
    while (files.hasNext) filenames += files.next().getPath.toString
    println(filenames.toList)

    filenames.toList
  }

  def readdata(dir : String, spark : SparkSession): DataFrame={
    val startTime : Long = System.nanoTime()
    val datalist : List[String] = ls(dir)
    println("data list is " + datalist)
    //val spark = new SparkSession.Builder().appName("iy").master("yarn").getOrCreate()
    //var df : DataFrame = spark.emptyDataFrame
    val schema = StructType(
      StructField("filename", StringType, true) :: StructField("file_contents", StringType, true) :: Nil)
    var df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    for (dataFile <- datalist){
      val dfdata = spark.read.format("com.databricks.spark.data").load(dataFile)
      val dfStage = dfdata.select("operational_metadata.buffer_server.filename", "operational_metadata.buffer_server.file_contents")
        .withColumnRenamed("operational_metadata.buffer_server.filename","filename")
        .withColumnRenamed("operational_metadata.buffer_server.file_contents","file_contents")
      val dfResult = dfStage.withColumn("file_contents", dfStage.col("file_contents").cast("string"))
      df = df.union(dfResult)
    }
    val endTime : Long = System.nanoTime()
    val readdataTime : Long = (endTime - startTime)/1000000000
    println(s"Total time taken - reading data: $readdataTime seconds")

    df
  }


  def yDetection(fileContent : String, conn : Connection, maxline18 : Int, maxline17 : Int,
                       maxpoint : Int, hbaseNamespace : String, dOffset : Double, historyDays : Int, alltogether : Int, hdfsDataPath : String): Boolean ={
    /*
    dOffset, historyDays, alltogether are used in Function analyse
    conn is used in Function analyse
     */

    val startTime : Long = System.nanoTime()
    var scale : Int = 1000

    val KL:fileExtractANTLR = if (fileContent.contains("Record FileRecord")){
      val version : Double = 1.8
      new fileExtractANTLR(fileContent,version,1,maxline18)
    } else {
      val version : Double= 1.7
      scale = 1
      new fileExtractANTLR(fileContent, version, 1, maxline17)
    }

    /*****x file parser*****/
    if (fileContent.contains("Record FileRecord")){
      try{
        val domainType : String = KL.imageInfoMap.get("domainType")
        /* domainType example: Puma9850M
           in x file:
           Field InspectionStationID 3 {"KLA-TENCOR", "Puma9850M", "KTPUAX0800"}
        */
        if (domainType == "CIRCL" || KL.imageInfoMap.get("DiePitchX") == "300000000") {
          //FIXME: Temporary Until Meeting with RDA for CIRCL domains

          return false
        }
      }
      catch {
        case e: Exception => println("exception caught: " + e)
          if (KL.imageInfoMap.get("domainarge") == "domainarge") {
            println("blackmap")
          }
      }
    }else{
      try{
        if(KL.imageInfoMap.get("domainarge") eq "domainarge") {
          println("blackmap")

          return false
        }
      }catch {
        case e: Exception => println("exception caught: " + e)
      }
    }
    var endTime : Long = System.nanoTime()
    val xParsingTime : Long = (endTime - startTime)/1000000000
    println(s"Total time taken - x parsing: $xParsingTime seconds")

    //error handling
    val deviceID : String = KL.imageInfoMap.get("DeviceID")
    if (deviceID == null){
      println("missing device ID")
       return false
    }
    val imageID : String = KL.imageInfoMap.get("imageID")
    val domainID : String = KL.imageInfoMap.get("InspectionStationID")
    val domainType : String = KL.imageInfoMap.get("domainType")
    val iConf : iy.Configuration = new iy.Configuration

    if (deviceID == "BARE") {
      iConf.flatPositionNo = 0
    }
    if (imageID.length != 12) {
      println("Invalid Scribe ID")
      return false
    }

    if (KL.pointList.size > maxpoint) {
      println("BlackMap")
      return false
    }
    //var record:String = null

    /* KSP domain is especially for test images/igs.
       reveiw domain is after inspection....which is to take picture on the point, it is too late for RDA
       currently both of them are not used in iy..
    */
    println(s"domainID: $domainID")
    if ((!domainID.contains("KT28") && !domainID.contains("KTBF")
      && domainID.contains("KTPU") && domainID.contains("AMU") &&
      domainID.contains("RMAC")) || domainType.contains("SEM")){
      println("KSP domain or review domain")
    }
    //TODO  check algo parameters and testfile loading
//    var testFile : URI = null
//    var robotArmFile : URI = null
//    val cFile : Array[URI] = new Array[URI](5)
//
//    try {
//      testFile = new URI(hdfsDataPath + "/testMTPART.csv#testfile")
//      robotArmFile = new URI(hdfsDataPath + "/RobotArmDimension.csv#robotarmFile")
//
//      for (i<-0 to 4){
//        cFile(i) = new URI(hdfsDataPath + "/ParamConfig" + i + ".properties#cFile" + i)
//      }
//    } catch {
//      case e: Exception => println("exception caught: " + e)
//        println("Error in input parameters or reference files. Exiting...")
//    }

    val testFilePath : String = hdfsDataPath + "/testMTPART.csv"
    val testConf : Configuration = new Configuration()
    val path : Path = new Path(testFilePath)
    val hadoop : FileSystem = FileSystem.get(testConf)
    //println(s"test csv path: $path")

    for (algoConfIndex<-0 to 4){
      val cFilePath : String = hdfsDataPath + "/ParamConfig" + algoConfIndex.toString + ".properties"
      //println(s"algorithm parameter path : $cFilePath")
      iConf.loadFromProperties(cFilePath, algoConfIndex)
    }

    val bf : BufferedReader = new BufferedReader(new InputStreamReader(hadoop.open(path)))
    /*
		"testfile": testMTPART.csv
		Its usage currently is to provide a reference for flatPositionNo which indicates notch alignment in test for each device
		in f10n: under /eng/mti/singapore/fab_10/rda_fingerprinting/RDA_Fingerprinting_WD/Data/testMTPART.csv

		FlatPositionNo: degree of notch position in test system. 0 is notch down, 90 is notch left, 180 is notch up and 270 is notch right. This field is mandatory.
		*/
    iConf.flatPositionNo = -1
    breakable{
      while(bf.readLine() != null)
      {
        val record = bf.readLine()
        //println(s"test CSV line: $record")
        if (record.contains(deviceID)){
          iConf.flatPositionNo = record.split(",")(4).toInt
          break}
      }
    }

    bf.close()
    //quit if invalid device ID
    if (deviceID.equals("None")){
      println("device ID is str None")
      iConf.flatPositionNo = 0
    }
    println(s"flatPositionNo: ${iConf.flatPositionNo}")
    if(iConf.flatPositionNo < 0){
      println("Invalid device ID")
      return false
    }

    /*****prepare image history info*****/
    /*****obtain rowkey list of HBase table imagehist by scanning with image id*****/
    val imageHistHTName : String = hbaseNamespace + ":imagehist"
    val imageHistIndexHTName : String = hbaseNamespace + ":imagehistindexbyimage"

    val hbaseAdm : Admin = conn.getAdmin
    if(!hbaseAdm.isTableAvailable(TableName.valueOf(imageHistHTName.getBytes)) ||
      !hbaseAdm.isTableAvailable(TableName.valueOf(imageHistIndexHTName.getBytes))) {
      println("image history table not exist")
      return false
    }

    val imageHistIndexHT: Table = conn.getTable(TableName.valueOf(Bytes.toBytes(imageHistIndexHTName)))

    val imageidToBytes : Array[Byte] = Bytes.toBytes(imageID)
    imageidToBytes(imageidToBytes.length - 1) = (imageidToBytes(imageidToBytes.length - 1) + 1.toByte).toByte

    // check imageidToBytes value
    val scan : Scan = new Scan().withStartRow(Bytes.toBytes(imageID)).withStopRow(imageidToBytes)

    val rowkeyScan: ResultScanner = imageHistIndexHT.getScanner(scan)
    val rowkey : util.ArrayList[String] = new util.ArrayList[String]()
    for (r: Result <- rowkeyScan){
      rowkey.add(Bytes.toString(r.getValue(Bytes.toBytes("key"), Bytes.toBytes("value"))))
    }
    println("rowkey: " + util.Arrays.toString(rowkey.toArray))
    imageHistIndexHT.close()

    val imageHistHT : Table = conn.getTable(TableName.valueOf(Bytes.toBytes(imageHistHTName)))
    val imageHistory : util.ArrayList[String] = new util.ArrayList[String]()
    for (s:String <- rowkey){
      val g : Get = new Get(Bytes.toBytes(s))
      val r : Result = imageHistHT.get(g)
      imageHistory.add(Bytes.toString(r.getRow) + "|" + Bytes.toString(r.getValue(Bytes.toBytes("rundata"), Bytes.toBytes("value"))))
    }
    imageHistHT.close()

    /*****create an instance of imagepointProfile*****/
    var wdp : imagepointProfile = null
    var wdpBack : imagepointProfile = null

    if(domainID.contains("RMAC")){
      wdp = KL.createimagepointProfile("front", scale)
      wdpBack = KL.createimagepointProfile("back", scale)
    }
    else{
      wdp = KL.createimagepointProfile("all", scale)
      wdpBack = KL.createimagepointProfile("all", scale)
      wdpBack.pointList.clear()
    }

    /*****categorize x file with flag*****/
    var flag : Boolean = false
    var j : Int =0
    for (i<-0 until wdp.pointList.size()){
      j += Math.abs(wdp.pointList.get(i).classNumber)
    }
    if(wdp.inspdomainID.contains("KSP")){
      flag = true
      println("KSP domain")
    }else if (wdp.inspdomainID.contains("KT28") || wdp.inspdomainID.contains("KT29") ||
      wdp.inspdomainID.contains("KTPU") || wdp.inspdomainID.contains("KTBF") ||
      wdp.inspdomainID.contains("KMAC") || wdp.inspdomainID.contains("RMAC")){
      flag = true
      println("RDA prod domain")
    }

    if(j > 0 && !wdp.inspdomainID.contains("MAC")){
      println("Non-prodcution scan")
      flag = false
    }

    println(s"classnumber accumulation j: $j")
    println(s"inspdomainID: ${wdp.inspdomainID}")
    endTime = System.nanoTime()
    val hisTime : Long = (endTime - startTime)/1000000000
    println(s"Total time taken - image history & flag x files: $hisTime seconds")

    if(flag){
      println("go into analysis")
      /*****invoke iy algorithm*****/
      analyse(KL, wdp, "", iConf, conn, imageHistory, dOffset, hbaseNamespace, alltogether, historyDays, hdfsDataPath)
      analyse(KL, wdpBack, "B", iConf, conn, imageHistory, dOffset, hbaseNamespace, alltogether, historyDays, hdfsDataPath)
    }
    endTime = System.nanoTime()
    val analysisTime : Long = (endTime - startTime)/1000000000
    println(s"Total time taken - iy analysis: $analysisTime seconds")
    hbaseAdm.close()

    flag
  }

  def analyse(KL : fileExtractANTLR, wdp : imagepointProfile, side : String, iConf : iy.Configuration,
              conn: Connection, imageHistory : util.ArrayList[String], dOffset : Double, hbaseNamespace : String, alltogether : Int, historyDays : Int, hdfsDataPath : String): Unit = {
    println((if (side == "B") "Backside" else "Frontside") + s" rowkey: ${Bytes.toString(KL.getHbaseRowKey)}")
    println((if (side == "B") "Backside" else "Frontside") + s" point count: ${wdp.pointList.size}")
    wdp.analyse(iConf)
    if(side.equals(""))
      retrievetestXY(KL, wdp)
    addyInfo(KL, wdp, side)
    writeInfoToHBase(conn, KL, side, hbaseNamespace, alltogether)

    val circleSize : Int = wdp.circleList.size()
    val domainList : Array[String] = new Array[String](circleSize)
    val timeList : Array[String] = new Array[String](circleSize)
    val areaList : Array[String] = new Array[String](circleSize)

    if (circleSize > 0){
      var temp : Array[String] = Array[String]()
      var temp1 : Array[String] = Array[String]()
      var temp2 : Array[String] = Array[String]()
      val area : util.ArrayList[String] = new util.ArrayList[String]
      val domain : util.ArrayList[String] = new util.ArrayList[String]
      val d1 : util.ArrayList[Double] = new util.ArrayList[Double]
      val d2 : util.ArrayList[Double] = new util.ArrayList[Double]
      val rot : util.ArrayList[Double] = new util.ArrayList[Double]
      val robotArmFilePath : String = hdfsDataPath + "/RobotArmDimension.csv"

      val robotArmDimensionConf : Configuration = new Configuration()
      val path : Path = new Path(robotArmFilePath)
      val hadoop : FileSystem = FileSystem.get(robotArmDimensionConf)
      println(s"robot arm dimension csv path: $path")
      val bf : BufferedReader = new BufferedReader(new InputStreamReader(hadoop.open(path)))
      var record : String = bf.readLine()
      //continue in scala
      while (record != null) {
        temp = record.split(",")
        breakable{
          if (temp.length < 5 ||
          (temp.length > 13 && ((temp(13) == "FRONT" && side == "B") || (temp(13) == "BACK" && side == "")))){
          break()}
        }
        area.add(temp(1))
        domain.add(temp(4))
        try{
          if (temp.length > 8 && temp(8).length > 0)
            d1.add(temp(8).toDouble)
          else d1.add(-999999.0)
        }
        catch {
          case e: Exception => println("exception caught: " + e)
            d1.add(-999999.0)
        }

        try
            if (temp.length > 9 && temp(9).length > 0) d2.add(temp(9).toDouble)
            else d2.add(-999999.0)
        catch {
          case e: Exception => println("exception caught: " + e)
            d2.add(-999999.0)
        }

        if (temp.length > 12 && temp(12).contains("YES") && temp(12).split(" ").length > 1) {
          val tmp : String = temp(12).split(" ")(1).trim.replace("deg", "")
          try
              if (tmp.length > 0) rot.add(tmp.toDouble)
              else rot.add(9999.0)
          catch {
            case e: Exception => println("exception caught: " + e)
              rot.add(9999.0)
          }
        }
        else rot.add(9999.0)
        record = bf.readLine()
      }
      bf.close()
      hadoop.close()

      val imageHistdomain : util.ArrayList[String] = new util.ArrayList[String]
      val imageHistWS : util.ArrayList[String] = new util.ArrayList[String]
      val imageHistTime : util.ArrayList[String] = new util.ArrayList[String]
      val convertedTimestamp : String = KL.imageInfoMap.get("ConvertedTimestamp")

      for (s: String <- imageHistory){
        temp = s.split("\\|")
        temp1 = s.split("_")
        if (temp1(0).compareTo(convertedTimestamp) <= 0){
          temp2 = temp(1).split(",")
          imageHistWS.add(temp2(2))
          imageHistdomain.add(temp2(1))
          imageHistTime.add(temp1(0))
          println(temp1(0) + "    " + temp2(1) + "    " + temp2(2))
        }
      }
      println(s"imageHistdomain: ${imageHistdomain.toArray}")

      for (i<-0 until circleSize){
        val d : Double= wdp.circleList.get(i).linearReg.p.r
        val r : Double = wdp.circleList.get(i).linearReg.p.theta
        var domainlist : String = ""
        for (j<-0 until domain.size){
          if (Math.abs(d1.get(j) - d / 1000) <= dOffset || Math.abs(d2.get(j) - d / 1000) <= dOffset &&
            (rot.get(j) > 999.0 || Math.abs(rot.get(j) / 180.0 * Math.PI - r) < 0.3 ||
            Math.abs((if (rot.get(j) >= 180.0) rot.get(j) - 360.0 else rot.get(j)) / 180.0 * Math.PI + Math.PI - r) < 0.3))
            domainlist += "," + domain.get(j)
        }
        domainList(i) = ""
        timeList(i) = ""
        areaList(i) = ""
        for (j <- imageHistdomain.size - 1 to 0 by -1){
          if (domainlist.contains(imageHistdomain.get(j)) && !domainList(i).contains(imageHistdomain.get(j))){
            if (domainList(i).length > 0)
              domainList(i) += "," + imageHistdomain.get(j)
            else domainList(i) = imageHistdomain.get(j)
            if (timeList(i).length > 0)
              timeList(i) += "," + imageHistTime.get(j)
            else timeList(i) = imageHistTime.get(j)
            var t : Int = 0
            breakable(
              for (k <- 0 until domain.size){
                if (domain.get(k) == imageHistdomain.get(j)){
                  if (areaList(i).length > 0)
                    areaList(i) += "," + area.get(k)
                  else areaList(i) += area.get(k)
                  t = k
                  break
                }
              }
            )
            if (t == domain.size)
              if (areaList(i).length > 0)
                areaList(i) += ",null"
              else areaList(i) += "null"
          }
        }
      }

    }
    println(s"domainList to write into HBase y table: $domainList")
    println((if (side == "B") "Backside " else "Frontside ") + "Num yes: " + KL.imageSummaryMap.get(side + "NO_yES"))

    writeInfoToyHbase(KL, conn, domainList, timeList, areaList, wdp.circleList.size, side, hbaseNamespace)

    val htName : String = hbaseNamespace + ":y"
    val ht : Table = conn.getTable(TableName.valueOf(htName.getBytes))
    val p : Put = new Put(KL.getHbaseRowKey)

    if (wdp.circleList.size > 0) {
      for (k <- 0 until wdp.circleList.size()) {
        val out: ByteArrayOutputStream = new ByteArrayOutputStream
        ImageIO.write(wdp.drawimageMap(1, k, side), "jpg", out)
        p.addColumn(Bytes.toBytes("data"), Bytes.toBytes(side + "yImage" + (k + 1)), out.toByteArray)
      }
      finalConsolidation(conn, KL.getHbaseRowKey, side, dOffset, historyDays, hbaseNamespace)
    }

    p.addColumn(Bytes.toBytes("data"), Bytes.toBytes(side + "Email_Status"), Bytes.toBytes("0"))
    ht.put(p)
    ht.close()
  }

  def retrievetestXY(KL : fileExtractANTLR, wdp : imagepointProfile): Unit = {
    KL.headerList.add("testX")
    KL.headerList.add("testY")

    for (pointProf : pointProfile <- wdp.pointList) {
      /*
      scala array:     var z = Array("Zara", "Nuha", "Ayan")
      java array:      String[] cars = {"Volvo", "BMW", "Ford", "Mazda"};
      */
      val xyIndexTrans : Array[Int] = Array(pointProf.xIndexTrans, pointProf.yIndexTrans)
      val testXY : Array[Int] = wdp.pd.convertRDAXYtotestXY(xyIndexTrans)
      var a : String = KL.getpointList.get(pointProf.pointIndex)
      a = a.concat(" " + testXY(0) + " " + testXY(1))
      KL.pointList.set(pointProf.pointIndex, a)
    }
  }

  def addyInfo(KL: fileExtractANTLR, wdp: imagepointProfile, side: String): Unit = {
    val yes : Int = wdp.circleList.size
    KL.imageSummaryMap.put(side + "NO_yES", "" + yes)
    if (yes > 0)
    {
      for (x <-0 until yes) {
        val y_s : String = "THETA:" + wdp.circleList.get(x).linearReg.p.theta + ",R:" + wdp.circleList.get(x).linearReg.p.r
        KL.imageSummaryMap.put(side + "y" + (x + 1), y_s)
      }
    }
  }

  def writeInfoToHBase(conn : Connection, KL : fileExtractANTLR, side : String, hbaseNamespace : String, alltogether : Int):Unit = {
    val hbaseAdmin : Admin = conn.getAdmin

    try {
      val htName : String = hbaseNamespace + ":" +
        (if(alltogether.equals(1)) "alldevice" else KL.imageInfoMap.get ("DeviceID"))

      if (!hbaseAdmin.isTableAvailable(TableName.valueOf(htName.getBytes))) {
        val xHTB : TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(htName))
        val cfb1 : ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("image_info".getBytes)
        val cfb2 : ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("point_info".getBytes)
        val cfb3 : ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("image_summary".getBytes)
        val cf1 : ColumnFamilyDescriptor = cfb1.build
        val cf2 : ColumnFamilyDescriptor= cfb2.build
        val cf3 : ColumnFamilyDescriptor= cfb3.build
        cfb1.setTimeToLive(31104000)
        cfb2.setTimeToLive(31104000)
        cfb3.setTimeToLive(31104000)
        xHTB.setColumnFamily(cf1)
        xHTB.setColumnFamily(cf2)
        xHTB.setColumnFamily(cf3)
        val xHT : TableDescriptor = xHTB.build()
        hbaseAdmin.createTable(xHT)
        hbaseAdmin.close()
      }

      writeimageInfo(htName, KL, conn)
      writepointInfo(htName, KL, conn)
      writeSummaryInfo(htName, KL, conn, side)
      println("Finish writing information to HBase")
    } catch {
      case e: Exception => println("exception caught: " + e)
        println("ERROR in FILE!!!!!!")
    }
  }

  def writeimageInfo (htName : String, KL : fileExtractANTLR, conn : Connection): Unit ={
    println("writing image info to HBase...")
    val ht : Table = conn.getTable(TableName.valueOf(htName.getBytes()))

    val put : Put = new Put(KL.getHbaseRowKey)

    val hcfam  : Array[Byte] = "image_info".getBytes()
    var hcol : Array[Byte] = null
    var hval : Array[Byte] = null
    /*
    In Java:
    Map.Entry<String, String> entry
    */

    for (entry <- KL.imageInfoMap.entrySet) {
      hcol = entry.getKey.getBytes
      hval = entry.getValue.getBytes
      put.addColumn(hcfam, hcol, hval)
    }
    ht.put(put)
    ht.close()
  }

  def writepointInfo(htName : String, KL : fileExtractANTLR, conn : Connection): Unit ={
    println("creating points list...")
    val pointMap : util.HashMap[String, String] = new util.HashMap[String, String]

    for (x <- 0 until KL.getpointList.size){
      val point_p : Array[String] = KL.getpointList.get(x).trim.split("[\\s]+", KL.getHeaderList.size)
      var point : String = ""

      for (y <- point_p.indices){
        if (y > 0){
          point += ","
        }
        point += KL.getHeaderList.get(y) + ":" + point_p(y)
      }

      pointMap.put("point" + point_p(0), point)
    }
    println("writing points to HBase...")
    val ht : Table = conn.getTable(TableName.valueOf(htName.getBytes()))
    var put : Put = new Put(KL.getHbaseRowKey)

    val point_i = pointMap.keySet.iterator
    var curpoint : String = null
    //ensures Put object doesn't get to large. This is to avoid GC problems and timeouts caused by network latency

    var count : Int = 0
    while (point_i.hasNext){
      curpoint = point_i.next()
      put.addColumn("point_info".getBytes, curpoint.getBytes, pointMap.get(curpoint).getBytes)
      count += 1

      if (count == 1000){
        ht.put(put)
        put = new Put(KL.getHbaseRowKey)
        count = 0
      }
    }

    if (count != 0){
      ht.put(put)
    }
    ht.close()
  }

  def writeSummaryInfo (htName : String, KL : fileExtractANTLR, conn : Connection, side : String) : Unit ={
    println("writing summary to hbase...")
    val ht : Table = conn.getTable(TableName.valueOf(htName.getBytes))
    val put : Put = new Put(KL.getHbaseRowKey)
    val hcfam : Array[Byte] = "image_summary".getBytes
    var hcol : Array[Byte] = null
    var hval : Array[Byte] = null

    for (rec: String <- KL.imageSummaryMap.keySet){
      hcol = rec.getBytes
      hval = KL.imageSummaryMap.get(rec).getBytes
      if (side == "" || (side == "B" && (rec.contains("By") || rec.contains("BNO_yES"))))
        put.addColumn(hcfam, hcol, hval)
    }
    ht.put(put)
    ht.close()
  }

  def writeInfoToyHbase(KL : fileExtractANTLR, conn : Connection, domainList : Array[String],
                              timeList : Array[String], areaList : Array[String], circleNo : Int, side : String, hbaseNamespace : String): Unit = {
    val hbaseAdm : Admin = conn.getAdmin
    try{
      val htName: String = hbaseNamespace + ":y"
      if (!hbaseAdm.isTableAvailable(TableName.valueOf(htName.getBytes))) {
        val xHT : TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(htName))
        val cf : ColumnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes)
        val cfd : ColumnFamilyDescriptor = cf.build

        cf.setTimeToLive(31104000)
        xHT.setColumnFamily(cfd)
        val xHTD : TableDescriptor = xHT.build
        hbaseAdm.createTable(xHTD)
        hbaseAdm.close()
      }
      println("writing y info to y hbase...")
      val ht: Table = conn.getTable(TableName.valueOf(htName.getBytes))
      val p: Put = new Put(KL.getHbaseRowKey)

      if (circleNo > 0) {
        p.addColumn(Bytes.toBytes("data"), Bytes.toBytes(side + "Email_Status"), Bytes.toBytes("1"))
      }
      val hcfam : Array[Byte] = "data".getBytes
      var hcol : Array[Byte] = null
      var hval : Array[Byte] = null
      var i = KL.imageSummaryMap.keySet.iterator
      var l : Int = 0
      while (i.hasNext) {
        val rec : String = i.next
        if (rec.toString.contains(side + "y") || rec.toString.contains(side + "NO_y")){
          hcol = rec.getBytes()
          hval = KL.imageSummaryMap.get(rec).getBytes
          p.addColumn(hcfam, hcol, hval)
        }
        if (rec.toString.matches(side + "NO_yES"))
          l = KL.imageSummaryMap.get(rec).toString.toInt
      }

      for (j<- 0 until l){
        hcol = new Text(side + "domain_LIST" + (j + 1)).toString.getBytes
        hval = domainList(j).getBytes
        p.addColumn(hcfam, hcol, hval)
        hcol = new Text(side + "TIMESTAMP_LIST" + (j + 1)).toString.getBytes
        hval = timeList(j).getBytes
        p.addColumn(hcfam, hcol, hval)
        hcol = new Text(side + "AREA_LIST" + (j + 1)).toString.getBytes
        hval = areaList(j).getBytes
        p.addColumn(hcfam, hcol, hval)
      }

      i = KL.imageInfoMap.keySet.iterator
      while (i.hasNext) {
        val rec : String = i.next
        if (rec.toString.contains("DeviceID") || rec.toString.contains("InspectionStationID")) {
          hcol = rec.toString.getBytes
          hval = KL.imageInfoMap.get(rec).getBytes
          p.addColumn(hcfam, hcol, hval)
        }
      }

      ht.put(p)
      ht.close()
      println("Finish writing into y table")
    } catch {
      case e: Exception => println("exception caught: " + e)
        println("ERROR in y INFOR !!!!!!!!!!!")
    }
  }

  def finalConsolidation(conn : Connection, rowkey : Array[Byte], side : String, dOffset : Double, historyDays : Int, hbaseNamespace : String): Unit={
    println(Bytes.toString(rowkey))
    domainMatching.domain_list(conn, Bytes.toString(rowkey), dOffset * 1000, dOffset * 1000, 0.3, 0.3, historyDays, side, hbaseNamespace)
  }

}
