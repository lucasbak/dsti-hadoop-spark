

import org.apache.spark.internal.Logging
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.elasticsearch.spark.sql._




object Transformer extends Logging {


  case class Accident(accident_index: String,geolocation: String, number_of_vehicles: String,number_of_casualties :String,
                      date : String,
                      year: String)

  def main(args : Array[String]): Unit = {

    //System.setProperty("hive.metastore.uris", "thrift://ec2-52-212-76-132.eu-west-1.compute.amazonaws.com:9083")
    /** Configuring SparkSession **/
    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[Accident].schema
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("transformerLucasbak")
      .config("es.index.auto.create","true")
      .config("es.nodes","ec2-52-212-76-132.eu-west-1.compute.amazonaws.com")
      .config("es.port","29000")
      .getOrCreate()
    import spark.implicits._
    /**************** start - full wide columns declaration ******************************/
    /*case class Accident(accident_index: String,location_easting_osgr: String,location_northing_osgr: String,longitude: String,
                        latitude: String,police_force: String,accident_severity: String,number_of_vehicles: Int,number_of_casualties :Int,
                        date : String,day_of_week: String,time: String,local_authority_district: String,local_authority_highway: String,
                        first_Road_Class: String,first_Road_Number: String,road_type: String,speed_limit: String,
                        junction_detail: String,junction_control: String,second_road_class: String,second_road_number: String,
                        pedestrian_crossinghuman_control: String,pedestrian_crossingphysical_facilities: String,light_conditions: String,
                        weather_conditions: String,road_surface_conditions: String,special_conditions_at_site: String,carriageway_hazards: String,
                        urban_or_rural_srea: String,did_police_officer_attend_scene_of_accident: String,lsoa_of_accident_location: String,
                        year: String)

    val esDF = lowerHeaderPeopleDF.map {
      case Row(accident_index: String,location_easting_osgr: String,location_northing_osgr: String,longitude: String,
      latitude: String,police_force: String,accident_severity: String,number_of_vehicles: Int,number_of_casualties :Int,
      date : String,day_of_week: String,time: String,local_authority_district: String,local_authority_highway: String,
      first_Road_Class: String,first_Road_Number: String,road_type: String,speed_limit: String,
      junction_detail: String,junction_control: String,second_road_class: String,second_road_number: String,
      pedestrian_crossinghuman_control: String,pedestrian_crossingphysical_facilities: String,light_conditions: String,
      weather_conditions: String,road_surface_conditions: String,special_conditions_at_site: String,carriageway_hazards: String,
      urban_or_rural_srea: String,did_police_officer_attend_scene_of_accident: String,lsoa_of_accident_location: String,
      year: String) => Accident(accident_index,location_easting_osgr,location_northing_osgr,longitude,
        latitude,police_force,accident_severity,number_of_vehicles,number_of_casualties,
        date ,day_of_week,time,local_authority_district,local_authority_highway,
        first_Road_Class,first_Road_Number,road_type,speed_limit,
        junction_detail,junction_control,second_road_class,second_road_number,
        pedestrian_crossinghuman_control,pedestrian_crossingphysical_facilities,light_conditions,
        weather_conditions,road_surface_conditions,special_conditions_at_site,carriageway_hazards,
        urban_or_rural_srea,did_police_officer_attend_scene_of_accident,lsoa_of_accident_location,
        year)
    }*/
    /**************** end - full wide columns declaration ******************************/
    import spark.implicits._


    /*val schema_2 = StructType(Seq(
      StructField("accident_index", StringType),
      StructField("geolocation", StringType),
      StructField("number_of_vehicles", StringType),
      StructField("number_of_casualties", StringType),
      StructField("date", StringType),
      StructField("year", StringType)
    ))

    val encoder = RowEncoder(schema)
    */
    /** Load Dataframe from HDFS **/
    // Create an RDD of Accidents objects from a text file, convert it to a Dataframe
    //val peopleDF = spark.read.option("header", "true").schema(schema)csv("/user/lucas/dlk/staging_accidents_dataflow/accidents_2005_to_2007.csv")
    val peopleDF = spark.read.option("header", "true").csv("/user/lucas/dlk/staging_accidents_dataflow/accidents_2005_to_2007.csv")

    /** LowerCase the headers **/
    val lowerHeaderPeopleDF = peopleDF.toDF(peopleDF.columns map(_.toLowerCase): _*)
      lowerHeaderPeopleDF.show(10)
    // Register the DataFrame as a temporary view
    //spark.sql("show databases").show()
    //peopleDF.write.saveAsTable("lucas_db.accidents_view")
    /** Elastic search mapping the id **/
    def esConfig(): Map[String,String] = {
      Map("es.mapping.id" -> "accident_index")
    }

    /**create a new column name geolocation from latitute and longitude **/
    //lowerHeaderPeopleDF.withColumn("geolocation", lowerHeaderPeopleDF("longitude")+"-"+lowerHeaderPeopleDF("latitude")).show()
    /** Create a sub convert to rdd */
    val lowerHeaderRDD = lowerHeaderPeopleDF.rdd
    // remove the header
    val esRDD = lowerHeaderRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    // map rdd line to accident object
    val esDF = esRDD.map(
      x => Accident(x.getString(0), x.getString(4)+","+x.getString(3),x.getString(7), x.getString(8),x.getString(9),x.getString(32))
    ).toDF()
    // save toes
    esDF.saveToEs("lucas_index/accidents_0507")

    /** map each row to the Accident Class for elasticdarch Mapping */
    /*val esDF = finalAccidentDF.map {

      //case Row(accident_index: String, geolocation: String, number_of_vehicles: Int, number_of_casualties: Int,
      //date: String, year: String) => Accident(accident_index, geolocation, number_of_vehicles, number_of_casualties,
      //  date,
      //  year)
      case Row(accident_index: String, geolocation: String, number_of_vehicles: Int, number_of_casualties: Int,
      date: String, year: String) => Accident(accident_index, geolocation, number_of_vehicles, number_of_casualties,
        date,
        year)
      //case row => Accident(row.getString(0), row.getString(1), row.getInt(2), row.getInt(3), row.getString(4), row.getString(5))
    }*/

    //val esDF = finalAccidentDF.map {

      //case Row(accident_index: String, geolocation: String, number_of_vehicles: Int, number_of_casualties: Int,
      //date: String, year: String) => Accident(accident_index, geolocation, number_of_vehicles, number_of_casualties,
      //  date,
      //  year)
      //case Row(accident_index: String, geolocation: String, number_of_vehicles: String, number_of_casualties: String,
      //date: String, year: String) => Accident(accident_index, geolocation, number_of_vehicles, number_of_casualties,
        //date,
        //year)
      //case row => row
      //case row => Accident(row.getString(0), row.getString(1), row.getInt(2), row.getInt(3), row.getString(4), row.getString(5))
    //}

    //esRDD.saveToEs("lucas_index/accidents_0507")
    /**  Write data to Elasticsearch Cluster */
     //EsSpark.saveToEs(esDF.rdd, "lucas_index/accidents_0507", esConfig)

    /*// create DataFrame from RDD (Programmatically Specifying the Schema) val headerColumns = rdd.first().split(",").to[List] // extract headers [..] first def dfSchema(columnNames: List[String]): StructType = { StructType( Seq( StructField(name = "manager_name", dataType = StringType, nullable = false), StructField(name = "client_name", dataType = StringType, nullable = false), StructField(name = "client_gender", dataType = StringType, nullable = false), StructField(name = "client_age", dataType = IntegerType, nullable = false), StructField(name = "response_time", dataType = DoubleType, nullable = false), StructField(name = "satisfaction_level", dataType = DoubleType, nullable = fals) ) ) } // create a data row def row(line: List[String]): Row = { Row(line(0), line(1), line(2), line(3).toInt, line(4).toDouble, line(5).toDouble) } // define a schema for the file val schema = dfSchema(headerColumns) val data = rdd .mapPartitionsWithIndex((index, element) => if (index == 0) it.drop(1) else it) // skip header .map(_.split(",").to[List]) .map(row) val dataFrame = spark.createDataFrame(data, schema)
    // create DataFrame from RDD (Programmatically Specifying the Schema)
    val headerColumns = esDF.rdd.first().split(",").to[List]
    // extract headers [esDF.rdd] first
    def dfSchema(columnNames: List[String]): StructType = {
      StructType( Seq(
        StructField(
          name = "manager_name",
          dataType = StringType,
          nullable = false),
        StructField(
          name = "client_name",
          dataType = StringType,
          nullable = false),
        StructField(
          name = "client_gender",
          dataType = StringType,
          nullable = false),
        StructField(
          name = "client_age",
          dataType = IntegerType,
          nullable = false),
        StructField(
          name = "response_time",
          dataType = DoubleType,
          nullable = false),
        StructField(
          name = "satisfaction_level",
          dataType = DoubleType,
          nullable = false) ) ) }
    // create a data row
    def row(line: List[String]): Row = { Row(line(0), line(1), line(2), line(3).toInt, line(4).toDouble, line(5).toDouble) }
    // define a schema for the file
    val schema = dfSchema(headerColumns)
    val data = rdd
      .mapPartitionsWithIndex((index, element) => if (index == 0) it.drop(1) else it) // skip header
      .map(_.split(",").to[List])
      .map(row)
    val dataFrame = spark.createDataFrame(data, schema)
    99
  */

  }

  }