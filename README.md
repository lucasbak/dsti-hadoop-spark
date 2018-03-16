
# SPARK - HIVE - ELK Practical

## Step 1 - Get the dataset

* signin/singup to https://www.kaggle.com
* go in the dataset explorer
* download the following dataset: https://www.kaggle.com/daveianhickey/2000-16-traffic-flow-england-scotland-wales on your computer
* send the zip from your downloads in your home directory on the dsti_cluster
```bash
    scp ~/Downloads/1-6m-accidents-traffic-flow-over-16-years.zip lucas@ec2-52-209-205-181.eu-west-1.compute.amazonaws.com:/home/lucas/
```
* connect to your home on the dsti cluster
```bash
    ssh lucas@ec2-52-209-205-181.eu-west-1.compute.amazonaws.com
```
* extract sample data
```bash
    unzip 1-6m-accidents-traffic-flow-over-16-years.zip 
```

## Step 2 - Put the data inside hdfs

* connect to your home on the dsti cluster
```bash
    ssh lucas@ec2-52-209-205-181.eu-west-1.compute.amazonaws.com
```

* create a db_directorys
```bash
  hdfs dfs -mkdir dlk
  hdfs dfs -mkdir dlk/hive_db
  hdfs dfs -mkdir dlk/hive_db/accidents_dataflow
  hdfs dfs -mkdir dlk/staging_accidents_dataflow
```

* put the data file inside hdfs
```bash
  hdfs dfs -put ./accident*.csv dlk/staging_accidents_dataflow/
```

* verify the data is here
```bash
  hdfs dfs -ls dlk/staging_accidents_dataflow
```

## Step 3 - Prepare the Hive Schema

* connect to your home on the dsti cluster
```bash
    ssh lucas@ec2-52-209-205-181.eu-west-1.compute.amazonaws.com
```

* connect hiveserver2
Note: double quotes are mandatory for the url
```bash
    beeline -u "jdbc:hive2://ec2-34-240-171-186.eu-west-1.compute.amazonaws.com:2181,ec2-52-212-76-132.eu-west-1.compute.amazonaws.com:2181,ec2-52-209-205-181.eu-west-1.compute.amazonaws.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n lucas
```

* create DB & TABLE
```bash
    # database creation
    CREATE DATABASE IF NOT EXISTS lucas_db LOCATION '/user/lucas/hive_db' ;
    # table create
    CREATE TABLE IF NOT EXISTS lucas_db.accidents_dataflow (Accident_Index STRING,Location_Easting_OSGR STRING,Location_Northing_OSGR STRING,Longitude STRING,Latitude STRING,Police_Force STRING,Accident_Severity STRING,Number_of_Vehicles INT,Number_of_Casualties INT,AccidentDate  STRING,Day_of_Week STRING,Time STRING,Local_Authority_District STRING,Local_Authority_Highway STRING,1st_Road_Class STRING,1st_Road_Number STRING,Road_Type STRING,Speed_limit STRING,Junction_Detail STRING,Junction_Control STRING,second_Road_Class STRING,second_Road_Number STRING,Pedestrian_CrossingHuman_Control STRING,Pedestrian_CrossingPhysical_Facilities STRING,Light_Conditions STRING,Weather_Conditions STRING,Road_Surface_Conditions STRING,Special_Conditions_at_Site STRING,Carriageway_Hazards STRING,Urban_or_Rural_Area STRING,Did_Police_Officer_Attend_Scene_of_Accident STRING,LSOA_of_Accident_Location STRING,Year STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION  '/user/lucas/hive_db/accidents_dataflow';
```

## Step 4 - Create a Spark Program To Load the Data Into Hive

* create a new Scala Program with SBT support 
 - scala version should be at least 2.10.5
 - read the following [documentation](https://www.scala-sbt.org/release/docs/) to install
 sbt on your computer
 - You can use maven if not easy with sbt
 - for example my build.sbt looks like
```
  name := "spark2-to-hive"
  version := "1.0"
  scalaVersion := "2.11.6"
```

* add the following lines inside `build.sbt` to have spark 2.2  Dependency(available on the cluster)

```
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
```

* write the code to parse one of the 3 csv files from staging_accidents_dataflow dir
example of main func
```scala
    val spark = SparkSession
      .builder()
      .appName("transformerLucasbak")
      .getOrCreate()

    // Create an RDD of Accidents objects from a text file, convert it to a Dataframe
    val peopleDF = spark.read.option("header", "false").csv("/user/lucas/dlk/staging_accidents_dataflow/accidents_2005_to_2007.csv")

    // Create the Table accidents_view
    peopleDF.write.saveAsTable("lucas_db.accidents_view")

```

* package the jar (you should have installed sbt previously) (local machine)
```bash
    sbt package
```

* send the jar to your home directory on the cluster
```bash
    scp target/scala-2.11/spark2-to-hive_2.11-1.0.jar lucas@ec2-52-209-205-181.eu-west-1.compute.amazonaws.com:/home/lucas/
```

* connect to the cluster (local machine)
```bash
    ssh lucas@ec2-52-209-205-181.eu-west-1.compute.amazonaws.com
```

* submit the job (remote machine)
```bash
    /usr/hdp/2.6.4.0-91/spark2/bin/spark-submit --master yarn --deploy-mode cluster --driver-memory 2048m --executor-memory 2048m  spark2-to-elastic_2.10-1.0.jar 
```

* check the status with the tracking application url. Example form is http://ec2-34-240-171-186.eu-west-1.compute.amazonaws.com:8088/proxy/application_1520860355522_0017/

* connect to hive again and visualise your data
```bash
    USE lucas_db;
    SELECT * from lucas_db.accidents_view limit 10;
```

* Well done you have written a Spark job running onna hadoop cluster, reading file from HDFS and writing to HIVE DB !

## Step 5 - Create a Spark Program To Load the Data Into Elasticsearch

* Create a new spark/scala project in order to load the data to Elasticsearch
Cou can reuse the precedent, just don't mess with configuration, code etc...

* Create a `assembly.sbt` file to put inside your  project folder and add inside
Check the [sbt-assembly documentation](https://github.com/sbt/sbt-assembly) to get the right version of sbt-assembly to use
and add the following line, for me it is
```
  addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")
```

* Add the following line inside your `build.sbt` file

```

libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "5.6.7"
// Jar Configuration
lazy val commonSettings = Seq(
  version := "0.1-spark2-to-elastic",
  organization := "com.adaltas.bakalian",
  scalaVersion := "2.11.4"
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    assemblyJarName in assembly := "spark2-to-elastic-fat.jar",
    mainClass in assembly := Some("Transformer")
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

```


* now from your project folder type (local machine)
```bash
    cd /home/lucas/spark-to-elastic
    sbt assembly
```
a new jar should be present inside target directly suffixed with `assembly-version`

* copy this jar to remote cluster (local machine)
```bash
    scp target/scala-2.12/spark2-to-elastic_2.11-1.0.jar lucas@ec2-52-209-205-181.eu-west-1.compute.amazonaws.com:/home/lucas/
```


## Step 6 - Create an elasticsearch index and mapping

* connect to the cluster (node elasticsearch) (local machine)

```bash
    ssh lucas@ec2-52-212-76-132.eu-west-1.compute.amazonaws.com 
```

* create and index with your name
```bash
    curl -XPUT '10.0.0.52:29000/lucas_index?pretty' -H 'Content-Type: application/json' -d'
      {
          "settings" : {
              "index" : {
                  "number_of_shards" : 3, 
                  "number_of_replicas" : 2 
              }
          }
      }
  '

```
You should have the following response
```


```

You can check with the following command
```bash
    curl 10.0.0.52:29000/_cat/indices?pretty=true
```

* create a mapping for accidents between 2005 and 2007

```



curl -XPUT '10.0.0.52:29000/lucas_index/_mapping/accidents_0507?pretty' -H 'Content-Type: application/json' -d'
{
  "properties": {
    "accident_index": {"type": "text"},
    "geolocation": {"type": "geo_point"},
    "number_of_vehicles": {"type": "text"},
    "number_of_casualties" :{"type": "text"},
    "date" : {"type": "date","format": "dd/MM/yyyy"},
    "year": {"type": "text"}

  }
}
'


```

## Step 7 - Populate Elasticsearch by submitting your application


* connect to your home on the dsti cluster
```bash
    ssh lucas@ec2-52-209-205-181.eu-west-1.compute.amazonaws.com
```

* submit your application
```
/usr/hdp/2.6.4.0-91/spark2/bin/spark-submit --master yarn --deploy-mode cluster --driver-memory 2048m --executor-memory 2048m  --class Transformer spark2-to-elastic-assembly-1.0.jar

```

