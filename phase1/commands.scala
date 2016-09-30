import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;


import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.GeometryFactory;
import org.datasyslab.geospark.spatialOperator.RangeQuery; 
import org.datasyslab.geospark.spatialRDD.PointRDD;
import com.vividsolutions.jts.geom.Envelope;

// 1st question
val queryEnvelope=new Envelope (-113.79,-109.73,32.99,35.08); 
val objectRDD = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv"); 
val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, 0).getRawPointRDD().count();

// 2nd question
val queryEnvelope=new Envelope (-113.79,-109.73,32.99,35.08); 
val objectRDD1 = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv"); 
objectRDD1.buildIndex("rtree");
val resultSize = RangeQuery.SpatialRangeQuery(objectRDD1, queryEnvelope, 0).getRawPointRDD().count();

//3rd question
val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-113.79, 35.08));
var result=KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 5);

//4th question
val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-113.79, 35.08));
var result=KNNQuery.SpatialKnnQueryUsingIndex(objectRDD1, queryPoint, 5);


//5th question
val objectRDD3 = new RectangleRDD(sc,"hdfs://rajesh:54310/home/dataset/zcta510.csv", 0,",");
