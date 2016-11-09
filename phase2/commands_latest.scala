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


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import java.io.PrintWriter;



var timeSleep = 180;

var forloopmin = 0;
var forloopmax = 100;


//question 1 
TimeUnit.SECONDS.sleep(timeSleep);
val now = Calendar.getInstance().getTime();
print("question1 starting:"+now+"\n");

val out=new PrintWriter("out1.txt");
out.println("question1 starting:"+now);

for( a<- forloopmin to  forloopmax ) {
val newPointRDD = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv"); 
}

val now = Calendar.getInstance().getTime()
print("question1 ending:"+now+"\n")
out.println("question1 ending:"+now+"\n")
out.flush();
out.close();

// 2a question
TimeUnit.SECONDS.sleep(timeSleep);
val now = Calendar.getInstance().getTime()
print("question2a starting:"+now)

val out=new PrintWriter("out2a.txt");
out.println("question2a starting:"+now);

for( a<- forloopmin to  forloopmax ) {
val queryEnvelope=new Envelope (-113.79,-109.73,32.99,35.08);
val objectRDD = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv"); 
val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, 0).getRawPointRDD().count();
}
val now = Calendar.getInstance().getTime()
print("question2a ending:"+now)
out.println("question2a ending:"+now+"\n")
out.flush();
out.close();

// 2b question
TimeUnit.SECONDS.sleep(timeSleep);
val now = Calendar.getInstance().getTime()
print("question2b starting:"+now)

val out=new PrintWriter("out2b.txt");
out.println("question2b starting:"+now);

for( a<- forloopmin to  forloopmax ) {
val queryEnvelope=new Envelope (-113.79,-109.73,32.99,35.08);
val objectRDD1 = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv"); 
objectRDD1.buildIndex("rtree");
val resultSize = RangeQuery.SpatialRangeQueryUsingIndex(objectRDD1, queryEnvelope, 0).getRawPointRDD().count();
}
val now = Calendar.getInstance().getTime()
print("question2b ending:"+now)
out.println("question2b ending:"+now+"\n")
out.flush();
out.close();

//3a question
TimeUnit.SECONDS.sleep(timeSleep);
val now = Calendar.getInstance().getTime()
print("question3a starting:"+now)

val out=new PrintWriter("out3a.txt");
out.println("question3a starting:"+now);

for( a<- forloopmin to  forloopmax ) {
val objectRDD = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv");

val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-113.79, 35.08));
var result=KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 5);
}
val now = Calendar.getInstance().getTime()
print("question3a ending:"+now)

out.println("question3a ending:"+now+"\n")
out.flush();
out.close();


//3b question
TimeUnit.SECONDS.sleep(timeSleep);
val now = Calendar.getInstance().getTime()
print("question3b starting:"+now)
val out=new PrintWriter("out3b.txt");
out.println("question3b starting:"+now);

for( a<- forloopmin to  forloopmax ) {
println(a);
val objectRDD1 = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv"); 
objectRDD1.buildIndex("rtree");
val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-113.79, 35.08));
var result=KNNQuery.SpatialKnnQueryUsingIndex(objectRDD1, queryPoint, 5);
}
val now = Calendar.getInstance().getTime()
print("question3b ending:"+now)

out.println("question3b ending:"+now+"\n")
out.flush();
out.close();

//4a question
TimeUnit.SECONDS.sleep(timeSleep);
val now = Calendar.getInstance().getTime()
print("question4a starting:"+now)
val out=new PrintWriter("out4a.txt");
out.println("question4a starting:"+now);
for( a<- forloopmin to  9) {
println(a);
val objectRDD3 = new RectangleRDD(sc,"hdfs://rajesh:54310/home/dataset/zcta510.csv", 0,"csv","equalgrid",6);
val objectRDD = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv","equalgrid",6); 
val joinQuery = new JoinQuery(sc,objectRDD,objectRDD3);
val resultSize = joinQuery.SpatialJoinQuery(objectRDD,objectRDD3).count();
}
val now = Calendar.getInstance().getTime()
print("question4a ending:"+now)

out.println("question4a ending:"+now+"\n")
out.flush();
out.close();

//4b question
TimeUnit.SECONDS.sleep(timeSleep);
val now = Calendar.getInstance().getTime()
print("question4b starting:"+now)
val out=new PrintWriter("out4b.txt");
out.println("question4b starting:"+now);

for( a<- forloopmin to  9 ) {
println(a);
val objectRDD3 = new RectangleRDD(sc,"hdfs://rajesh:54310/home/dataset/zcta510.csv", 0,"csv","equalgrid",6);
val objectRDD = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv","equalgrid",6);
objectRDD.buildIndex("rtree");
val joinQuery = new JoinQuery(sc,objectRDD,objectRDD3);
val resultSize = joinQuery.SpatialJoinQueryUsingIndex(objectRDD,objectRDD3).count();
}
val now = Calendar.getInstance().getTime()
print("question4b ending:"+now)
out.println("question4b ending:"+now+"\n")
out.flush();
out.close();

//4c question
TimeUnit.SECONDS.sleep(timeSleep);
val now = Calendar.getInstance().getTime()
print("question4c starting:"+now)

val out=new PrintWriter("out4c.txt");
out.println("question4c starting:"+now);

for( a<- forloopmin to  9 ) {
println(a);
val objectRDD3 = new RectangleRDD(sc,"hdfs://rajesh:54310/home/dataset/zcta510.csv", 0,"csv");
val objectRDD = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv","rtree",6);
val joinQuery = new JoinQuery(sc,objectRDD,objectRDD3);
val resultSize = joinQuery.SpatialJoinQuery(objectRDD,objectRDD3).count();
}
val now = Calendar.getInstance().getTime()
print("question4c ending:"+now)

out.println("question4c ending:"+now+"\n")
out.flush();
out.close();


// cartesian product 
val now = Calendar.getInstance().getTime()
print("questiontask starting:"+now)
for( a<- forloopmin to  1 ) {
println(a);
val objectRDD = new PointRDD(sc, "hdfs://rajesh:54310/home/dataset/arealm.csv", 0, "csv","equalgrid", 10); /* The O means spatial attribute starts at Column 0 and the 10 means 10 RDD partitions */
val rectangleRDD = new RectangleRDD(sc, "hdfs://rajesh:54310/home/dataset/zcta510.csv", 0, "csv","equalgrid",10); /* The O means spatial attribute starts at Column 0. You might need to "collect" all rectangles into a list and do the Carteian Product join. */
val joinQuery = new JoinQuery(sc,objectRDD,rectangleRDD);
val resultSize = joinQuery.SpatialJoinQueryUsingCartesianProduct(objectRDD, rectangleRDD).count();
}
val now = Calendar.getInstance().getTime()
print("questiontask ending:"+now)
