import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkConf

public class Phase3 implements Serializable{


    public static float standardDev(float mean, float sum_x_squared) 
    {
    	float total_cells = 31 *( (40.9 - 40.5)/.01  * (74.25 - 73.7)/.01); 

    	float standDev = Math.sqrt((sum_x_squared / total_cells ) - Math.pow(mean,2));
    	return standDev;
    }

    public static float getis_OrdValue(float mean_val, float std_dev, float spatial_weight, float x_val)
    {
    	float total_cells = 31 *( (40.9 - 40.5)/.01  * (74.25 - 73.7)/.01);
    	float g_val = (spatial_weight * x_val - mean_val * spatial_weight);
    	//bottom half eq calc 
    	float under_sqrt_eq = (total_cells * (spatial_weight) - Math.pow(spatial_weight,2) )/(total_cells - 1);
    	float tmp_bottom_eq = std_dev * Math.sqrt(under_sqrt_eq);
    	//now combine top and bottom to get g value 
    	g_val = g_val / tmp_bottom_eq;
    	return g_val;
    }

    public static flaot meanValue(int total_pts)
    {
    	float total_cells = 31 *( (40.9 - 40.5)/.01  * (74.25 - 73.7)/.01);
    	float mean = total_pts/ total_cells; 

    	return mean;
    }


    public static int spatialWeight(int x, int y, int z)
    {
    	int neighbor_val = 0;
    	//it is the first or last layer in the time cube 
    	if(z == 1 || z == 31)
    	{
    		//check if it is a corner cube
    		if( (x == 0 && y == 0) || (x ==39 && y == 0) || (x == 0 && y == 54) || (x == 39 && y == 54 ))
    			neighbor_val = 7;
    		else if( (x == 0 && y != 0) || (x ==39 && y != 0) || (x == 0 && y != 54) || (x == 39 && y =! 54 ) )
    			neighbor_val =11;
    		else
    			neighbor_val = 17; 

    	}
    	//else it is a stacked layer in the time cube - i.e. has a top and bottom layer enclosing it
    	else
    	{
    		//check if it is a corner cube
    		if( (x == 0 && y == 0) || (x ==39 && y == 0) || (x == 0 && y == 54) || (x == 39 && y == 54 ))
    			neighbor_val = 11;
    		else if( (x == 0 && y != 0) || (x ==39 && y != 0) || (x == 0 && y != 54) || (x == 39 && y =! 54 ) )
    			neighbor_val =17;
    		else
    			neighbor_val = 26;  
    	}
    	return neighbor_val;
    }


    //this function will be used on one day of data - need to add the data to this function delceration 
    public static float[][] groupCellData(PointRDD objectRDD)
    {
    	float[][] nyc_map_day = new float[40][55];
    	int cell_y = 0;
    	int cell_x = 0;
    	//constants for cell boundires 
    	float latitude_min = 40.5;
    	float latitude_max = 40.9;
    	float longitude_max = -73.7;
    	float longitude_min = -74.25;
    	float cell_length = .01;

    	float lat_boundry = 0.0;
    	float long_boundry = 0.0;

    	int total_cells = 0; 

    	//mean Value 
    	float mean_Value = 0.0;

    	//standard dev values 
    	float x_squared_sum = 0.0;
    	float standard_Dev_value =0.0;

    	//for each latitude itterate over all cells
    	for(int i=latitude_min; i <= (latitude_max - cell_length); i=i+cell_length)
    	{
    		//itterate all longitude blocks 
    		for(int j=longitude_min; j <= (longitude_max - cell_length); j=j+cell_length)
    		{
    			lat_boundry = i + cell_length;
    			long_boundry = j + cell_length;
    			//collect all values within the cell blk
    			//set boundry envelope 
    			val queryEnvelope=new Envelope (long_boundry,j,i,lat_boundry);
    			//query for the values within the envelope 
    			val points_in_cell = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, 0).getRawPointRDD().count();
    			mean_Value += points_in_cell;
    			//square and sum the value for this cell - used for standard dev 
    			x_squared_sum += Math.pow(points_in_cell,2);

    			//need to store the g -value in the nyc_map_day array ??????????

    			// spatial weight calc ??????????

    			total_cells++;
    			cell_y++;

    		} // end of inner for loop

    		total_cells++;
    		cell_x ++;
    		cell_y = 0;
    	}//end of for loop

    	//calcualte the mean value for all the cells 
    	mean_Value = mean_Value / total_cells;

    	//calc standard dev 
    	standard_Dev_value = standardDev(mean_Value,total_cells,x_squared_sum);

    	return nyc_map_day;

    }

}
