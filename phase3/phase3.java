import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkConf

public class Phase3 implements Serializable{

    public static float getis_OrdValue(float mean_val, float std_dev, int xcell, int ycell, int zcell, float count_in_cell)
    {
    	float spatial_weight = spatialWeight(xcell, ycell, zcell); 
    	float total_cells = 31 *( (40.9 - 40.5)/.01  * (74.25 - 73.7)/.01);
    	float g_val = (spatial_weight * count_in_cell - mean_val * spatial_weight);
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

}
