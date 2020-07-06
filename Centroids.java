import java.io.*;
import java.util.*;

public class Centroids {
    public static void main(String args[]) throws IOException{
        File P = new File("Centroids.csv");
        P.createNewFile();
        FileWriter fwp = new FileWriter("Centroids.csv");
        
        int numPoints = 50;
        for(int i=0;i<numPoints;i++){
            Random r = new Random();
            int xDimLow = 1000;
            int xDimHigh = 9000;
            int xDim = r.nextInt(xDimHigh - xDimLow) + xDimLow;

            int yDimLow = 1000;
            int yDimHigh = 9000;
            int yDim = r.nextInt(yDimHigh - yDimLow) + yDimLow;

            fwp.append(xDim+"");
            fwp.append(",");
            fwp.append(yDim+"");
            fwp.append("\n");
        }
        fwp.flush();
        fwp.close();
    }
}


