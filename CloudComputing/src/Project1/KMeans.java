package Project1;
/*suryapraharsha
 * U00880191
 * maddijanakirama.2@wright.edu
 * 
 * 
 * 
 * 
 * 
 * */
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





public class KMeans {

    public static int _nrows;
    public static int[] _label = null;
    public static double[][] _centroids;
    public static int _ndims;
    public static int _drows;
    public double[][] _data;
    private int _numClusters;
    //Mapper class
    public static class KMap extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void setup(Mapper.Context con) throws IOException {

            LoadCentroids(con); 
            readrows(con); 
            
        }
        //reading centroid file using CSV helper
        	public static void LoadCentroids(Context con) throws IOException {
           //KMeans classob = new KMeans();
        	////Reading data and calculating the rows and columns
            BufferedReader reader;
            CSVHelper csv=new CSVHelper();
            ArrayList<String> values; 
            Configuration conf = con.getConfiguration();
            String filename = conf.get("centroid");
            new Path(filename);
            FileSystem.get(conf);
            try {
                reader = new BufferedReader(new FileReader(filename));
                _nrows =1;
                values = csv.parseLine(reader);
                
                _ndims = values.size();
                while(reader.readLine()!=null)
                _nrows++;
                
               
              
                reader.close();
                _centroids = new double[_nrows][_ndims]; // initialize the centroid 2-darray//
               
                String line;
                int counter = 0;
                reader = new BufferedReader(new FileReader(filename));
                while ((line = reader.readLine()) != null) {
                    String[] both = line.split("\\t");
                    String last=both[1];
                    
                    StringTokenizer cpoint=new StringTokenizer(last,",");
                    
                    int lcount=0;
                    while(cpoint.hasMoreTokens()){
                        double localdata=Double.parseDouble(cpoint.nextToken());
                        _centroids[counter][lcount]=localdata;
                        lcount++;
                    }
                       
                   
                    counter++;
                }
                
                for(int i=0;i<2;i++) {
                	for(int j=0;j<2;j++) {
                		System.out.printf("\n centroids[%d][%d]"+_centroids[i][j],i,j);
                	}
                }
                
                reader.close();
               

            } catch (Exception e) {
                
            }

        }
        // Reading from data.csv file using csv helper
        public static void readrows(Context con) throws IOException {
        	BufferedReader reader;
            CSVHelper csv =new CSVHelper();
            KMeans classob = new KMeans();
            Configuration conf = con.getConfiguration();
            String filename = conf.get("datafile");
            new Path(filename);
            FileSystem.get(conf);
     
                reader = new BufferedReader(new FileReader(filename));
               
                while(reader.readLine()!=null)
                    _drows++;
                reader.close();
           
                classob._data = new double[KMeans._drows][KMeans._ndims];
           
        }
        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            KMeans obj= new KMeans();
           
            String data = value.toString();
            System.out.println("data is "+data);
            String[] tempboth = data.split("\t");
            System.out.println("tempdata is "+tempboth[1]);
            StringTokenizer lastdata = new StringTokenizer(tempboth[1], ",");
            
            double[] initialdata = new double[_ndims];

            int num = 0;
            while (lastdata.hasMoreTokens()) {
                initialdata[num] = Double.parseDouble(lastdata.nextToken());
                num++;
            }
            
            int label = obj.closest(initialdata);
            System.out.println("labels are "+label);
            
            LongWritable fkey = new LongWritable(label);
            StringBuilder fvalue = new StringBuilder();
            for (int i = 0; i < initialdata.length; i++) {
                String local = Double.toString(initialdata[i]);
                if (i != initialdata.length - 1) {
                    fvalue.append(local).append(" ");
                }
                if (i == initialdata.length - 1) {
                    fvalue.append(local);
                }
            }

            Text fvalue2 = new Text();
            fvalue2.set(fvalue.toString());

            con.write(fkey, fvalue2);
        }
    }
    //Reducer class
    public static class KReduce extends Reducer<LongWritable, Text, LongWritable, Text> {

        protected void reduce(LongWritable localkey, Iterable<Text> mapdata, Context con)
                throws IOException, InterruptedException
        {
            double[] add_value = new double[_ndims];
            double[] ini_data = new double[_ndims];
            int c = 0;
            for (Text data : mapdata)
              {
                StringTokenizer tokens = new StringTokenizer(data.toString()," ");
                int count = 0;
                while (tokens.hasMoreTokens())
                {
                    String getdata = tokens.nextToken();
                    ini_data[count] = Double.parseDouble(getdata);
                    count++;
                }

                for (int k = 0; k < ini_data.length; k++) {
                    double getadd = ini_data[k] + add_value[k];
                    add_value[k] = getadd;
                    //System.out.println("add is "+add_value[k]);
                }

                c++;
             }

            double[] newCentroid = updateCentroid(add_value, c);    
            System.out.println(Arrays.toString(newCentroid));
            con.write(localkey,new Text(Arrays.toString(newCentroid)));

        }

        public static double[] updateCentroid(double[] add, int count) {
            double[] getcen = new double[add.length];
            for (int i = 0; i < add.length; i++) {
                double aver = add[i] / count;
                getcen[i] = aver;

            }
            return getcen;
        }
        

    }
    
    // num of clusters = num of centroids
    // find the closest centroid for the record v 
    private int closest(double[] v) {

        double mindist = dist(v, _centroids[0]);
        int label = 0;
        for (int i = 1; i < _nrows; i++) {
            double t = dist(v, _centroids[i]);
            if (mindist > t) {
                mindist = t;
                label = i;
            }
        }
        return label;
   
    }
    // compute Euclidean distance between two vectors v1 and v2
    private double dist(double[] v1, double[] v2) {
        double sum = 0;
        for (int i = 0; i < _ndims; i++) {
            double d = v1[i] - v2[i];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }
    public void clustering(int numClusters, int niter, double [][] centroids) 
    {
        _numClusters = numClusters;
        if (centroids !=null)
            _centroids = centroids;
        else{
          // randomly selected centroids
          _centroids = new double[_numClusters][];

          ArrayList idx= new ArrayList();
          for (int i=0; i<numClusters; i++){
            int c;
            do{
              c = (int) (Math.random()*_nrows);
            }while(idx.contains(c)); // avoid duplicates
            idx.add(c);

            // copy the value from _data[c]
            _centroids[i] = new double[_ndims];
            for (int j=0; j<_ndims; j++)
              _centroids[i][j] = _data[c][j];
          }
          System.out.println("selected random centroids");

        }

        double [][] c1 = _centroids;
        double threshold = 0.001;
        int round=0;

        while (true){
          // update _centroids with the last round results
          _centroids = c1;

          //assign record to the closest centroid
          _label = new int[_nrows];
          for (int i=0; i<_nrows; i++){
            _label[i] = closest(_data[i]);
          }
          
          // recompute centroids based on the assignments  
         
          round ++;
          if ((niter >0 && round >=niter) || converge(_centroids, c1, threshold))
            break;
        }

        System.out.println("Clustering converges at round " + round);
    }
    private boolean converge(double [][] c1, double [][] c2, double threshold){
        // c1 and c2 are two sets of centroids 
        double maxv = 0;
        for (int i=0; i< _numClusters; i++){
            double d= dist(c1[i], c2[i]);
            if (maxv<d)
                maxv = d;
        } 

        if (maxv <threshold)
          return true;
        else
          return false;
        
      }

    public int [] getLabel()
    {
      return _label;
    }
    public int nrows(){
      return _nrows;
    }   
 
    @SuppressWarnings("deprecation")
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        conf.set("centroid",args[2]);
        conf.set("datafile", args[0]);
        FileSystem fs= FileSystem.get(conf);
        int iteration=1;
        for(int i=0;i<iteration;i++){
        Job job = new Job(conf, "KMeans");
        
        job.setMapperClass(KMap.class);
        job.setReducerClass(KReduce.class);
        
        job.setJarByClass(KMap.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileUtil.fullyDelete(fs,new Path(args[1]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
       
        job.waitForCompletion(true);
        
        }
    }

  
   
}