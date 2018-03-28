package worker;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class CompressImage implements RequestHandler<S3Event, String> {
	
	

    private static AmazonS3Client s3;
    private static DynamoDB dynamodb;
	private static String tableName = "guestbook-charu";
    Map<String,Regions> regionmap;
    Properties prop;
    public CompressImage() throws IOException
    {
    super();
     s3 = new AmazonS3Client();
    
     }
   public String handleRequest(S3Event s3event, Context context) {

    
		// TODO Auto-generated method stub
		
           try {
        	   //Hint: Implement code for handling incomming S3 Events
        	   S3EventNotificationRecord record = s3event.getRecords().get(0);
               String srcBucket = record.getS3().getBucket().getName();
                String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
               srcKey = URLDecoder.decode(srcKey, "UTF-8");
             
               CompressImage.startCompress(srcBucket,srcKey);
              LambdaLogger logger = context.getLogger();
              logger.log("Compressed");
                
           } catch (Exception e) {
   			// TODO Auto-generated catch block
   			e.printStackTrace();
   		}
            	
            	
		return null;
		}


            public static  void startCompress(String srcBucket,String srcKey)
            {
            try
            {
             
                
            	//Hint: Here you need to pull s3 image object after that compress the image.
            	//Hint: Implement code for uploading it back to  folder inside s3 bucket
				
            	 S3Object object=CompressImage.pullobject(srcBucket,srcKey);
                 System.out.println("S3Object: "+object.getKey());
                 File compressedfile=CompressImage.convert(object, srcKey);
                 srcKey = FilenameUtils.getName(srcKey);
                 CompressImage.pushobject(srcBucket,srcKey, compressedfile);
                
              
              
            }
            catch(Exception e)
            {
              
              System.out.println("1. Exception occured with trace"+e.getMessage());
              
            }



            }

            public static S3Object pullobject(String srcBucket,String filename)
            {
            S3Object object = null;
            try
            {
              System.out.println("Uploadbucket: "+srcBucket +" " + filename);
              object =s3.getObject(new GetObjectRequest(srcBucket,filename));
              System.out.println(object);
            }
            catch(Exception e)
            {
              System.out.println("2.Exception occured with trace"+e.getMessage());
              
            }
            return object;


            }


            public static File convert( S3Object object,String filename)
            {
            	  File compressedImageFile = null;
            	try{

            		 
            		 S3ObjectInputStream objectContent = object.getObjectContent();
            		  filename = FilenameUtils.getName(filename);
            		  IOUtils.copy(objectContent, new FileOutputStream("//tmp//"+filename));
            		    File input = new File("//tmp//"+filename);
            		      BufferedImage image = ImageIO.read(input);
            		      compressedImageFile = new File("//tmp//"+filename);
            		      OutputStream os =new FileOutputStream(compressedImageFile);

            		      Iterator<ImageWriter>writers =  ImageIO.getImageWritersByFormatName("jpg");
            		      ImageWriter writer = (ImageWriter) writers.next();

            		      ImageOutputStream ios = ImageIO.createImageOutputStream(os);
            		      writer.setOutput(ios);

            		      ImageWriteParam param = writer.getDefaultWriteParam();
            		      
            		      param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            		      param.setCompressionQuality(0.05f);
            		      writer.write(null, new IIOImage(image, null, null), param);
            		      
            		      os.close();
            		      ios.close();
            		      writer.dispose();
            	}catch(IOException e){
            		System.out.println(e);
            	}
				return compressedImageFile;



            }




            public static void pushobject(String srcBucket,String filename,File compressedImageFile)
            {
            try
            {
              System.out.println("Uploadbucket: "+srcBucket);
              s3.putObject(new PutObjectRequest(srcBucket,"converted/"+filename,compressedImageFile).withCannedAcl(CannedAccessControlList.PublicRead));
              
            }

            catch(Exception e)
            {
            	System.out.println("4.Exception occured with trace\n"+e+"\n"+e.getMessage());
              
            }

         }
       
}
