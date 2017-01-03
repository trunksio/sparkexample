import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.input.PortableDataStream;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.lept;
import org.bytedeco.javacpp.tesseract;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.List;

import static org.bytedeco.javacpp.lept.pixDestroy;

/**
 * Created by lewis on 26/12/16.
 */
public class ocrFromPdf {

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, PortableDataStream> pdfRDD = sparkContext.binaryFiles(args[1]);
        JavaPairRDD<String, String> xText = pdfRDD.mapValues(new ExtractText());
        xText.saveAsTextFile(args[2]);

    }
}
class ExtractText implements Function<PortableDataStream, String>{
    public String call(PortableDataStream p){

        StringBuffer text = new StringBuffer();

        tesseract.TessBaseAPI api = new tesseract.TessBaseAPI();
        // Initialize tesseract-ocr with English, without specifying tessdata path
        if (api.Init(null, "eng") != 0) {
            System.err.println("Could not initialize tesseract.");
            System.exit(1);
        }


       try {
           PDDocument document = PDDocument.load(new ByteArrayInputStream(p.toArray()));
           List<PDPage> list = document.getDocumentCatalog().getAllPages();
           System.out.println("Total files to be converted -> "+ list.size());


           int pageNumber = 1;
           for (PDPage page : list) {
               BytePointer outText;
               BufferedImage pdfImage = page.convertToImage();
               ByteArrayOutputStream baos = new ByteArrayOutputStream();
               ImageIO.write(pdfImage, "png", baos);
               byte[] imageInByte = baos.toByteArray();
               lept.PIX image = lept.pixReadMemPng(imageInByte, imageInByte.length);

               api.SetImage(image);
               // Get OCR result
               outText = api.GetUTF8Text();

               //System.out.println("OCR output:\n" + outText.getString());
               text.append(outText.getString());
               // Destroy used object and release memory

               outText.deallocate();
               pixDestroy(image);
           }
           }
        catch (Exception e){
            e.printStackTrace();
           }
        api.End();
        return text.toString();
    }


}