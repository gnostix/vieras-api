package gr.gnostix.api.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import word.api.interfaces.IDocument;
import word.w2004.Document2004;
import word.w2004.elements.BreakLine;
import word.w2004.elements.Heading1;
import word.w2004.style.HeadingStyle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by rebel on 27/1/15.
 */
public class Reporting {

    Logger logger = LoggerFactory.getLogger(Reporting.class.getName());


    public File generateReport(Date dateFrom, Date dateTo, String exportFormat, String fullPath) {
        // a) get data
        // b) create figures
        // c) load template document
        // d) replace data and figures and create new document
        // d) redirect to download document

        final Date now = new Date();
        final SimpleDateFormat df = new SimpleDateFormat("EEE d MMM yyyy",Locale.ENGLISH);
        final String dateFromStr = df.format(dateFrom);
        final String dateToStr = df.format(dateFrom);
        String title = "AlxReport";

        final String documentFname = title.substring(0, title.length() > 25 ? 25 : title.length()).replaceAll(" ", "_")
                + "-" + "From_" + dateFromStr.replaceAll(" ", "_") + "_to_" + dateToStr.replaceAll(" ", "_");

        //final String documentFname = "AlxReport";
        final String documentFullPath = fullPath + "/" + documentFname;

        logger.info("---------------> Generating report '" + documentFname + "' to '" + documentFullPath);


        //JAVA2WORD
        IDocument myDoc = new Document2004();
        // myDoc.setPageOrientationLandscape();
        // default is Portrait be can be changed.
        myDoc.encoding(Document2004.Encoding.ISO8859_1); //or ISO8859-1. Default is UTF-8

        myDoc.addEle(BreakLine.times(1).create()); // this is one breakline

        // Headings
        myDoc.addEle(Heading1.with("Business Intelligence Report (" + dateFrom + "-" + dateTo + ")").withStyle()
                .align(HeadingStyle.Align.CENTER).create());
        File fileObj = new File(documentFullPath + ".doc");
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(fileObj);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        writer.println(myDoc);
        writer.close();

        return fileObj;

    }

}
