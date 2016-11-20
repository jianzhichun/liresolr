package net.semanticmetadata.lire.solr;

import net.semanticmetadata.lire.imageanalysis.*;
import net.semanticmetadata.lire.indexing.hashing.BitSampling;
import org.apache.commons.codec.binary.Base64;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.eclipse.jetty.util.security.Credential.MD5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.XPathEntityProcessor.URL;

/**
 * An entity processor like the one for Tika to support data base imports and alike
 * Special thanks to Giuseppe Becchi, who triggered the development, tested it and
 * found the location the critical bug
 *
 * @author Mathias Lux, mathias@juggle.at on 17.12.13.
 */
public class LireEntityProcessor extends EntityProcessorBase {
	private static final Logger logger = LoggerFactory.getLogger(LireEntityProcessor.class);
    protected boolean done = false;
    protected LireFeature[] listOfFeatures = new LireFeature[]{
            new ColorLayout(), new PHOG(), new EdgeHistogram(), new JCD(), new OpponentHistogram()
    };
    protected static HashMap<Class, String> classToPrefix = new HashMap<Class, String>(5);
    int count = 0;

    static {
        classToPrefix.put(ColorLayout.class, "cl");
        classToPrefix.put(EdgeHistogram.class, "eh");
        classToPrefix.put(PHOG.class, "ph");
        classToPrefix.put(OpponentHistogram.class, "oh");
        classToPrefix.put(JCD.class, "jc");
    }


    protected void firstInit(Context context) {
        super.firstInit(context);
        done = false;
    }

    /**
     * @return a row where the key is the name of the field and value can be any Object or a Collection of objects. Return
     * null to signal end of rows
     */
    public Map<String, Object> nextRow() {
        if (done) {
            done = false;
            return null;
        }
        Map<String, Object> row = new HashMap<String, Object>();
        DataSource<InputStream> dataSource = context.getDataSource();
        // System.out.println("\n**** " + context.getResolvedEntityAttribute(URL));
        InputStream is = dataSource.getData(context.getResolvedEntityAttribute(URL));
        String path = context.getResolvedEntityAttribute(URL);
        path = "http://localhost:8080/images/"+path.substring(path.lastIndexOf("bj400_90M")+9);
//        row.put("id", UUID.randomUUID().toString());
        row.put("id", UUID.randomUUID().toString());
        row.put("path", path);
        // here we have to open the stream and extract the features. Then we put them into the row object.
        // basically I hope that the entity processor is called for each entity anew, otherwise this approach won't work.
        try {
            BufferedImage img = ImageIO.read(is);
//            row.put("id", context.getResolvedEntityAttribute(URL));
            for (int i = 0; i < listOfFeatures.length; i++) {
                LireFeature feature = listOfFeatures[i];
                feature.extract(img);
                String histogramField = classToPrefix.get(feature.getClass()) + "_hi";
                String hashesField = classToPrefix.get(feature.getClass()) + "_ha";
                row.put(histogramField, Base64.encodeBase64String(feature.getByteArrayRepresentation()));
                row.put(hashesField, ParallelSolrIndexer.arrayToString(BitSampling.generateHashes(feature.getDoubleHistogram())));
            }
        } catch (IOException e) {
            wrapAndThrow(SEVERE, e, "Error loading image or extracting features.");
        }
        // if (count++>10)
        done = true;
        // dataSource.close();
        return row;
    }
}
