package nl.knaw.dans.dataverse.bridge.plugin.dar.easy;

import nl.knaw.dans.dataverse.bridge.plugin.common.*;
import nl.knaw.dans.dataverse.bridge.plugin.exception.BridgeException;
import nl.knaw.dans.dataverse.bridge.plugin.util.BridgeHelper;
import nl.knaw.dans.dataverse.bridge.plugin.util.StateEnum;
import org.apache.abdera.i18n.iri.IRI;
import org.apache.abdera.model.Entry;
import org.apache.abdera.model.Link;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/*
    @author Eko Indarto
 */
public class EasyIngestAction implements IAction {
    private ITransform iTransform = new EasyTransformer();
    private static final Logger LOG = LoggerFactory.getLogger(EasyIngestAction.class);
    private static final int timeout = 600000;
    private static final int chunkSize = 104857600;//100MB

    @Override
    public Map<String, String> transform(String ddiExportUrl, String apiToken, List<XslStreamSource> xslStreamSource) throws BridgeException {
        iTransform = new EasyTransformer();
        return iTransform.getTransformResult(ddiExportUrl, apiToken, xslStreamSource);
    }

    @Override
    public Optional<File> composeBagit(String bagitBaseDir, String apiToken, String ddiExportUrl, Map<String, String> transformedXml) throws BridgeException {
        LOG.info("Trying to compose bagit...");
        IBagitComposer iBagitComposer = new EasyBagComposer();
        DvFileList dvFileList = iTransform.getDvFileList(apiToken).get();
        File bagitFile = iBagitComposer.buildBag(bagitBaseDir, ddiExportUrl, transformedXml, dvFileList);
        return Optional.of(bagitFile);
    }

    @Override
    public EasyResponseDataHolder execute(Optional<File> baggitZippedFileOpt, IRI colIri, String uid, Optional<String> pwd) throws BridgeException {
        EasyResponseDataHolder easyResponseDataHolder;
        long checkingTimePeriod = 5000;
        try {
            File bagitZippedFile = baggitZippedFileOpt.get();
            LOG.info("Trying to ingest '" + bagitZippedFile.getName());
            long bagitZippedFileSize = bagitZippedFile.length();
            LOG.info("Triying to get MD5 for " + bagitZippedFile.getAbsolutePath());
            LOG.info(bagitZippedFile.getName() + " has size: " + formatFileSize(bagitZippedFileSize));
            int numberOfChunks = 0;
            if (bagitZippedFileSize > chunkSize) {
                numberOfChunks = getNumberOfChunks(bagitZippedFileSize, chunkSize);
                LOG.info("The '" + bagitZippedFile.getName() + "' file will send to EASY in partly, " + numberOfChunks + " times, each " + formatFileSize(chunkSize));
            }

            DigestInputStream dis = getDigestInputStream(bagitZippedFile);

            CloseableHttpClient http = BridgeHelper.createHttpClient(colIri.toURI(), uid, pwd.get(), timeout);
            CloseableHttpResponse response = BridgeHelper.sendChunk(dis, chunkSize, "POST", colIri.toURI(), "bag.zip.1", "application/octet-stream", http,
                    chunkSize < bagitZippedFileSize);

            String bodyText = BridgeHelper.readEntityAsString(response.getEntity());
            if (response.getStatusLine().getStatusCode() != 201) {
                LOG.error("FAILED. Status = " + response.getStatusLine());
                LOG.error("Response body follows:");
                LOG.error(bodyText);
                throw new BridgeException("Status = " + response.getStatusLine() + ". Response body follows:" + bodyText, this.getClass());
            }
            LOG.info("SUCCESS. Deposit receipt follows:");
            LOG.info(bodyText);

            Entry receipt = BridgeHelper.parse(bodyText);
            Link seIriLink = receipt.getLink("edit");
            URI seIri = seIriLink.getHref().toURI();

            long remaining = bagitZippedFileSize - chunkSize;
            if (remaining > 0)
                LOG.info("Trying to ingest the remaining '" + formatFileSize(remaining));
            else
                LOG.info("Ingesting is finish.");
            int count = 2;
            numberOfChunks --;
            while (remaining > 0) {
                checkingTimePeriod += 2000;
                LOG.info("POST-ing chunk of {} to SE-IRI (remaining: {}) ... [{}]", formatFileSize(chunkSize), formatFileSize(remaining), numberOfChunks);
                response = BridgeHelper.sendChunk(dis, chunkSize, "POST", seIri, "bag.zip." + count++, "application/octet-stream", http, remaining > chunkSize);
                numberOfChunks --;
                remaining -= chunkSize;
                bodyText = BridgeHelper.readEntityAsString(response.getEntity());
                if (response.getStatusLine().getStatusCode() != 200) {
                    LOG.error("FAILED. Status = " + response.getStatusLine());
                    LOG.error("Response body follows:");
                    LOG.error(bodyText);
                } else {
                    LOG.info("[bag.zip.{}] SUCCESS. Deposit receipt follows:", count);
                    LOG.info(bodyText);
                }
            }
            LOG.info("****** '" + bagitZippedFile.getName() + "' file is ingested. Now, check the EASY sword process state....");
            LOG.info("Retrieving Statement IRI (Stat-IRI) from deposit receipt ...");
            receipt = BridgeHelper.parse(bodyText);
            Link statLink = receipt.getLink("http://purl.org/net/sword/terms/statement");
            IRI statIri = statLink.getHref();
            LOG.info("Stat-IRI = " + statIri);
            easyResponseDataHolder = trackDeposit(http, statIri.toURI(), checkingTimePeriod, bagitZippedFile.getName());
            LOG.info(easyResponseDataHolder.getState());
        } catch (FileNotFoundException e) {
            LOG.error("FileNotFoundException: " + e.getMessage());
            throw new BridgeException("execute - FileNotFoundException, msg: " + e.getMessage(), e, this.getClass());
        } catch (NoSuchAlgorithmException e) {
            LOG.error("NoSuchAlgorithmException: " + e.getMessage());
            throw new BridgeException("execute - NoSuchAlgorithmException, msg: " + e.getMessage(), e, this.getClass());
        } catch (URISyntaxException e) {
            LOG.error("URISyntaxException: " + e.getMessage());
            throw new BridgeException("execute - URISyntaxException, msg: " + e.getMessage(), e, this.getClass());
        } catch (IOException e) {
            throw new BridgeException("execute - IOException, msg: " + e.getMessage(), e, this.getClass());
        }
        return easyResponseDataHolder;
    }

    private int getNumberOfChunks(long filesize, long chunkSize) {
        int numberOfChunk = 0;
        if ((filesize%chunkSize) != 0)
            numberOfChunk = 1;

        int x = (int) Math.floorDiv(filesize, chunkSize);

        return (numberOfChunk + x);
    }

    private String formatFileSize(long size) {
        String hrSize;

        double b = size;
        double k = size/1024.0;
        double m = ((size/1024.0)/1024.0);
        double g = (((size/1024.0)/1024.0)/1024.0);
        double t = ((((size/1024.0)/1024.0)/1024.0)/1024.0);

        DecimalFormat dec = new DecimalFormat("0.00");

        if ( t>1 ) {
            hrSize = dec.format(t).concat(" TB");
        } else if ( g>1 ) {
            hrSize = dec.format(g).concat(" GB");
        } else if ( m>1 ) {
            hrSize = dec.format(m).concat(" MB");
        } else if ( k>1 ) {
            hrSize = dec.format(k).concat(" KB");
        } else {
            hrSize = dec.format(b).concat(" Bytes");
        }

        return hrSize;
    }

    private DigestInputStream getDigestInputStream(File bagitZipFile) throws FileNotFoundException, NoSuchAlgorithmException {
        FileInputStream fis = new FileInputStream(bagitZipFile);
        MessageDigest md = MessageDigest.getInstance("MD5");
        return new DigestInputStream(fis, md);
    }

    private EasyResponseDataHolder trackDeposit(CloseableHttpClient http, URI statUri, long checkingTimePeriod, String filename) throws BridgeException {
        //filename is just needed for logging convenient especially when a lot of ingest process in the same time.
        EasyResponseDataHolder easyResponseDataHolder;
        CloseableHttpResponse response;
        LOG.info("Checking Time Period: " + checkingTimePeriod + " milliseconds.");
        LOG.info("Start polling Stat-IRI for the current status of the deposit, waiting {} milliseconds before every request ...", checkingTimePeriod);
        while (true) {
            try {
                Thread.sleep(checkingTimePeriod);
                LOG.info("Checking deposit status ... of " + filename);
                response = http.execute(new HttpGet(statUri));
                if (response.getStatusLine().getStatusCode() != 200)
                    throw new BridgeException("Status code != 200.  statUri: " + statUri, this.getClass());

                easyResponseDataHolder = new EasyResponseDataHolder(response.getEntity().getContent());
                String state = easyResponseDataHolder.getState();
                LOG.info("[{}] Response state from EASY: {}", filename, state);
                if (state.equals(StateEnum.ARCHIVED.toString()) || state.equals(StateEnum.INVALID.toString())
                        || state.equals(StateEnum.REJECTED.toString()) || state.equals(StateEnum.FAILED.toString()))
                    return easyResponseDataHolder;
            } catch (InterruptedException e) {
                throw new BridgeException("InterruptedException ", e, this.getClass());
            } catch (ClientProtocolException e) {
                throw new BridgeException("ClientProtocolException ", e, this.getClass());
            } catch (IOException e) {
                throw new BridgeException("IOException ", e, this.getClass());
            }
        }
    }
}
