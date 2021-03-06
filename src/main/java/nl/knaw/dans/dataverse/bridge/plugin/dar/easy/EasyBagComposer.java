package nl.knaw.dans.dataverse.bridge.plugin.dar.easy;

import gov.loc.repository.bagit.Bag;
import gov.loc.repository.bagit.BagFactory;
import gov.loc.repository.bagit.Manifest;
import gov.loc.repository.bagit.PreBag;
import gov.loc.repository.bagit.transformer.impl.ChainingCompleter;
import gov.loc.repository.bagit.transformer.impl.DefaultCompleter;
import gov.loc.repository.bagit.transformer.impl.TagManifestCompleter;
import net.lingala.zip4j.exception.ZipException;
import nl.knaw.dans.dataverse.bridge.plugin.bagit.BagInfoCompleter;
import nl.knaw.dans.dataverse.bridge.plugin.common.DvFileList;
import nl.knaw.dans.dataverse.bridge.plugin.common.IBagitComposer;
import nl.knaw.dans.dataverse.bridge.plugin.exception.BridgeException;
import nl.knaw.dans.dataverse.bridge.plugin.util.BridgeHelper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/*
    @author Eko Indarto
 */
public class EasyBagComposer implements IBagitComposer {
    private static final Logger LOG = LoggerFactory.getLogger(EasyBagComposer.class);
    private Path bagitDir;
    private Path bagTempDir;
    private static long MAX_FILESIZE = 2147483648L;
    @Override
    public File buildBag(String bagitBaseDir, String srcExportedUrl, Map<String, String> transformedXml, DvFileList dvFileList) throws BridgeException {
        LOG.info("Build bag....  Bagit base dir: " + bagitBaseDir);
        bagTempDir = createTempDirectory(bagitBaseDir);
        Path metadataDir = createMetadataDir();
        createDdiAndJsonXml(srcExportedUrl);
        createDatasetXmlFile(metadataDir, transformedXml.get("dataset.xml"));
        createFilesXmlFile(metadataDir, transformedXml.get("files.xml"));
        downloadFiles(dvFileList);
        composeBagit();
        return createBagitZip();
    }

    private Path createMetadataDir() throws BridgeException {
        LOG.info("bagitDir: " + bagitDir);
        LOG.info("bagitDir absoluth path " + bagitDir.toAbsolutePath());
        Path metadataDir = Paths.get(bagitDir + "/metadata");
        try {
            Files.createDirectories(metadataDir);
        } catch (IOException e) {
            throw new BridgeException("buildEasyBag - Files.createDirectories, msg: " + e.getMessage(), e, this.getClass());
        }
        return metadataDir;
    }

    //log uses '****' just to make easy to read the log.
    private void createDdiAndJsonXml(String ddiEportUrl) throws BridgeException {
        try {
            String ddiFilename = getExportedDvFilename(ddiEportUrl,"xml");
            LOG.info("**** Creating DDI file. Filename: " + ddiFilename + ". From " + ddiEportUrl);
            FileUtils.copyURLToFile(new URL(ddiEportUrl), new File(bagTempDir + "/data/" + ddiFilename));
            LOG.info("**** Creating " + ddiFilename + " is done.");
            LOG.info("**** Creating JSON file from " + ddiEportUrl);
            String jsonFilename = getExportedDvFilename(ddiEportUrl, "json");
            LOG.info("**** Creating JSON file. Filename: " + jsonFilename + ". From " + ddiEportUrl);
            //json: http://ddvn.dans.knaw.nl:8080/api/datasets/:persistentId/?persistentId=hdl:12345/JLO8HN
            FileUtils.copyURLToFile(new URL(ddiEportUrl.replace("export?exporter=ddi&", ":persistentId/?"))
                    ,  new File(bagTempDir + "/data/" + jsonFilename));
            LOG.info("**** Creating " + jsonFilename + " is done.");
        } catch (IOException e) {
            throw new BridgeException("Error creating during creating DDI and Json xml ", e, this.getClass());
        }
    }
    /*
    How can I throw CHECKED exceptions from inside Java 8 streams?
    Oracle messed it up.
    They cling on the concept of checked exceptions, but inconsistently forgot to take care of checked exceptions when designing the functional interfaces, streams, lambda etc.
    see: https://stackoverflow.com/questions/27644361/how-can-i-throw-checked-exceptions-from-inside-java-8-streams

     This method will throw runtime exception.
     */

    /*
    public boolean downloadFiles(Map<String, String> restrictedFiles, String apiToken, Map<String, String> publicFiles) throws IOException {
        try {
            publicFiles.forEach((k, v) -> {
                LOG.info("Start Download file: " + v + " to file: " + k);
                try {
                    FileUtils.copyURLToFile(new URL(v),  new File(bagTempDir + "/data/" + k));
                } catch (IOException e) {
                    throwWrapped(e);
                }
                LOG.info("Download file: " + k + " is finish");
            });

            restrictedFiles.forEach((k, v) -> {
                LOG.info("Start Download file: " + v + " to file: " + k);
                try {
                    FileUtils.copyURLToFile(new URL(v + "?key=" + apiToken), new File(bagTempDir + "/data/" + k));
                } catch (IOException e) {
                    throwWrapped(e);
                }
                LOG.info("Download file: " + k + " is finish");
            });

        }
        catch (BridgeException w) {
            throw (IOException) w.cause;
        }

        return false;
    }*/



    private void downloadFiles(DvFileList dvFileList) throws BridgeException {

        for (Map.Entry<String, String> publicFile : dvFileList.getPublicFiles().entrySet()){
            try {
                LOG.info("**** Starting download public file '" + publicFile.getKey() + " from " + publicFile.getValue());
                Instant start = Instant.now();
                File downloadedPublicFile = new File(bagTempDir + "/data/" + publicFile.getKey());
                FileUtils.copyURLToFile(new URL(publicFile.getValue()),  downloadedPublicFile);
                Instant finish = Instant.now();
                long timeElapsed = Duration.between(start, finish).getSeconds();
                LOG.info("**** Download public file "+ publicFile.getKey() + " is done in " + timeElapsed + " seconds.");
                if (downloadedPublicFile.length() > MAX_FILESIZE){
                    String msg = publicFile.getKey() + " (" + publicFile.getValue() +") has size more than 2G. Actual size: " + downloadedPublicFile.length() + " bytes.";
                    LOG.error(msg);
                    throw new BridgeException(msg, this.getClass());
                }
            } catch (IOException e) {
                throw new BridgeException("[downloadFiles] Public File. URL: " + publicFile.getValue()
                        + " File name: " + publicFile.getKey() + "; errror msg: " + e.getMessage(), e, this.getClass());
            }
        }
        for (Map.Entry<String, String> restrictedFile : dvFileList.getRestrictedFiles().entrySet()){
            try {
                LOG.info("**** Starting download restricted file '" + restrictedFile.getKey() + " from " + restrictedFile.getValue());
                Instant start = Instant.now();
                File downloadedRestrictedFile = new File(bagTempDir + "/data/" + restrictedFile.getKey());
                FileUtils.copyURLToFile(new URL(restrictedFile.getValue() + "?key=" + dvFileList.getApiToken()), downloadedRestrictedFile);
                Instant finish = Instant.now();
                long timeElapsed = Duration.between(start, finish).getSeconds();
                LOG.info("**** Download public file "+ restrictedFile.getKey() + " is done in " + timeElapsed + " seconds.");
                if (downloadedRestrictedFile.length() > MAX_FILESIZE){
                    String msg = restrictedFile.getKey() + " (" + restrictedFile.getValue() + ") has size more than 2G. Actual size: " + downloadedRestrictedFile.length() + " bytes.";
                    LOG.error(msg);
                    throw new BridgeException(msg, this.getClass());
                }
            } catch (IOException e) {
                throw new BridgeException("[downloadFiles] - Restricted File. URL: " + restrictedFile.getValue()
                        + " File name: " + restrictedFile.getKey() + "; errror msg: " + e.getMessage(), e, this.getClass());
            }
        }
    }

    private void composeBagit() {
        BagFactory bf = new BagFactory();
        BagInfoCompleter bic = new BagInfoCompleter(bf);
        DefaultCompleter dc = new DefaultCompleter(bf);
        dc.setPayloadManifestAlgorithm(Manifest.Algorithm.SHA1);
        TagManifestCompleter tmc = new TagManifestCompleter(bf);
        tmc.setTagManifestAlgorithm(Manifest.Algorithm.SHA1);
        ChainingCompleter completer = new ChainingCompleter(dc, new BagInfoCompleter(bf), tmc);
        PreBag pb = bf.createPreBag(bagTempDir.toFile());
        pb.makeBagInPlace(BagFactory.Version.V0_97, false, completer);
        Bag b = bf.createBag(bagTempDir.toFile());
    }

    private File createBagitZip() throws BridgeException {
        LOG.info("Creating bagit zip: {}.zip.", bagTempDir.toFile().getAbsolutePath());
        File zipFile = new File(bagTempDir.toFile().getAbsolutePath() + ".zip");
        try {
            BridgeHelper.zipDirectory(bagTempDir.toFile(), zipFile);
            boolean bagDirIsDeleted = FileUtils.deleteQuietly(bagTempDir.toFile());
            if (bagDirIsDeleted)
                LOG.info("{} file is deleted.", bagTempDir.toFile().getAbsolutePath());
            else
                LOG.warn("Deleting {} file is failed.", bagTempDir.toFile().getAbsolutePath());

        } catch (ZipException e) {
            throw new BridgeException("createBagitZip is faild, msg: " + e.getMessage(), e, this.getClass());

        }
        LOG.info("{} is created.", zipFile.getName());
        return zipFile;
    }

//    static BridgeException throwBridge(Throwable t) {
//        throw new BridgeException(t);
//    }

    private String getExportedDvFilename(String ddiEportUrl, String ext) {
        return (ddiEportUrl.split("persistentId=")[1])
                .replace(":","-")
                .replace("/","-") + "." + ext;
    }
    private void createDatasetXmlFile(Path metadataDir, String datasetXml) throws BridgeException {
        File datasetXmlFile = new File(metadataDir + "/dataset.xml");
        try {
            boolean newXmlFileIsCreated = datasetXmlFile.createNewFile();
            if (!newXmlFileIsCreated)
                throw new BridgeException("Failed to create dataset.xml file", this.getClass());
            Files.write(datasetXmlFile.toPath(), datasetXml.getBytes());
        } catch (IOException e) {
            String msg = "createDatasetXmlFile, msg: " + e.getMessage();
            LOG.error("ERROR: " , msg);
            throw new BridgeException(msg, e, this.getClass());
        }
    }

    private void createFilesXmlFile(Path metadataDir, String filesXml) throws BridgeException {
        File filesXmlFile = new File(metadataDir + "/files.xml");
        try {
            Files.write(filesXmlFile.toPath(), filesXml.getBytes());
        } catch (IOException e) {
            String msg = "createFilesXmlFile, msg: " + e.getMessage();
            LOG.error(msg);
            throw new BridgeException(msg, e, this.getClass());
        }
    }

    private Path createTempDirectory(String baseDir) throws BridgeException {
        try {
            bagitDir = Files.createTempDirectory(Paths.get(baseDir), "bagit");
            return bagitDir;
        } catch (IOException e) {
            String msg = "createTempDirectory, msg: " + e.getMessage();
            LOG.error(msg);
            throw new BridgeException(msg, e, this.getClass());
        }
    }
}
