package nl.knaw.dans.dataverse.bridge.plugin.dar.easy;

import nl.knaw.dans.dataverse.bridge.plugin.exception.BridgeException;
import nl.knaw.dans.dataverse.bridge.plugin.util.StateEnum;
import org.apache.abdera.model.Category;
import org.apache.abdera.model.Document;
import org.apache.abdera.model.Entry;
import org.apache.abdera.model.Feed;
import org.apache.abdera.parser.Parser;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public class EasyResponseDataHolder extends nl.knaw.dans.dataverse.bridge.plugin.common.ResponseDataHolder {
    private static final Logger LOG = LoggerFactory.getLogger(EasyResponseDataHolder.class);
    private String state;
    private String pid;
    private String landingPage;
    private String feedXml;

    public EasyResponseDataHolder(InputStream content) throws BridgeException {
        init(content);
    }

    @Override
    public void init(InputStream inputStream) throws BridgeException {
        try {
            feedXml = IOUtils.toString(inputStream, "UTF-8");
            //Eko says, we can validate the feedXml by using javax.xml.validation.Validator
            //But this simple check is enough for now.
            if (feedXml == null || feedXml.isEmpty() ) {
                LOG.error("feedXml is null or empty");
                throw new BridgeException("init() - feed is null or empty", this.getClass());
            }
            LOG.info(feedXml);
        } catch (IOException e) {
            throw new BridgeException(e.getMessage(), e, this.getClass());
        }
        Parser parser = getInstance().getParser();
        Document<Feed> doc = parser.parse(new ByteArrayInputStream(feedXml.getBytes()));
        Feed feed = doc.getRoot();
        List<Category> categories = feed.getCategories("http://purl.org/net/sword/terms/state");
        if (categories.size() != 1)
            throw new BridgeException("Zero or multiples categories. Catagories size:  " + categories.size(), this.getClass());
        else {
            Category category = categories.get(0);
            state = category.getTerm();
            if (state.equals(StateEnum.ARCHIVED.toString())){
                List<Entry> entries = feed.getEntries();
                if (entries.size() != 1) {
                    throw new BridgeException("Categories size is not equals 1. Size: " + categories.size(), this.getClass());
                } else {
                    Entry entry = entries.get(0);
                    landingPage = entry.getLink("self").getHref().toString();
                    pid = entry.getLink("self").getHref().getPath().replaceFirst("/", "");
                }
            } else {
                String msg = "State is : " + state + " feed: " + feedXml;
                LOG.debug(msg);
            }
        }
    }

    @Override
    public String getState() {
        return state;
    }

    @Override
    public Optional<String> getPid() {
        return Optional.of(pid);
    }

    @Override
    public Optional<String> getLandingPage() {
        return Optional.of(landingPage);
    }

    @Override
    public Optional<String> getFeedXml() {
        return Optional.of(feedXml);
    }

}
