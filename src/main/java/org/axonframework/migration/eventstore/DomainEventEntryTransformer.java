package org.axonframework.migration.eventstore;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.mapper.CannotResolveClassException;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.eventstore.EventUpcaster;
import org.axonframework.serializer.SerializedDomainEventData;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.STAXEventReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.xml.stream.XMLStreamException;

import static java.lang.String.format;

/**
 * @author Allard Buijze
 */
public class DomainEventEntryTransformer {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    @Autowired
    @Qualifier("identifierMapping")
    private Properties identifierMapping;

    @Autowired
    @Qualifier("configuration")
    private Properties configuration;

    @Autowired(required = false)
    private XStream xStream;

    private final Set<String> silencedIdentifiers = new ConcurrentSkipListSet<String>();

    public SerializedDomainEventData transform(byte[] serializedEvent, String aggregateType,
                                               String aggregateIdentifier, long sequenceNumber, String timeStamp,
                                               List<EventUpcaster> upcasters) throws XMLStreamException {
        final byte[] payload = serializedEvent;
        if (payload != null) {
            Document eventPayload = new STAXEventReader().readDocument(new InputStreamReader(
                    new ByteArrayInputStream(payload), UTF_8));
            for (EventUpcaster upcaster : upcasters) {
                if (Document.class.equals(upcaster.getSupportedRepresentation())) {
                    eventPayload = (Document) upcaster.upcast(eventPayload);
                }
            }
            final Element rootElement = eventPayload.getRootElement();
            String newIdentifierName = getIdentifier(rootElement.getName());
            if (newIdentifierName == null || "".equals(newIdentifierName)) {
                if (Boolean.parseBoolean(configuration.getProperty("autoResolveIdentifier"))) {
                    newIdentifierName = guessNewIdentifierName(rootElement);
                }
                if (newIdentifierName == null || "".equals(newIdentifierName)) {
                    if (silencedIdentifiers.add(rootElement.getName())) {
                        System.out.println(format("No identifier mapping available for [%s]", rootElement.getName()));
                    }
                    return null;
                }
            }
            NewDomainEventEntry newEntry = new NewDomainEventEntry(aggregateType, aggregateIdentifier, sequenceNumber,
                                                                   timeStamp);
            final Element metaData = rootElement.element("metaData");
            final String payloadType = rootElement.getName();
            final String payloadRevision = rootElement.attributeValue("eventRevision");

            newEntry.setPayloadType(payloadType);
            newEntry.setPayloadRevision(payloadRevision);

            rootElement.remove(metaData);
            rootElement.remove(rootElement.element("sequenceNumber"));
            rootElement.remove(rootElement.attribute("eventRevision"));
            rootElement.element("aggregateIdentifier").setName(newIdentifierName);

            newEntry.setPayload(rootElement.asXML().getBytes(UTF_8));
            metaData.setName("meta-data");
            final Element values = metaData.element("values");
            Iterator<Element> it = values.elementIterator();
            while (it.hasNext()) {
                final Element metaDataEntry = it.next();
                // each entry has 2 child elements
                Element keyElement = (Element) metaDataEntry.elements().get(0);
                Element valueElement = (Element) metaDataEntry.elements().get(1);
                if ("_identifier".equals(keyElement.getTextTrim())) {
                    newEntry.setEventIdentifier(valueElement.getTextTrim());
                } else if (!"_timestamp".equals(keyElement.getTextTrim())) {
                    values.remove(metaDataEntry);
                    metaData.add(metaDataEntry);
                }
            }
            metaData.remove(values);
            newEntry.setMetaData(metaData.asXML().getBytes(UTF_8));
            return newEntry;
        }
        return null;
    }

    private String guessNewIdentifierName(Element rootElement) {
        Class clazz;
        try {
            clazz = xStream.getMapper().realClass(rootElement.getName());
        } catch (NoClassDefFoundError e) {
            return null;
        } catch (CannotResolveClassException e) {
            return null;
        }
        Iterable<Field> fields = ReflectionUtils.fieldsOf(clazz);
        TreeSet<String> candidates = new TreeSet<String>();
        for (Field field : fields) {
            if (!Modifier.isTransient(field.getModifiers())
                    && !Modifier.isStatic(field.getModifiers())
                    && rootElement.element(field.getName()) == null) {
                candidates.add(field.getName());
            }
        }

        if (candidates.size() == 1) {
            return candidates.first();
        } else {
            if (silencedIdentifiers.add(rootElement.getName())) {
                System.out.println(format(
                        "Unknown identifier name for event of type [%s]. There is more than one candidate: %s. "
                                + "Make sure there is a mapping for it in identifiers.properties",
                        rootElement.getName(),
                        candidates));
            }
            return null;
        }
    }

    private String getIdentifier(String payloadType) {
        return identifierMapping.getProperty(payloadType);
    }
}
