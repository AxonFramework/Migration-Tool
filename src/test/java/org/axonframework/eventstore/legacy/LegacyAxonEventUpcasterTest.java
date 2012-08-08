/*
 * Copyright (c) 2010-2011. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.eventstore.legacy;

import org.dom4j.Document;
import org.dom4j.io.STAXEventReader;
import org.junit.*;

import java.io.StringReader;

import static org.junit.Assert.*;

/**
 * @author Allard Buijze
 */
public class LegacyAxonEventUpcasterTest {

    private static final String NEW_SKOOL_MESSAGE =
            "<org.axonframework.eventstore.legacy.LegacyAxonEventUpcasterTest_-TestEvent eventRevision=\"0\">"
                    + "<metaData><values>"
                    + "<entry><string>_timestamp</string><localDateTime>2010-09-15T21:43:01.000</localDateTime></entry>"
                    + "<entry><string>_identifier</string><uuid>36f20a77-cdba-4e63-8c02-825486aad301</uuid></entry>"
                    + "</values></metaData>"
                    + "<sequenceNumber>0</sequenceNumber>"
                    + "<aggregateIdentifier>62daf7f6-c3ab-4179-a212-6b1da2a6ec72</aggregateIdentifier>"
                    + "<name>oldskool</name>"
                    + "<date>2010-09-15T00:00:00.000+02:00</date>"
                    + "<dateTime>2010-09-15T21:43:01.078+02:00</dateTime>"
                    + "<period>PT0.100S</period>"
                    + "</org.axonframework.eventstore.legacy.LegacyAxonEventUpcasterTest_-TestEvent>";
    private static final String NEW_SKOOL_MESSAGE_WITH_ADDITIONAL_METADATA =
            "<org.axonframework.eventstore.legacy.LegacyAxonEventUpcasterTest_-TestEvent eventRevision=\"0\">"
                    + "<metaData><values>"
                    + "<entry><string>_timestamp</string><localDateTime>2010-09-15T21:43:01.000</localDateTime></entry>"
                    + "<entry><string>_identifier</string><uuid>36f20a77-cdba-4e63-8c02-825486aad301</uuid></entry>"
                    + "<entry><string>someKey</string><string>someValue</string></entry>"
                    + "</values></metaData>"
                    + "<sequenceNumber>0</sequenceNumber>"
                    + "<aggregateIdentifier>62daf7f6-c3ab-4179-a212-6b1da2a6ec72</aggregateIdentifier>"
                    + "<name>oldskool</name>"
                    + "<date>2010-09-15T00:00:00.000+02:00</date>"
                    + "<dateTime>2010-09-15T21:43:01.078+02:00</dateTime>"
                    + "<period>PT0.100S</period>"
                    + "</org.axonframework.eventstore.legacy.LegacyAxonEventUpcasterTest_-TestEvent>";
    private static final String OLD_SKOOL_MESSAGE =
            "<org.axonframework.eventstore.legacy.LegacyAxonEventUpcasterTest_-TestEvent>"
                    + "<timestamp>2010-09-15T21:43:01.000</timestamp>"
                    + "<eventIdentifier>36f20a77-cdba-4e63-8c02-825486aad301</eventIdentifier>"
                    + "<sequenceNumber>0</sequenceNumber>"
                    + "<aggregateIdentifier>62daf7f6-c3ab-4179-a212-6b1da2a6ec72</aggregateIdentifier>"
                    + "<name>oldskool</name>"
                    + "<date>2010-09-15T00:00:00.000+02:00</date>"
                    + "<dateTime>2010-09-15T21:43:01.078+02:00</dateTime>"
                    + "<period>PT0.100S</period>"
                    + "</org.axonframework.eventstore.legacy.LegacyAxonEventUpcasterTest_-TestEvent>";
    private LegacyAxonEventUpcaster testSubject;

    @Before
    public void setUp() {
        testSubject = new LegacyAxonEventUpcaster();
    }

    /**
     * Test to make sure that events created during the time events did not have an explicit MetaData object can still
     * be read.
     *
     * @throws java.io.UnsupportedEncodingException
     *
     */
    @Test
    public void testUpcastOldStyleEvent() throws Exception {
        Document result = testSubject.upcast(new STAXEventReader().readDocument(new StringReader(OLD_SKOOL_MESSAGE)));
        assertEquals("0", result.getRootElement().attributeValue("eventRevision"));
        assertEquals("62daf7f6-c3ab-4179-a212-6b1da2a6ec72", result.getRootElement().element("aggregateIdentifier").getTextTrim());
        assertTrue(result.asXML().contains("<entry><string>_timestamp</string><localDateTime>2010-09-15T21:43:01.000"));
    }

    /**
     * Test to make sure that new events created can be read.
     *
     * @throws java.io.UnsupportedEncodingException
     *
     */
    @Test
    public void testDeserializeNewStyleEvent() throws Exception {
        Document result = testSubject.upcast(new STAXEventReader().readDocument(new StringReader(NEW_SKOOL_MESSAGE)));
        assertEquals("0", result.getRootElement().attributeValue("eventRevision"));
    }
}