package org.axonframework.migration.eventstore;

import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Lob;

/**
 * @author Allard Buijze
 */
@SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
@Entity
@IdClass(DomainEventEntry.PK.class)
public class DomainEventEntry {

    @Id
    @Column(updatable = false)
    private String type;
    @Id
    @Column(updatable = false)
    private String aggregateIdentifier;
    @Id
    @Column(updatable = false)
    private long sequenceNumber;
    @Basic(optional = false)
    private String eventIdentifier;
    @Basic(optional = false)
    @Column(updatable = false)
    private String timeStamp;
    @Basic(optional = false)
    private String payloadType;
    @Basic
    private String payloadRevision;
    @Basic
    @Lob
    private byte[] metaData;
    @Basic(optional = false)
    @Lob
    private byte[] payload;
    @Column(updatable = false)
    @Basic
    @Lob
    private byte[] serializedEvent;

    public byte[] getSerializedEvent() {
        return serializedEvent;
    }

    public void setEventIdentifier(String eventIdentifier) {
        this.eventIdentifier = eventIdentifier;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    public void setPayloadRevision(String payloadRevision) {
        this.payloadRevision = payloadRevision;
    }

    public void setMetaData(byte[] metaData) {
        this.metaData = metaData;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    /**
     * Primary key definition of the AbstractEventEntry class. Is used by JPA to support composite primary keys.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static class PK implements Serializable {

        private static final long serialVersionUID = 9182347799552520594L;

        private String aggregateIdentifier;
        private String type;
        private long sequenceNumber;

        /**
         * Constructor for JPA. Not to be used directly
         */
        PK() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PK pk = (PK) o;

            if (sequenceNumber != pk.sequenceNumber) {
                return false;
            }
            if (!aggregateIdentifier.equals(pk.aggregateIdentifier)) {
                return false;
            }
            if (!type.equals(pk.type)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = aggregateIdentifier.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "PK{" +
                    "type='" + type + '\'' +
                    ", aggregateIdentifier='" + aggregateIdentifier + '\'' +
                    ", sequenceNumber=" + sequenceNumber +
                    '}';
        }
    }
}
