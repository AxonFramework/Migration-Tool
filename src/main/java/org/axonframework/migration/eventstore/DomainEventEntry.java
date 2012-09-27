package org.axonframework.migration.eventstore;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;

/**
 * @author Allard Buijze
 */
@SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
@Entity
public class DomainEventEntry {

    @Id
    @GeneratedValue
    private Long id;
    @Basic
    private String aggregateIdentifier;
    @Basic
    private long sequenceNumber;
    @Basic
    private String timeStamp;
    @Basic
    private String type;
    @Basic
    @Lob
    private byte[] serializedEvent;

    public Long getId() {
        return id;
    }

    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public String getType() {
        return type;
    }

    public byte[] getSerializedEvent() {
        return serializedEvent;
    }
}
