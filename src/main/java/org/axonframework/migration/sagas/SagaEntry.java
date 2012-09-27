package org.axonframework.migration.sagas;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;

@Entity
@SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
public class SagaEntry {

    @Id
    private String sagaId;
    @Basic
    private String sagaType;
    @Basic
    private String revision;
    @Lob
    private byte[] serializedSaga;

    public byte[] getSerializedSaga() {
        return serializedSaga;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaType(String sagaType) {
        this.sagaType = sagaType;
    }
}
