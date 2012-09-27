package org.axonframework.migration.sagas;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@SuppressWarnings("UnusedDeclaration")
@Entity
public class AssociationValueEntry {

    @Id
    @GeneratedValue
    private Long id;

    @Basic
    private String sagaId;

    @Basic
    private String associationKey;

    @Basic
    private String associationValue;

    @Basic
    private String sagaType;


}