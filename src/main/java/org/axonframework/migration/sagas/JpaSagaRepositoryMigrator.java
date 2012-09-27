package org.axonframework.migration.sagas;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author Allard Buijze
 */
public class JpaSagaRepositoryMigrator implements TransactionCallback<Boolean> {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private PlatformTransactionManager txManager;

    private TransactionTemplate txTemplate;

    public JpaSagaRepositoryMigrator(ApplicationContext context) {
        context.getAutowireCapableBeanFactory().autowireBean(this);
        this.txTemplate = new TransactionTemplate(txManager);
    }

    public void run() {
        while (txTemplate.execute(this)) {
            // next batch..
        }
    }

    @Override
    public Boolean doInTransaction(TransactionStatus status) {
        List<SagaEntry> sagaEntries = entityManager.createQuery("SELECT e FROM SagaEntry e WHERE e.sagaType is null")
                                                   .setMaxResults(1000)
                                                   .getResultList();
        if (sagaEntries.isEmpty()) {
            return false;
        }
        for (SagaEntry entry : sagaEntries) {
            byte[] serializedSaga = entry.getSerializedSaga();
            XMLStreamReader reader = null;
            try {
                reader = XMLInputFactory.newFactory()
                                        .createXMLStreamReader(new InputStreamReader(new ByteArrayInputStream(
                                                serializedSaga), Charset.forName("UTF-8")));
                while (!reader.isStartElement()) {
                    reader.next();
                }
                String sagaName = reader.getLocalName();
                entry.setSagaType(sagaName);
                entityManager.createQuery("UPDATE AssociationValueEntry e SET e.sagaType = :sagaType "
                                                  + "WHERE e.sagaId = :sagaId")
                             .setParameter("sagaType", sagaName)
                             .setParameter("sagaId", entry.getSagaId())
                             .executeUpdate();
            } catch (XMLStreamException e) {
                e.printStackTrace();
                return false;
            } finally {
                tryClose(reader);
            }
        }
        return true;
    }

    private void tryClose(XMLStreamReader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (XMLStreamException e) {
                // whatever
            }
        }
    }
}
