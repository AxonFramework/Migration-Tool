/*
 * Copyright (c) 2010-2012. Axon Framework
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

package org.axonframework.migration.eventstore;

import org.axonframework.eventstore.EventUpcaster;
import org.axonframework.serializer.SerializedDomainEventData;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * @author Allard Buijze
 */
public class JpaEventStoreMigrator {

    private static final int CONVERSION_BATCH_SIZE = 50;
    private static final int QUERY_BATCH_SIZE = 100000;
    private static final int MAX_BACKLOG_SIZE = QUERY_BATCH_SIZE / CONVERSION_BATCH_SIZE;

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private PlatformTransactionManager txManager;

    @Autowired
    private DomainEventEntryTransformer transformer;

    @Autowired
    @Qualifier("configuration")
    private Properties configuration;

    private TransactionTemplate txTemplate;

    private final ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(MAX_BACKLOG_SIZE);
    private final ExecutorService executor = new ThreadPoolExecutor(10, 20, 15,
                                                                    TimeUnit.SECONDS,
                                                                    workQueue,
                                                                    new ThreadPoolExecutor.CallerRunsPolicy());

    private List<EventUpcaster> upcasters;

    public JpaEventStoreMigrator(ApplicationContext context) {
        context.getAutowireCapableBeanFactory().autowireBean(this);
        txTemplate = new TransactionTemplate(txManager);
        txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        upcasters = new ArrayList<EventUpcaster>(context.getBeansOfType(EventUpcaster.class).values());
    }

    public boolean run() throws Exception {
        final AtomicInteger updateCount = new AtomicInteger();
        final AtomicInteger skipCount = new AtomicInteger();
        final AtomicLong lastId = new AtomicLong(Long.parseLong(configuration.getProperty("lastProcessedId", "-1")));
        try {
            TransactionTemplate template = new TransactionTemplate(txManager);
            template.setReadOnly(true);
            System.out.println("Starting conversion. Fetching batches of " + QUERY_BATCH_SIZE + " items.");
            while (template.execute(new TransactionCallback<Boolean>() {
                @Override
                public Boolean doInTransaction(TransactionStatus status) {
                    final Session hibernate = entityManager.unwrap(Session.class);
                    Iterator<Object[]> results = hibernate.createQuery(
                            "SELECT e.aggregateIdentifier, e.sequenceNumber, e.type, e.id FROM DomainEventEntry e "
                                    + "WHERE e.id > :lastIdentifier ORDER BY e.id ASC")
                                                          .setFetchSize(1000)
                                                          .setMaxResults(QUERY_BATCH_SIZE)
                                                          .setReadOnly(true)
                                                          .setParameter("lastIdentifier", lastId.get())
                                                          .iterate();
                    if (!results.hasNext()) {
                        System.out.println("Empty batch. Assuming we're done.");
                        return false;
                    } else if (Thread.interrupted()) {
                        System.out.println("Received an interrupt. Stopping...");
                        return false;
                    }
                    while (results.hasNext()) {
                        List<ConversionItem> conversionBatch = new ArrayList<ConversionItem>();
                        while (conversionBatch.size() < CONVERSION_BATCH_SIZE && results.hasNext()) {
                            Object[] item = results.next();
                            String aggregateIdentifier = (String) item[0];
                            long sequenceNumber = (Long) item[1];
                            String type = (String) item[2];
                            Long entryId = (Long) item[3];
                            lastId.set(entryId);
                            conversionBatch.add(new ConversionItem(sequenceNumber, aggregateIdentifier, type, entryId));
                        }
                        if (!conversionBatch.isEmpty()) {
                            executor.submit(new TransformationTask(conversionBatch, skipCount));
                        }
                    }
                    return true;
                }
            })) {
                System.out.println("Reading next batch, starting at ID " + lastId.get() + ".");
                System.out.println("Estimated backlog size is currently: " + (workQueue.size() * CONVERSION_BATCH_SIZE));
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
            if (lastId.get() >= 0) {
                System.out.println("Processed events from old event store up to (and including) id = " + lastId.get());
            }
        }
        System.out.println("In total " + updateCount.get() + " items have been converted.");
        return skipCount.get() == 0;
    }

    private class TransformationTask implements Runnable, TransactionCallback<Void> {

        private final List<ConversionItem> conversionItems;
        private final AtomicInteger skipCount;

        public TransformationTask(List<ConversionItem> conversionItems, AtomicInteger skipCount) {
            this.conversionItems = new ArrayList<ConversionItem>(conversionItems);
            this.skipCount = skipCount;
        }

        @Override
        public void run() {
            txTemplate.execute(this);
        }

        @Override
        public Void doInTransaction(TransactionStatus status) {
            try {
                for (ConversionItem conversionItem : conversionItems) {
                    long count = (Long)
                            entityManager.createQuery("SELECT count(e) FROM NewDomainEventEntry e "
                                                              + "WHERE e.aggregateIdentifier = :aggregateIdentifier "
                                                              + "AND e.sequenceNumber = :sequenceNumber "
                                                              + "AND e.type = :type")
                                         .setParameter("aggregateIdentifier", conversionItem.getAggregateIdentifier())
                                         .setParameter("type", conversionItem.getType())
                                         .setParameter("sequenceNumber", conversionItem.getSequenceNumber())
                                         .getSingleResult();
                    if (count != 0) {
                        return null;
                    }

                    DomainEventEntry entry = entityManager.find(DomainEventEntry.class, conversionItem.getEntryId());
                    SerializedDomainEventData newEntry = transformer.transform(entry.getSerializedEvent(),
                                                                               entry.getType(),
                                                                               entry.getAggregateIdentifier(),
                                                                               entry.getSequenceNumber(),
                                                                               entry.getTimeStamp(),
                                                                               upcasters);
                    if (newEntry != null) {
                        entityManager.persist(newEntry);
                    } else {
                        skipCount.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                skipCount.incrementAndGet();
            }
            return null;
        }
    }

    private static class ConversionItem {

        private final long sequenceNumber;
        private final String aggregateIdentifier;
        private final String type;
        private final long entryId;

        private ConversionItem(long sequenceNumber, String aggregateIdentifier, String type, long entryId) {
            this.sequenceNumber = sequenceNumber;
            this.aggregateIdentifier = aggregateIdentifier;
            this.type = type;
            this.entryId = entryId;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        public String getAggregateIdentifier() {
            return aggregateIdentifier;
        }

        public String getType() {
            return type;
        }

        public long getEntryId() {
            return entryId;
        }
    }
}
