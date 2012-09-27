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

package org.axonframework.migration;

import org.axonframework.eventstore.EventUpcaster;
import org.axonframework.migration.eventstore.DomainEventEntry;
import org.axonframework.migration.eventstore.DomainEventEntryTransformer;
import org.axonframework.migration.eventstore.NewDomainEventEntry;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
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

    ExecutorService executor = new ThreadPoolExecutor(10,
                                                      10,
                                                      5,
                                                      TimeUnit.SECONDS,
                                                      new ArrayBlockingQueue<Runnable>(50),
                                                      new ThreadPoolExecutor.CallerRunsPolicy());

    private List<EventUpcaster> upcasters;

    public static void main(String[] args) throws Exception {
        try {
            ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
                    "/META-INF/spring/migration-config.xml",
                    "file:app-specific-context.xml");
            JpaEventStoreMigrator runner = new JpaEventStoreMigrator();
            runner.run(context);
            context.stop();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            while (System.in.available() > 0) {
                System.in.skip(System.in.available());
            }
            System.out.println("Press enter to quit");
            new Scanner(System.in).nextLine();
        }
    }

    private void run(ClassPathXmlApplicationContext context) throws Exception {
        context.getAutowireCapableBeanFactory().autowireBean(this);
        txTemplate = new TransactionTemplate(txManager);
        txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        upcasters = new ArrayList<EventUpcaster>(context.getBeansOfType(EventUpcaster.class).values());
        final AtomicInteger updateCount = new AtomicInteger();
        final AtomicLong lastId = new AtomicLong(Long.parseLong(configuration.getProperty("lastProcessedId", "-1")));
        try {
            TransactionTemplate template = new TransactionTemplate(txManager);
            template.setReadOnly(true);
            while(template.execute(new TransactionCallback<Boolean>() {
                @Override
                public Boolean doInTransaction(TransactionStatus status) {
                    final Session hibernate = entityManager.unwrap(Session.class);
                    System.out.println("Fetching entries from old event store");
                    Iterator<Object[]> results = hibernate.createQuery(
                            "SELECT e.aggregateIdentifier, e.sequenceNumber, e.type, e.id FROM DomainEventEntry e WHERE e.id > :lastIdentifier ORDER BY e.id ASC")
                                                          .setFetchSize(1000)
                                                          .setMaxResults(100000)
                                                          .setParameter("lastIdentifier", lastId.get())
                                                          .iterate();
                    System.out.println("Starting conversion process");
                    if (!results.hasNext()) {
                        System.out.println("Empty batch. Assuming we're done.");
                        return false;
                    } else if (Thread.interrupted()) {
                        System.out.println("Received an interrupt. Stopping...");
                        return false;
                    }
                    while (results.hasNext()) {
                        Object[] item = results.next();
                        String aggregateIdentifier = (String) item[0];
                        long sequenceNumber = (Long) item[1];
                        String type = (String) item[2];
                        Long entryId = (Long) item[3];
                        lastId.set(entryId);
                        executor.submit(new TransformationTask(aggregateIdentifier, sequenceNumber, type, entryId));
                        final int updated = updateCount.incrementAndGet();
                        if (updated % 1000 == 0) {
                            System.out.println("Converted (or skipped) " + updateCount + " items");
                        }
                    }
                    return true;
                }
            })) {
                System.out.println("Preparing next batch of 100 000.");
            };
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);
            if (lastId.get() >= 0) {
                System.out.println("Processed events from old event store up to (and including) id = " + lastId.get());
            }
        }
        System.out.println("In total " + updateCount.get() + " items have been converted.");
    }

    private class TransformationTask implements Runnable, TransactionCallback<Void> {

        private final String aggregateIdentifier;
        private final long sequenceNumber;
        private final String type;
        private final long entryId;

        public TransformationTask(String aggregateIdentifier, long sequenceNumber, String type, long entryId) {
            this.aggregateIdentifier = aggregateIdentifier;
            this.sequenceNumber = sequenceNumber;
            this.type = type;
            this.entryId = entryId;
        }

        @Override
        public void run() {
            txTemplate.execute(this);
        }

        @Override
        public Void doInTransaction(TransactionStatus status) {
            try {
                long count = (Long)
                        entityManager.createQuery("SELECT count(e) FROM NewDomainEventEntry e "
                                                          + "WHERE e.aggregateIdentifier = :aggregateIdentifier "
                                                          + "AND e.sequenceNumber = :sequenceNumber "
                                                          + "AND e.type = :type")
                                     .setParameter("aggregateIdentifier", aggregateIdentifier)
                                     .setParameter("type", type)
                                     .setParameter("sequenceNumber", sequenceNumber)
                                     .getSingleResult();
                if (count != 0) {
                    return null;
                }

                DomainEventEntry entry = entityManager.find(DomainEventEntry.class, entryId);
                final NewDomainEventEntry newEntry = transformer.transform(entry, upcasters);
                if (newEntry != null) {
                    entityManager.persist(newEntry);
                    entityManager.flush();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
