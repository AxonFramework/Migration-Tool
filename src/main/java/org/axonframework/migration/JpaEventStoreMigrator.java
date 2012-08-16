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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
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
            while(System.in.available() > 0) {
                System.in.skip(System.in.available());
            }
            System.out.println("Press enter to quit");
            new Scanner(System.in).nextLine();
        }
    }

    private void run(ClassPathXmlApplicationContext context) throws Exception {
        context.getAutowireCapableBeanFactory().autowireBean(this);

        upcasters = new ArrayList<EventUpcaster>(context.getBeansOfType(EventUpcaster.class).values());

        TransactionTemplate txTemplate = new TransactionTemplate(txManager);
        final int batchSize = 100;
        final AtomicInteger updateCount = new AtomicInteger();
        final AtomicInteger lastBatchSize = new AtomicInteger(batchSize);
        while (lastBatchSize.get() == batchSize) {
            txTemplate.execute(new TransactionCallbackWithoutResult() {
                @Override
                protected void doInTransactionWithoutResult(TransactionStatus status) {
                    lastBatchSize.set(0);
                    List<DomainEventEntry> entries = entityManager.createQuery(
                            "SELECT e FROM DomainEventEntry e WHERE e.eventIdentifier = :empty")
                                                                  .setMaxResults(batchSize)
                                                                  .setParameter("empty", "")
                                                                  .getResultList();
                    for (DomainEventEntry entry : entries) {
                        try {
                            if (transformer.transform(entry, upcasters)) {
                                lastBatchSize.incrementAndGet();
                                updateCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        System.out.println("In total " + updateCount.get() + " items have been converted.");
    }
}
