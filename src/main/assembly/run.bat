@ECHO OFF
java -cp core_deps/*;events/*;upcasters/*;other_deps/* org.axonframework.migration.JpaEventStoreMigrator