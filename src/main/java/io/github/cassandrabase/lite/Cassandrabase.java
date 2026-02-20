package io.github.cassandrabase.lite;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.github.cassandrabase.lite.entity.ChangelogLockEntity;
import io.github.cassandrabase.lite.exception.CassandrabaseException;
import io.github.cassandrabase.lite.exception.ChangeLogAlreadyExistException;
import io.github.cassandrabase.lite.model.CassandraConfigs;
import io.github.cassandrabase.lite.repository.ChangelogLockRepository;
import io.github.cassandrabase.lite.types.ChangeLogOrder;
import io.github.cassandrabase.lite.util.HashGen;
import io.github.cassandrabase.lite.xml.CassandraBaseConfig;
import io.github.cassandrabase.lite.xml.ChangeLog;
import io.github.cassandrabase.lite.xml.ChangeSet;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Cassandrabase implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(Cassandrabase.class);
    private final CqlSession cqlSession;
    private final CassandraConfigs cassandraConfigs;
    private final String identifier;
    private final ChangelogLockRepository changelogLockRepository;
    private final AtomicBoolean updated = new AtomicBoolean(false);
    private String md5Key;
    private final Object classObject;
    private final Properties properties;

    public Cassandrabase(CqlSession cqlSession, String identifier, Object classObject) throws CassandrabaseException {
        this(cqlSession, identifier, classObject, new Properties());
    }

    public Cassandrabase(CqlSession cqlSession, String identifier, Object classObject, Properties properties) throws CassandrabaseException {
        try {
            this.classObject = classObject;
            this.cqlSession = cqlSession;
            this.identifier = identifier;
            this.properties = properties;
            this.changelogLockRepository = new ChangelogLockRepository(this.cqlSession);
            this.cassandraConfigs = this.getPrimaryChangeLog();
        } catch (SAXException e) {
            throw new CassandrabaseException(e);
        }
    }

    private ChangeLog dynamicBeforeChangeLog;
    private ChangeLog dynamiAfterChangeLog;

    public void addDynamicChangeLog(ChangeLog dynamicChangeLog, ChangeLogOrder changeLogOrder) {
        if (changeLogOrder.equals(ChangeLogOrder.BEFORE_STATIC_CHANGE_LOG)) {
            if (Objects.isNull(this.dynamicBeforeChangeLog)) {
                this.dynamicBeforeChangeLog = dynamicChangeLog;
            } else {
                throw new ChangeLogAlreadyExistException("Before-DynamicChangeLog already has been set.");
            }
        }
        if (changeLogOrder.equals(ChangeLogOrder.AFTER_STATIC_CHANGE_LOG)) {
            if (Objects.isNull(this.dynamiAfterChangeLog)) {
                this.dynamiAfterChangeLog = dynamicChangeLog;
            } else {
                throw new ChangeLogAlreadyExistException("After-DynamicChangeLog already has been set.");
            }
        }
    }


    private Runnable runnableBefore;

    public void runBefore(Runnable runnableBefore) {
        this.runnableBefore = runnableBefore;
    }

    private Runnable runnableAfter;

    public void runAfter(Runnable runnableAfter) {
        this.runnableAfter = runnableAfter;
    }


    private void init(boolean isAsync) {
        List<ChangeSet> orderedChangeSetsPre = cassandraConfigs.getPreChangeLog().getChangeSets().stream().sorted(Comparator.comparing(ChangeSet::getOrder)).toList();
        List<ChangeSet> orderedChangeSets = cassandraConfigs.getChangeLog().getChangeSets().stream().sorted(Comparator.comparing(ChangeSet::getOrder)).toList();
        final StringJoiner keyAsString = new StringJoiner("#");
        orderedChangeSetsPre.stream().map(ChangeSet::getMd5Sum).forEach(keyAsString::add);
        Optional.ofNullable(this.dynamicBeforeChangeLog).ifPresent(changeLog -> {
            changeLog.getChangeSets().forEach(changeSet -> keyAsString.add(changeSet.getMd5Sum()));
        });
        orderedChangeSets.stream().map(ChangeSet::getMd5Sum).forEach(keyAsString::add);
        Optional.ofNullable(this.dynamiAfterChangeLog).ifPresent(changeLog -> {
            changeLog.getChangeSets().forEach(changeSet -> keyAsString.add(changeSet.getMd5Sum()));
        });
        this.md5Key = HashGen.generateHash(keyAsString.toString(), HashGen.ALGType.MD5);
        log.info("Major version Key (MD5): {}", md5Key);
        if (!this.changelogLockRepository.tableExists()) {
            if (isAsync) {
                final List<CompletableFuture<AsyncResultSet>> asyncResultSetCompletableFutureList = new ArrayList<>();
                this.cassandraConfigs.getPreChangeLog()
                        .getChangeSets()
                        .stream()
                        .sorted(Comparator.comparing(ChangeSet::getOrder))
                        .forEach(changeSet -> {
                            asyncResultSetCompletableFutureList.add(this.executeAsync(changeSet));
                        });

                CompletableFuture.allOf(asyncResultSetCompletableFutureList.toArray(new CompletableFuture[0])).join();
                for (CompletableFuture<AsyncResultSet> asyncResultSetCompletableFuture : asyncResultSetCompletableFutureList) {
                    try {
                        asyncResultSetCompletableFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                this.cassandraConfigs.getPreChangeLog()
                        .getChangeSets()
                        .stream()
                        .sorted(Comparator.comparing(ChangeSet::getOrder))
                        .forEach(this::execute);
            }
            log.info("Changelog Tables created.");
        } else {
            log.info("Changelog Tables already exists.");
        }
        final boolean isAcquired = this.changelogLockRepository.acquireLock(this.md5Key, this.cassandraConfigs.getMajorVersion(), this.identifier);
        this.updated.set(isAcquired);
        if (isAcquired) {
            log.info("Lock acquired By {}. Proceeding to update.", this.identifier);
        } else {
            int count = 0;
            boolean isDone = false;
            while (!isDone) {
                try {
                    ChangelogLockEntity entity = this.changelogLockRepository.getByLockId(this.md5Key, this.cassandraConfigs.getMajorVersion());
                    if (entity.getLocked()) {
                        Thread.sleep(1_000);
                        log.info("Waiting for lock... Lock is being acquired by another instance: {}", entity.getLockedBy());
                    }
                    isDone = !entity.getLocked();
                    count++;
                    if (count > 20) {
                        log.warn("It seems like the acquirer crashed unexpectedly.");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("The Update already has been done by another instance. Nothing to update.");
        }
    }

    public void validateInParallel() {
        this.init(true);
        if (this.updated.get()) {
            log.info("Start applying changes in parallel...");
            if (Objects.nonNull(this.dynamicBeforeChangeLog)) {
                this.saveChangeLog(this.dynamicBeforeChangeLog,true);
            } else {
                log.debug("No dynamic changeLog found for updating before static changeLog.");
            }
            if (Objects.nonNull(this.runnableBefore)) {
                this.runnableBefore.run();
            } else {
                log.debug("No runnable for updating before static changeLog.");
            }
            this.saveChangeLog(this.cassandraConfigs.getChangeLog(),true);
            if (Objects.nonNull(this.dynamiAfterChangeLog)) {
                this.saveChangeLog(this.dynamiAfterChangeLog,true);
            } else {
                log.debug("Not dynamic changeLog found for updating after static changeLog.");
            }
            if (Objects.nonNull(this.runnableAfter)) {
                this.runnableBefore.run();
            } else {
                log.debug("No runnable for updating after static changeLog.");
            }
        }
    }

    public void validate() {
        this.init(false);
        if (this.updated.get()) {
            log.info("Start applying changes...");
            if (Objects.nonNull(this.dynamicBeforeChangeLog)) {
                this.saveChangeLog(this.dynamicBeforeChangeLog,false);
            } else {
                log.debug("No dynamic changeLog found for updating before static changeLog.");
            }
            if (Objects.nonNull(this.runnableBefore)) {
                this.runnableBefore.run();
            } else {
                log.debug("No runnable for updating before static changeLog.");
            }
            this.saveChangeLog(this.cassandraConfigs.getChangeLog(),false);
            if (Objects.nonNull(this.dynamiAfterChangeLog)) {
                this.saveChangeLog(this.dynamiAfterChangeLog,false);
            } else {
                log.debug("Not dynamic changeLog found for updating after static changeLog.");
            }
            if (Objects.nonNull(this.runnableAfter)) {
                this.runnableBefore.run();
            } else {
                log.debug("No runnable for updating after static changeLog.");
            }
        }
    }

    private void saveChangeLog(ChangeLog changeLog, boolean async) {
        if (async) {
            this.saveChangeLogAsync(changeLog);
        } else {
            changeLog.getChangeSets().stream().sorted(Comparator.comparing(ChangeSet::getOrder)).forEach(this::execute);
        }
    }

    private void saveChangeLogAsync(ChangeLog changeLog) {
        final List<CompletableFuture<AsyncResultSet>> asyncResultSetCompletableFutureList = new ArrayList<>();

        changeLog.getChangeSets().stream().sorted(Comparator.comparing(ChangeSet::getOrder))
                .forEach(changeSet -> {
                    asyncResultSetCompletableFutureList.add(executeAsync(changeSet));
                });
        CompletableFuture.allOf(asyncResultSetCompletableFutureList.toArray(new CompletableFuture[0])).join();
        for (CompletableFuture<AsyncResultSet> asyncResultSetCompletableFuture : asyncResultSetCompletableFutureList) {
            try {
                asyncResultSetCompletableFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private void execute(ChangeSet changeSet) {
        SimpleStatement simpleStatement = SimpleStatement.newInstance(changeSet.getStatement()).setConsistencyLevel(changeSet.getConsistencyLevel());
        log.info("Updating Change log. [ChangeLogId: {}, Author : {}, Order : {}, RowKey : {}]", changeSet.getId(), changeSet.getAuthor(), changeSet.getOrder(), changeSet.getMd5Sum());
        cqlSession.execute(simpleStatement);
    }

    private CompletableFuture<AsyncResultSet> executeAsync(ChangeSet changeSet) {
        SimpleStatement simpleStatement = SimpleStatement.newInstance(changeSet.getStatement()).setConsistencyLevel(changeSet.getConsistencyLevel());
        log.info("Updating Change log asynchronously. [ChangeLogId: {}, Author : {}, Order : {}, RowKey : {}]", changeSet.getId(), changeSet.getAuthor(), changeSet.getOrder(), changeSet.getMd5Sum());
        return this.cqlSession.executeAsync(simpleStatement).toCompletableFuture();
    }


    private CassandraConfigs getPrimaryChangeLog() throws SAXException {
        try {
            final CassandraConfigs cassandraConfigs = new CassandraConfigs();
            String preChangeLogFile;
            String changeLogFile;
            {
                final InputStream inputStream = this.classObject.getClass().getClassLoader().getResourceAsStream("db/changeset-config.xml");
                final JAXBContext jaxbContext = JAXBContext.newInstance(CassandraBaseConfig.class);
                final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
                SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                File file;
                try (InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("CassandraBaseConfig.xsd")) {
                    Path path = Paths.get("CassandraBaseConfig.xsd");
                    if (!Files.exists(path)) {
                        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
                            assert resourceAsStream != null;
                            writer.write(new String(resourceAsStream.readAllBytes()));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    file = path.toFile();
                    Schema schema = schemaFactory.newSchema(file);
                    jaxbUnmarshaller.setSchema(schema);
                    final CassandraBaseConfig config = (CassandraBaseConfig) jaxbUnmarshaller.unmarshal(inputStream);
                    preChangeLogFile = config.getPreChangeLog().getPath();
                    changeLogFile = config.getChangeLog().getPath();
                    cassandraConfigs.setMajorVersion(config.getMajorVersion());
                }
                if (file.exists()) {
                    if (file.delete()) {
                        log.debug("deleted temp file.");
                    }
                }
            }

            {
                final JAXBContext jaxbContext = JAXBContext.newInstance(ChangeLog.class);
                final Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
                cassandraConfigs.setPreChangeLog(new ChangeLog());
                try (final InputStream changeLogFileStream = this.classObject.getClass().getClassLoader().getResourceAsStream("db/" + preChangeLogFile)) {
                    final ChangeLog preChangeLog = (ChangeLog) jaxbUnmarshaller.unmarshal(changeLogFileStream);
                    cassandraConfigs.setPreChangeLog(preChangeLog);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                cassandraConfigs.setChangeLog(new ChangeLog());
                try (final InputStream changeLogFileStream = this.classObject.getClass().getClassLoader().getResourceAsStream("db/" + changeLogFile)) {
                    final ChangeLog changeLog = (ChangeLog) jaxbUnmarshaller.unmarshal(changeLogFileStream);
                    cassandraConfigs.setChangeLog(changeLog);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            this.validate(cassandraConfigs);
            this.setProperties(cassandraConfigs);
            return cassandraConfigs;
        } catch (JAXBException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void validate(CassandraConfigs cassandraConfigs) {
        {
            HashSet<ChangeSet> changeSetsByOrder = new HashSet<>();
            HashSet<ChangeSet> changeSetsById = new HashSet<>();
            cassandraConfigs.getPreChangeLog().getChangeSets().forEach(changeSet -> {
                if (!changeSetsByOrder.add(changeSet)) {
                    throw new RuntimeException("Duplicate changeSet found in preChangeLog. [Order : " + changeSet.getOrder() + "]");
                }
                if (!changeSetsById.add(changeSet)) {
                    throw new RuntimeException("Duplicate changeSet found in preChangeLog. [ChangeSetId : " + changeSet.getId() + "]");
                }
            });
        }
        {
            HashSet<ChangeSet> changeSetsByOrder = new HashSet<>();
            HashSet<ChangeSet> changeSetsById = new HashSet<>();
            cassandraConfigs.getChangeLog().getChangeSets().forEach(changeSet -> {
                if (!changeSetsByOrder.add(changeSet)) {
                    throw new RuntimeException("Duplicate changeSet found in post changeLog. [Order : " + changeSet.getOrder() + "]");
                }
                if (!changeSetsById.add(changeSet)) {
                    throw new RuntimeException("Duplicate changeSet found in post changeLog. [ChangeSetId : " + changeSet.getId() + "]");
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    private void setProperties(CassandraConfigs cassandraConfigs) {
        cassandraConfigs.getChangeLog().getChangeSets().forEach(changeSet -> {
            StringSubstitutor sub = new StringSubstitutor((Map) this.properties);
            String updatedStatement = sub.replace(changeSet.getStatement());
            changeSet.setStatement(updatedStatement);
        });
        cassandraConfigs.getPreChangeLog().getChangeSets().forEach(changeSet -> {
            StringSubstitutor sub = new StringSubstitutor((Map) this.properties);
            String updatedStatement = sub.replace(changeSet.getStatement());
            changeSet.setStatement(updatedStatement);
        });
    }

    @Override
    public void close() {
        if (this.updated.get()) {
            if (this.changelogLockRepository.releaseLock(this.md5Key, this.cassandraConfigs.getMajorVersion(), this.identifier)) {
                log.info("lock released successfully [{}]", this.md5Key);
            } else {
                log.warn("lock released failed [{}]", this.md5Key);
            }
        }
    }
}
