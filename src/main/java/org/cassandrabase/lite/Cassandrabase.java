package org.cassandrabase.lite;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.cassandrabase.lite.entity.ChangelogLockEntity;
import org.cassandrabase.lite.model.CassandraConfigs;
import org.cassandrabase.lite.repository.ChangelogLockRepository;
import org.cassandrabase.lite.util.HashGen;
import org.cassandrabase.lite.xml.CassandraBaseConfig;
import org.cassandrabase.lite.xml.ChangeLog;
import org.cassandrabase.lite.xml.ChangeSet;
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
import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

public final class Cassandrabase implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(Cassandrabase.class);
    private final CqlSession cqlSession;
    private final CassandraConfigs cassandraConfigs;
    private final String identifier;
    private final ChangelogLockRepository changelogLockRepository;
    private final AtomicBoolean updated = new AtomicBoolean(false);
    private final String md5Key;
    private final Object classObject;

    public Cassandrabase(CqlSession cqlSession, String identifier, Object classObject) throws SAXException, IOException {
        this.classObject = classObject;
        this.cqlSession = cqlSession;
        this.identifier = identifier;
        this.changelogLockRepository = new ChangelogLockRepository(this.cqlSession);
        this.cassandraConfigs = this.getPrimaryChangeLog();
        List<ChangeSet> orderedChangeSets = cassandraConfigs.getPreChangeLog().getChangeSets().stream().sorted(Comparator.comparing(ChangeSet::getOrder)).toList();
        final StringJoiner keyAsString = new StringJoiner("#");
        for (ChangeSet changeSet : orderedChangeSets) {
            keyAsString.add(changeSet.getMd5Sum());
        }
        this.md5Key = HashGen.generateHash(keyAsString.toString(), HashGen.ALGType.MD5);
        log.info("Major version Key (MD5): {}", md5Key);
    }

    private void init() {
        if (!this.changelogLockRepository.tableExists()) {
            this.cassandraConfigs.getPreChangeLog()
                    .getChangeSets()
                    .stream()
                    .sorted(Comparator.comparing(ChangeSet::getOrder))
                    .forEach(this::execute);
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

    public void validate() {
        this.init();
        if (this.updated.get()) {
            log.info("Start applying changes...");
            this.saveChangeLog(this.cassandraConfigs.getChangeLog());
        }
    }

    private void saveChangeLog(ChangeLog changeLog) {
        changeLog.getChangeSets().stream().sorted(Comparator.comparing(ChangeSet::getOrder)).forEach(this::execute);
    }

    private void execute(ChangeSet changeSet) {
        SimpleStatement simpleStatement = SimpleStatement.newInstance(changeSet.getStatement()).setConsistencyLevel(changeSet.getConsistencyLevel());
        log.info("Updating Change log. [ChangeLogId: {}, Author : {}, Order : {}, RowKey : {}]", changeSet.getId(), changeSet.getAuthor(), changeSet.getOrder(), changeSet.getMd5Sum());
        cqlSession.execute(simpleStatement);
    }

    private File buildChangeLogPath(String[] paths) {
        return Paths.get("src/main/resources/db", paths).toFile();
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
                try (final InputStream changeLogFileStream = this.classObject.getClass().getClassLoader().getResourceAsStream("db/" + preChangeLogFile)) {
                    final ChangeLog preChangeLog = (ChangeLog) jaxbUnmarshaller.unmarshal(changeLogFileStream);
                    cassandraConfigs.setPreChangeLog(preChangeLog);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                try (final InputStream changeLogFileStream = this.classObject.getClass().getClassLoader().getResourceAsStream("db/" + changeLogFile)) {
                    final ChangeLog changeLog = (ChangeLog) jaxbUnmarshaller.unmarshal(changeLogFileStream);
                    cassandraConfigs.setChangeLog(changeLog);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return cassandraConfigs;
        } catch (JAXBException | IOException e) {
            throw new RuntimeException(e);
        }
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
