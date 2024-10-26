package io.github.cassandrabase.lite.repository;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import io.github.cassandrabase.lite.entity.ChangelogLockEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public class ChangelogLockRepository implements ChangelogLockEntity.Keys {
    private static final Logger log = LoggerFactory.getLogger(ChangelogLockRepository.class);
    private final CqlSession cqlSession;

    public ChangelogLockRepository(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
    }

    public boolean acquireLock(String lockId, Integer majorVersion, String identifier) {
        SimpleStatement acquireLockQuery = QueryBuilder
                .insertInto(TABLE_NAME)
                .value(ID, QueryBuilder.literal(lockId))
                .value(MAJOR_VERSION, QueryBuilder.literal(majorVersion))
                .value(LOCKED, QueryBuilder.literal(true))
                .value(LOCK_TIME, QueryBuilder.literal(Instant.now()))
                .value(LOCKED_BY, QueryBuilder.literal(identifier))
                .ifNotExists()
                .build();
        log.debug("ChangelogLockRepository:acquireLock:{}", acquireLockQuery.getQuery());
        return cqlSession
                .execute(acquireLockQuery)
                .wasApplied();
    }


    public boolean releaseLock(String lockId, Integer majorVersion, String identifier) {
        SimpleStatement acquireLockQuery = QueryBuilder
                .update(TABLE_NAME)
                .setColumn(LOCKED, QueryBuilder.literal(false))
                .whereColumn(ID)
                .isEqualTo(QueryBuilder.literal(lockId))
                .whereColumn(MAJOR_VERSION)
                .isEqualTo(QueryBuilder.literal(majorVersion))
                .ifColumn(LOCKED)
                .isEqualTo(QueryBuilder.literal(true))
                .build();
        log.debug("ChangelogLockRepository:releaseLock:{}", acquireLockQuery.getQuery());
        return cqlSession
                .execute(acquireLockQuery)
                .wasApplied();
    }

    public ChangelogLockEntity getByLockId(String md5Key, Integer majorVersion) {

        SimpleStatement simpleStatement = QueryBuilder.selectFrom(TABLE_NAME)
                .all()
                .whereColumn(ID)
                .isEqualTo(QueryBuilder.literal(md5Key))
                .whereColumn(MAJOR_VERSION)
                .isEqualTo(QueryBuilder.literal(majorVersion))
                .build()
                .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM);
        log.debug("ChangelogLockRepository:getByLockId:{}", simpleStatement.getQuery());
        return Optional
                .ofNullable(cqlSession
                        .execute(simpleStatement)
                        .one()
                )
                .map(ChangelogLockRepository::map)
                .orElse(null);

    }


    public static ChangelogLockEntity map(Row row) {
        ChangelogLockEntity changelogLockEntity = new ChangelogLockEntity();
        if (row.getColumnDefinitions().contains(ID)) {
            changelogLockEntity.setId(row.getString(ID));
        }
        if (row.getColumnDefinitions().contains(LOCKED)) {
            changelogLockEntity.setLocked(row.getBoolean(LOCKED));
        }
        if (row.getColumnDefinitions().contains(LOCK_TIME)) {
            changelogLockEntity.setLockTime(row.getInstant(LOCK_TIME));
        }
        if (row.getColumnDefinitions().contains(LOCKED_BY)) {
            changelogLockEntity.setLockedBy(row.getString(LOCKED_BY));
        }
        return changelogLockEntity;
    }

    public boolean tableExists() {
        SimpleStatement simpleStatement = QueryBuilder
                .selectFrom("system_schema", "tables")
                .column("table_name")
                .whereColumn("keyspace_name").isEqualTo(QueryBuilder.literal(cqlSession.getKeyspace().orElseThrow().asInternal()))
                .whereColumn("table_name").isEqualTo(QueryBuilder.literal(TABLE_NAME))
                .build();
        log.debug("ChangeLogEntity:tableExists:{}", simpleStatement.getQuery());
        return Objects.nonNull(cqlSession
                .execute(simpleStatement)
                .one());

    }
}
