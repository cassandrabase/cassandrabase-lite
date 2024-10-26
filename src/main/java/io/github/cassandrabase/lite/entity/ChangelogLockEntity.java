package io.github.cassandrabase.lite.entity;

import java.time.Instant;

public class ChangelogLockEntity {
    private String id;
    private Boolean locked;
    private Instant lockTime;
    private String lockedBy;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean getLocked() {
        return locked;
    }

    public void setLocked(Boolean locked) {
        this.locked = locked;
    }

    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(String lockedBy) {
        this.lockedBy = lockedBy;
    }

    public Instant getLockTime() {
        return lockTime;
    }

    public void setLockTime(Instant lockTime) {
        this.lockTime = lockTime;
    }

    public interface Keys {
        String TABLE_NAME = "changelog_lock";
        String ID = "id";
        String MAJOR_VERSION = "major_version";
        String LOCKED = "locked";
        String LOCK_TIME = "lock_time";
        String LOCKED_BY = "locked_by";
    }
}
