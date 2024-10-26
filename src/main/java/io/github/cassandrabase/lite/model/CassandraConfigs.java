package io.github.cassandrabase.lite.model;

import io.github.cassandrabase.lite.xml.ChangeLog;

public class CassandraConfigs {
    private ChangeLog preChangeLog;
    private ChangeLog changeLog;
    private Integer majorVersion;

    public ChangeLog getChangeLog() {
        return changeLog;
    }

    public void setChangeLog(ChangeLog changeLog) {
        this.changeLog = changeLog;
    }

    public Integer getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(Integer majorVersion) {
        this.majorVersion = majorVersion;
    }

    public ChangeLog getPreChangeLog() {
        return preChangeLog;
    }

    public void setPreChangeLog(ChangeLog preChangeLog) {
        this.preChangeLog = preChangeLog;
    }
}
