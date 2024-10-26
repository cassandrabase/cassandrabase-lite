package io.github.cassandrabase.lite.xml;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "cassandraBaseConfig")
public class CassandraBaseConfig {
    private Integer majorVersion;
    private Include preChangeLog;
    private Include changeLog;

    @XmlElement(name = "changeLog", required = true)
    public Include getChangeLog() {
        return changeLog;
    }

    public void setChangeLog(Include changeLog) {
        this.changeLog = changeLog;
    }

    @XmlElement(name = "preChangeLog", required = true)
    public Include getPreChangeLog() {
        return preChangeLog;
    }

    public void setPreChangeLog(Include preChangeLog) {
        this.preChangeLog = preChangeLog;
    }

    @XmlElement(name = "majorVersion", required = true)
    public Integer getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(Integer majorVersion) {
        this.majorVersion = majorVersion;
    }
}
