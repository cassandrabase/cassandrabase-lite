package io.github.cassandrabase.lite.xml;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "changeLog")
public class ChangeLog {

    private List<ChangeSet> changeSets = new ArrayList<>();

    public List<ChangeSet> getChangeSets() {
        return changeSets;
    }

    @XmlElement(name = "changeSet", type = ChangeSet.class)
    public void setChangeSets(List<ChangeSet> changeSets) {
        this.changeSets = changeSets;
    }
}
