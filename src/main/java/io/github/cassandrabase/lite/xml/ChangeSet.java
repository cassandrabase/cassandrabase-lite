package io.github.cassandrabase.lite.xml;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlTransient;
import io.github.cassandrabase.lite.util.HashGen;

import java.util.Objects;
import java.util.StringJoiner;

@XmlRootElement(name = "changeSet")
public class ChangeSet {
    private String id;
    private String author;
    private Integer order;
    private String statement;
    private String rollbackStatement;
    private DefaultConsistencyLevel consistencyLevel;
    private String md5Sum;


    @XmlAttribute(name = "author", required = true)
    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    @XmlAttribute(name = "consistencyLevel", required = true)
    public DefaultConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(DefaultConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    @XmlAttribute(name = "id", required = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @XmlTransient
    public String getMd5Sum() {
        if (Objects.isNull(md5Sum)) {
            String row = new StringJoiner("#")
                    .add(this.getId())
                    .add(this.getAuthor())
                    .add(String.valueOf(this.getOrder()))
                    .add(this.getStatement())
                    .toString();
            return HashGen.generateHash(row, HashGen.ALGType.MD5);
        }
        return md5Sum;
    }

    public void setMd5Sum(String md5Sum) {
        this.md5Sum = md5Sum;
    }

    @XmlAttribute(name = "order", required = true)
    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }

    public String getStatement() {
        if (Objects.nonNull(statement)) {
            return statement.replaceAll("[\\n\\r]+", " ")
                    .replaceAll("\\s+", " ")
                    .trim();
        }
        return null;
    }

    @XmlElement(name = "statement", required = true)
    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getRollbackStatement() {
        return rollbackStatement;
    }

    @XmlElement(name = "rollbackStatement")
    public void setRollbackStatement(String rollbackStatement) {
        this.rollbackStatement = rollbackStatement;
    }
    @Override
    public int hashCode() {
        return Objects.hash(order);
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ChangeSet changeSet) {
            return Objects.equals(changeSet.getOrder(), this.getOrder());
        } else {
            throw new RuntimeException("Object is not a ChangeSet");
        }
    }
}
