package io.distmap.persistent;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by אלכס on 25/02/2016.
 */
public class DBInfo {
    private String dbName;
    private List<String> dbHosts;
    private String userName;
    private String password;


    public static DBInfo getDBInfo(Properties properties){
        String userName = properties.getProperty("userName");
        String password = properties.getProperty("password");
        String dbName = properties.getProperty("dbName");
        String dbHosts = properties.getProperty("dbHosts");
        List<String> dbHostsList = Arrays.asList(dbHosts.split (","));
        return new DBInfo(dbName, dbHostsList,userName,password);
    }

    public static Properties getProperties(DBInfo dbInfo){
        Properties properties = new Properties();
        properties.setProperty("userName", dbInfo.getUserName());
        properties.setProperty("password", dbInfo.getPassword());
        properties.setProperty("dbName", dbInfo.getDbName());
        String hosts = dbInfo.getDbHosts().stream().reduce("", (x, y)-> x +","+ y);
        hosts = hosts.substring(1);
        properties.setProperty("dbHosts", hosts);
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DBInfo dbInfo = (DBInfo) o;

        if (dbName != null ? !dbName.equals(dbInfo.dbName) : dbInfo.dbName != null) return false;
        if (dbHosts != null ? !dbHosts.equals(dbInfo.dbHosts) : dbInfo.dbHosts != null) return false;
        if (userName != null ? !userName.equals(dbInfo.userName) : dbInfo.userName != null) return false;
        return password != null ? password.equals(dbInfo.password) : dbInfo.password == null;

    }

    @Override
    public int hashCode() {
        int result = dbName != null ? dbName.hashCode() : 0;
        result = 31 * result + (dbHosts != null ? dbHosts.hashCode() : 0);
        result = 31 * result + (userName != null ? userName.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        return result;
    }

    public String getDbName() {

        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public List<String> getDbHosts() {
        return dbHosts;
    }

    public void setDbHosts(List<String> dbHosts) {
        this.dbHosts = dbHosts;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public DBInfo(String dbName, List<String> dbHosts, String userName, String password) {
        this.dbName = dbName;
        this.dbHosts = dbHosts;
        this.userName = userName;
        this.password = password;
    }

}
