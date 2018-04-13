package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by prernasingh on 4/4/18.
 */

import java.io.Serializable;
import java.util.HashMap;


public class MessageData implements Serializable {
    private String key;
    private String value;
    private int predPort;
    private int succPort;
    private String messageType;
    private HashMap<String, String> mapData = null;

    public MessageData(){
        this.key = null;
        this.value = null;
        this.predPort = 0;
        this.succPort = 0;
        this.messageType = null;
        this.mapData= null;

    }

    public MessageData(String key, String value, int predPort, int succPort, String messageType, HashMap<String, String> mapData) {
        this.key = key;
        this.value = value;
        this.predPort = predPort;
        this.succPort = succPort;
        this.messageType = messageType;
        this.mapData= mapData;
    }



    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getPredPort() {
        return predPort;
    }

    public void setPredPort(int predPort) {
        this.predPort = predPort;
    }

    public int getSuccPort() {
        return succPort;
    }

    public void setSuccPort(int succPort) {
        this.succPort = succPort;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public HashMap<String, String> getMapData() {
        return mapData;
    }

    public void setMapData(HashMap<String, String> mapData) {
        this.mapData = mapData;
    }


}

