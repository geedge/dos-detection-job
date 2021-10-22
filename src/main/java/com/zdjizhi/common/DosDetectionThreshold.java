package com.zdjizhi.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;

/**
 * @author wlh
 */
public class DosDetectionThreshold implements Serializable {
    private String profileId;
    private String attackType;
    private ArrayList<String> serverIpList;
    private String serverIpAddr;
    private long packetsPerSec;
    private long bitsPerSec;
    private long sessionsPerSec;
    private int isValid;

    @Override
    public String toString() {
        return "DosDetectionThreshold{" +
                "profileId='" + profileId + '\'' +
                ", attackType='" + attackType + '\'' +
                ", serverIpList=" + serverIpList +
                ", serverIpAddr='" + serverIpAddr + '\'' +
                ", packetsPerSec=" + packetsPerSec +
                ", bitsPerSec=" + bitsPerSec +
                ", sessionsPerSec=" + sessionsPerSec +
                ", isValid=" + isValid +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DosDetectionThreshold threshold = (DosDetectionThreshold) o;
        return packetsPerSec == threshold.packetsPerSec &&
                bitsPerSec == threshold.bitsPerSec &&
                sessionsPerSec == threshold.sessionsPerSec &&
                isValid == threshold.isValid &&
                Objects.equals(profileId, threshold.profileId) &&
                Objects.equals(attackType, threshold.attackType) &&
                Objects.equals(serverIpList, threshold.serverIpList) &&
                Objects.equals(serverIpAddr, threshold.serverIpAddr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profileId, attackType, serverIpList, serverIpAddr, packetsPerSec, bitsPerSec, sessionsPerSec, isValid);
    }

    public String getProfileId() {
        return profileId;
    }

    public void setProfileId(String profileId) {
        this.profileId = profileId;
    }

    public String getAttackType() {
        return attackType;
    }

    public void setAttackType(String attackType) {
        this.attackType = attackType;
    }

    public ArrayList<String> getServerIpList() {
        return serverIpList;
    }

    public void setServerIpList(ArrayList<String> serverIpList) {
        this.serverIpList = serverIpList;
    }

    public String getServerIpAddr() {
        return serverIpAddr;
    }

    public void setServerIpAddr(String serverIpAddr) {
        this.serverIpAddr = serverIpAddr;
    }

    public long getPacketsPerSec() {
        return packetsPerSec;
    }

    public void setPacketsPerSec(long packetsPerSec) {
        this.packetsPerSec = packetsPerSec;
    }

    public long getBitsPerSec() {
        return bitsPerSec;
    }

    public void setBitsPerSec(long bitsPerSec) {
        this.bitsPerSec = bitsPerSec;
    }

    public long getSessionsPerSec() {
        return sessionsPerSec;
    }

    public void setSessionsPerSec(long sessionsPerSec) {
        this.sessionsPerSec = sessionsPerSec;
    }

    public int getIsValid() {
        return isValid;
    }

    public void setIsValid(int isValid) {
        this.isValid = isValid;
    }
}
