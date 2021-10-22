package com.zdjizhi.common;

import java.io.Serializable;
import java.util.Objects;

public class DosMetricsLog implements Serializable {

    private long sketch_start_time;
    private String attack_type;
    private String destination_ip;
    private long session_rate;
    private long packet_rate;
    private long bit_rate;
    private int partition_num;

    public int getPartition_num() {
        return partition_num;
    }

    public void setPartition_num(int partition_num) {
        this.partition_num = partition_num;
    }

    public long getSketch_start_time() {
        return sketch_start_time;
    }

    public void setSketch_start_time(long sketch_start_time) {
        this.sketch_start_time = sketch_start_time;
    }

    public String getAttack_type() {
        return attack_type;
    }

    public void setAttack_type(String attack_type) {
        this.attack_type = attack_type;
    }

    public String getDestination_ip() {
        return destination_ip;
    }

    public void setDestination_ip(String destination_ip) {
        this.destination_ip = destination_ip;
    }

    public long getSession_rate() {
        return session_rate;
    }

    public void setSession_rate(long session_rate) {
        this.session_rate = session_rate;
    }

    public long getPacket_rate() {
        return packet_rate;
    }

    public void setPacket_rate(long packet_rate) {
        this.packet_rate = packet_rate;
    }

    public long getBit_rate() {
        return bit_rate;
    }

    public void setBit_rate(long bit_rate) {
        this.bit_rate = bit_rate;
    }

    @Override
    public String toString() {
        return "DosMetricsLog{" +
                "sketch_start_time=" + sketch_start_time +
                ", attack_type='" + attack_type + '\'' +
                ", destination_ip='" + destination_ip + '\'' +
                ", session_rate=" + session_rate +
                ", packet_rate=" + packet_rate +
                ", bit_rate=" + bit_rate +
                ", partition_num=" + partition_num +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DosMetricsLog)) {
            return false;
        }
        DosMetricsLog that = (DosMetricsLog) o;
        return getSketch_start_time() == that.getSketch_start_time() &&
                getSession_rate() == that.getSession_rate() &&
                getPacket_rate() == that.getPacket_rate() &&
                getBit_rate() == that.getBit_rate() &&
                getPartition_num() == that.getPartition_num() &&
                Objects.equals(getAttack_type(), that.getAttack_type()) &&
                Objects.equals(getDestination_ip(), that.getDestination_ip());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSketch_start_time(), getAttack_type(), getDestination_ip(), getSession_rate(), getPacket_rate(), getBit_rate(), getPartition_num());
    }
}
