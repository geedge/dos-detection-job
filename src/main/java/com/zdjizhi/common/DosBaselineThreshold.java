package com.zdjizhi.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;

public class DosBaselineThreshold implements Serializable {
    private ArrayList<Integer> session_rate;
    private Integer session_rate_baseline_type;
    private Integer session_rate_default_value;

    public ArrayList<Integer> getSession_rate() {
        return session_rate;
    }

    public void setSession_rate(ArrayList<Integer> session_rate) {
        this.session_rate = session_rate;
    }

    public Integer getSession_rate_baseline_type() {
        return session_rate_baseline_type;
    }

    public void setSession_rate_baseline_type(Integer session_rate_baseline_type) {
        this.session_rate_baseline_type = session_rate_baseline_type;
    }

    public Integer getSession_rate_default_value() {
        return session_rate_default_value;
    }

    public void setSession_rate_default_value(Integer session_rate_default_value) {
        this.session_rate_default_value = session_rate_default_value;
    }

    @Override
    public String toString() {
        return "DosBaselineThreshold{" +
                "session_rate=" + session_rate +
                ", session_rate_baseline_type=" + session_rate_baseline_type +
                ", session_rate_default_value=" + session_rate_default_value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DosBaselineThreshold)) {
            return false;
        }
        DosBaselineThreshold that = (DosBaselineThreshold) o;
        return Objects.equals(getSession_rate(), that.getSession_rate()) &&
                Objects.equals(getSession_rate_baseline_type(), that.getSession_rate_baseline_type()) &&
                Objects.equals(getSession_rate_default_value(), that.getSession_rate_default_value());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSession_rate(), getSession_rate_baseline_type(), getSession_rate_default_value());
    }
}
