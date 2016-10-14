package com.booking.replication.monitor;

import org.apache.commons.lang.StringUtils;

public class ReplicatorHealthAssessment {
    private final Boolean isOk;
    private final String diagnosis;

    public static final ReplicatorHealthAssessment Normal = new ReplicatorHealthAssessment(true, null);

    public ReplicatorHealthAssessment(Boolean isOk, String diagnosis)
    {
        if (isOk && !StringUtils.isEmpty(diagnosis))
        {
            throw new IllegalArgumentException("If the status is OK, there can be no diagnosis associated with that");
        }

        this.isOk = isOk;
        this.diagnosis = diagnosis;
    }

    public Boolean isOk() {
        return isOk;
    }

    public String getDiagnosis() {
        return diagnosis;
    }
}
