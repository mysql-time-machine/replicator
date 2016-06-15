package com.booking.replication.applier;

/**
 * This class implements the ChaosMonkey concept for tasks.
 * It decides to fail write tasks in 1% of cases at different code point.
 */
public class ChaosMonkey {

    private final int CHAOS_MAX = 10000;

    private final int chaosPoint;

    public ChaosMonkey() {
        chaosPoint = (int) (Math.random() * CHAOS_MAX);
    }

    public boolean feelsLikeThrowingExceptionAfterTaskSubmitted() {
        return chaosPoint < 15;
    }

    public boolean feelsLikeFailingSubmitedTaskWithoutException() {
        return chaosPoint >= 15 && chaosPoint < 30;
    }

    public boolean feelsLikeThrowingExceptionForTaskInProgress() {
        return chaosPoint >= 30 && chaosPoint < 45;
    }

    public boolean feelsLikeFailingTaskInProgessWithoutException() {
        return chaosPoint >= 45 && chaosPoint < 60;
    }

    public boolean feelsLikeThrowingExceptionBeforeFlushingData() {
        return chaosPoint >= 60 && chaosPoint < 75;
    }

    public boolean feelsLikeFailingDataFlushWithoutException() {
        return chaosPoint >= 75 && chaosPoint < 100;
    }
}
