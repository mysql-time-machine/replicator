package com.booking.replication.applier;

/**
 * This class implements the ChaosMonkey concept for tasks.
 * It decides to fail write tasks in 1% of cases at different code points.
 */
public class ChaosMonkey {

    private final int CHAOS_MAX = 10000;

    private final int chaosPoint;

    public ChaosMonkey() {
        chaosPoint = (int)(Math.random()*CHAOS_MAX);
    }

    public boolean feelsLikeThrowingExceptionAfterTaskSubmitted() {
        if ( chaosPoint < 15) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean feelsLikeFailingSubmitedTaskWithoutException() {
        if ( chaosPoint >= 15 && chaosPoint < 30) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean feelsLikeThrowingExceptionForTaskInProgress() {
        if (chaosPoint >= 30 && chaosPoint < 45) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean feelsLikeFailingTaskInProgessWithoutException() {
        if (chaosPoint >= 45 && chaosPoint < 60) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean feelsLikeThrowingExceptionBeforeFlushingData() {
        if (chaosPoint >= 60 && chaosPoint < 75) {
            return true;
        }
        else {
            return false;
        }
    }

    public boolean feelsLikeFailingDataFlushWithoutException() {
        if (chaosPoint >= 75 && chaosPoint < 100) {
            return true;
        }
        else {
            return false;
        }
    }
}
