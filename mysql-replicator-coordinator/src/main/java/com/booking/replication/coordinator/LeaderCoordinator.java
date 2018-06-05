package com.booking.replication.coordinator;

public interface LeaderCoordinator {
    void onLeadershipTake(Runnable runnable);

    void onLeadershipLose(Runnable runnable);
}
