package com.booking.replication.coordinator;

public interface LeaderCoordinator {
    void onLeadershipTake(Runnable runnable);

    void onLeadershipLoss(Runnable runnable);
}
