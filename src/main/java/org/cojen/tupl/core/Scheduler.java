/*
 *  Copyright (C) 2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.util.PriorityQueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * Simple task scheduler that doesn't hoard waiting threads, unlike ScheduledThreadPoolExecutor.
 *
 * @author Brian S O'Neill
 */
public final class Scheduler {
    private final ExecutorService mExecutor;
    private final PriorityQueue<Delayed> mDelayed;

    private boolean mRunning;

    private static Scheduler cDaemon;

    public static synchronized Scheduler daemon() {
        if (cDaemon == null) {
            cDaemon = new Scheduler(Executors.newCachedThreadPool(r -> {
                var t = new Thread(r);
                t.setDaemon(true);
                return t;
            }));
        }

        return cDaemon;
    }

    public Scheduler() {
        this(Executors.newCachedThreadPool());
    }

    public Scheduler(ExecutorService executor) {
        if (executor == null) {
            throw new IllegalArgumentException();
        }
        mExecutor = executor;
        mDelayed = new PriorityQueue<>();
    }

    public void shutdown() {
        mExecutor.shutdown();
        synchronized (this) {
            mDelayed.clear();
            notify();
        }
    }

    public boolean isShutdown() {
        return mExecutor.isShutdown();
    }

    /**
     * @return false if shutdown
     */
    public boolean execute(Runnable task) {
        try {
            mExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            if (isShutdown()) {
                return false;
            }
            scheduleMillis(task, 1);
        }
        return true;
    }

    /**
     * @return false if shutdown
     */
    public boolean scheduleMillis(Runnable task, long delayMillis) {
        return scheduleNanos(task, delayMillis * 1_000_000);
    }

    /**
     * @return false if shutdown
     */
    public boolean scheduleNanos(Runnable task, long delayNanos) {
        return scheduleNanos(new Delayed.Runner(System.nanoTime() + delayNanos, task));
    }

    /**
     * @return false if shutdown
     */
    public synchronized boolean scheduleNanos(Delayed delayed) {
        mDelayed.add(delayed);

        if (!mRunning) {
            if (!doExecute(this::runDelayedTasks)) {
                return false;
            }
            mRunning = true;
        } else if (mDelayed.peek() == delayed) {
            notify();
        }

        return true;
    }

    /**
     * @return false if shutdown
     */
    private boolean doExecute(Runnable task) {
        while (true) {
            try {
                mExecutor.execute(task);
                return true;
            } catch (RejectedExecutionException e) {
                if (isShutdown()) {
                    return false;
                }
            }
            // Keep trying.
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }

    private void runDelayedTasks() {
        while (true) {
            Delayed delayed;
            synchronized (this) {
                while (true) {
                    delayed = mDelayed.peek();
                    if (delayed == null) {
                        mRunning = false;
                        return;
                    }
                    long delayNanos = delayed.mCounter - System.nanoTime();
                    if (delayNanos <= 0) {
                        break;
                    }
                    try {
                        wait((delayNanos + 999_999) / 1_000_000);
                    } catch (InterruptedException e) {
                        if (isShutdown()) {
                            mRunning = false;
                            return;
                        }
                    }
                }

                if (delayed != mDelayed.remove()) {
                    mRunning = false;
                    throw new AssertionError();
                }
            }

            try {
                doExecute(delayed);
            } catch (Throwable e) {
                synchronized (this) {
                    mRunning = false;
                }
                throw e;
            }
        }
    }
}
