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

package org.cojen.tupl.repl;

import java.io.Closeable;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.net.ConnectException;

import java.util.Map;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Replicator interface for applications which directly control the replication data, and are
 * also responsible for processing snapshots.
 *
 * @author Brian S O'Neill
 */
public interface DirectReplicator extends Replicator {
    /**
     * Start accepting replication data, to be called for new or existing members. For newly
     * restored members, the start method must be called to update its role.
     */
    void start() throws IOException;

    /**
     * Start by receiving a {@link #requestSnapshot snapshot} from another group member,
     * expected to be called only by newly joined members. New members are initially {@link
     * Role#RESTORING restoring}, so call the start method after restoration to update the role.
     *
     * @param options requested options; can pass null if none
     * @return null if no snapshot could be found and replicator hasn't started
     * @throws ConnectException if a snapshot was found, but requesting it failed
     * @throws IllegalStateException if already started
     */
    SnapshotReceiver restore(Map<String, String> options) throws IOException;

    /**
     * Connect to a remote replication group member, for receiving a database snapshot. An
     * {@link #snapshotRequestAcceptor acceptor} must be installed on the group member being
     * connected to for the request to succeed.
     * 
     * <p>The sender is selected as the one which has the fewest count of active snapshot
     * sessions. If all the counts are the same, then a sender is instead randomly selected,
     * favoring a follower over a leader.
     *
     * @param options requested options; can pass null if none
     * @return null if no snapshot could be found
     * @throws ConnectException if a snapshot was found, but requesting it failed
     */
    SnapshotReceiver requestSnapshot(Map<String, String> options) throws IOException;

    /**
     * Install a callback to be invoked when a snapshot is requested by a new group member.
     *
     * @param acceptor acceptor to use, or pass null to disable
     */
    void snapshotRequestAcceptor(Consumer<SnapshotSender> acceptor);

    /**
     * Returns a new reader which accesses data starting from the given position. The reader
     * returns EOF whenever the end of a term is reached. At the end of a term, try to obtain a
     * new writer to determine if the local member has become the leader.
     *
     * <p>When passing true for the follow parameter, a reader is always provided at the
     * requested position. When passing false for the follow parameter, null is returned if the
     * current member is the leader for the given position.
     *
     * <p><b>Note: Reader instances are not expected to be thread-safe.</b>
     *
     * @param position position to start reading from, known to have been committed
     * @param follow pass true to obtain an active reader, even if local member is the leader
     * @return reader or possibly null when follow is false
     * @throws InvalidReadException if position is lower than the start position, or if
     * position is higher than the commit position
     * @throws IllegalStateException if replicator is closed
     */
    Reader newReader(long position, boolean follow);

    /**
     * Returns a new writer for the leader to write into, or else returns null if the local
     * member isn't the leader. The writer stops accepting messages when the term has ended,
     * and possibly another leader has been elected.
     *
     * <p><b>Note: Writer instances are not expected to be thread-safe.</b>
     *
     * @return writer or null if not the leader
     * @throws IllegalStateException if an existing writer for the current term already exists
     */
    Writer newWriter();

    /**
     * Returns a new writer for the leader to write into, or else returns null if the local
     * member isn't the leader. The writer stops accepting messages when the term has ended,
     * and possibly another leader has been elected.
     *
     * <p><b>Note: Writer instances are not expected to be thread-safe.</b>
     *
     * @param position expected position to start writing from as leader; method returns null
     * if the given position is lower
     * @return writer or null if not the leader
     * @throws IllegalArgumentException if the given position is negative
     * @throws IllegalStateException if an existing writer for the current term already exists
     */
    Writer newWriter(long position);

    /**
     * Returns immediately if all data up to the given committed position is durable, or else
     * durably persists all data up to the highest position.
     *
     * @param position committed position required to be durable
     * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
     * @return false if timed out
     * @throws IllegalStateException if position is too high
     */
    boolean syncCommit(long position, long nanosTimeout) throws IOException;

    /**
     * Permit all data lower than the given position to be deleted, freeing up space in the log.
     *
     * @param position lowest position which must be retained
     */
    void compact(long position) throws IOException;

    /**
     * Direct interface for accessing replication data, for a given term.
     */
    public static interface Accessor extends Closeable {
        /**
         * Returns the fixed term being accessed.
         */
        long term();

        /**
         * Returns the position at the start of the term.
         */
        long termStartPosition();

        /**
         * Returns the current term end position, which is Long.MAX_VALUE if unbounded. The end
         * position is always permitted to retreat, but never lower than the commit position.
         */
        long termEndPosition();

        /**
         * Returns the next log position which will be accessed.
         */
        long position();

        /**
         * Returns the current term commit position, which might be lower than the start
         * position.
         */
        long commitPosition();

        /**
         * Invokes the given task when the commit position reaches the requested position. The
         * current commit position is passed to the task, or -1 if the term ended before the
         * position could be reached. If the task can be run when this method is called, then
         * the current thread invokes it immediately.
         */
        void uponCommit(long position, LongConsumer task);

        /**
         * Invokes the given task when the commit position reaches the end of the term. The
         * current commit position is passed to the task, or -1 if if closed. If the task can be
         * run when this method is called, then the current thread invokes it immediately.
         */
        default void uponEndCommit(LongConsumer task) {
            uponCommit(termEndPosition(), position -> {
                long endPosition = termEndPosition();
                if (endPosition == Long.MAX_VALUE) {
                    // Assume closed.
                    task.accept(-1);
                } else if (position == endPosition) {
                    // End reached.
                    task.accept(position);
                } else {
                    // Term ended even lower, so try again.                        
                    uponEndCommit(task);
                }
            });
        }

        @Override
        void close();
    }

    /**
     * Direct interface for reading from a replicator, for a given term.
     */
    public static interface Reader extends Accessor {
        // This interface is intentionally empty, as a placeholder.
    }

    /**
     * Direct interface for writing to a replicator, for a given term.
     */
    public static interface Writer extends Accessor {
        /**
         * Blocks until the commit position reaches the given position.
         *
         * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
         * @return current commit position, or -1 if deactivated before the position could be
         * reached, or -2 if timed out
         */
        long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException;

        /**
         * Blocks until the commit position reaches the end of the term.
         *
         * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
         * @return current commit position, or -1 if closed before the position could be
         * reached, or -2 if timed out
         */
        default long waitForEndCommit(long nanosTimeout) throws InterruptedIOException {
            long endNanos = nanosTimeout > 0 ? (System.nanoTime() + nanosTimeout) : 0;

            long endPosition = termEndPosition();

            while (true) {
                long position = waitForCommit(endPosition, nanosTimeout);
                if (position == -2) {
                    // Timed out.
                    return -2;
                }
                endPosition = termEndPosition();
                if (endPosition == Long.MAX_VALUE) {
                    // Assume closed.
                    return -1;
                }
                if (position == endPosition) {
                    // End reached.
                    return position;
                }
                // Term ended even lower, so try again.
                if (nanosTimeout > 0) {
                    nanosTimeout = Math.max(0, endNanos - System.nanoTime());
                }
            }
        }
    }
}
