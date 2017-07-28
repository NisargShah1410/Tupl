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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.HashMap;
import java.util.Map;

import java.util.function.LongPredicate;

import java.util.zip.Checksum;

import org.cojen.tupl.io.CRC32C;
import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.io.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ChannelManager {
    /*
      New connection header structure: (little endian fields)

      0:  Magic number (long)
      8:  Auth code (long)
      16: Member id (long)       -- 0: anonymous
      24: Connection type (int)  -- 0: replication RPC; FIXME: use a bit to enable CRCs
      28: CRC32C (int)

      Command header structure: (little endian fields)

      0:  Command length (3 bytes)  -- excludes the 8-byte command header itself
      3:  Opcode (byte)
      4:  Bytes read delta (uint)   -- first command sends 0

    */

    private static final long MAGIC_NUMBER = 480921776540805866L;
    private static final int CONNECT_TIMEOUT_MILLIS = 5000;
    private static final int RECONNECT_DELAY_MILLIS = 1000;
    private static final int INITIAL_READ_TIMEOUT_MILLIS = 1000;
    private static final int INIT_HEADER_SIZE = 32;

    private static final int
        OP_NOP          = 0, //OP_NOP_REPLY = 1,
        OP_REQUEST_VOTE = 2, OP_REQUEST_VOTE_REPLY = 3,
        OP_QUERY_TERMS  = 4, OP_QUERY_TERMS_REPLY  = 5,
        OP_QUERY_DATA   = 6, OP_QUERY_DATA_REPLY   = 7,
        OP_WRITE_DATA   = 8, OP_WRITE_DATA_REPLY   = 9;

    private final Scheduler mScheduler;
    private final long mAuth;
    private final Map<Long, Peer> mPeerMap;

    private ServerSocket mServerSocket;
    private Channel mLocalServer;

    private long mLocalMemberId;
    
    ChannelManager(Scheduler scheduler, long auth) {
        if (scheduler == null) {
            throw new IllegalArgumentException();
        }
        mScheduler = scheduler;
        mAuth = auth;
        mPeerMap = new HashMap<>();
    }

    /**
     * Set the local member id to a non-zero value, which cannot be changed later.
     */
    synchronized void setLocalMemberId(long localMemberId) {
        if (localMemberId == 0) {
            throw new IllegalArgumentException();
        }
        if (mLocalMemberId != 0) {
            throw new IllegalStateException();
        }
        mLocalMemberId = localMemberId;
    }

    synchronized long getLocalMemberId() {
        return mLocalMemberId;
    }

    /**
     * Starts accepting incoming channels, but does nothing if already started.
     */
    synchronized boolean start(SocketAddress listenAddress, Channel localServer)
        throws IOException
    {
        if (mLocalMemberId == 0) {
            throw new IllegalStateException();
        }

        if (mServerSocket != null) {
            return false;
        }

        ServerSocket ss = new ServerSocket();

        try {
            ss.setReuseAddress(true);
            ss.bind(listenAddress);
            execute(this::acceptLoop);
        } catch (Throwable e) {
            closeQuietly(null, ss);
            throw e;
        }

        mServerSocket = ss;
        mLocalServer = localServer;

        return true;
    }

    /**
     * Stop accepting incoming channels, close all existing channels, and disconnect all remote
     * members.
     */
    synchronized boolean stop() {
        if (mServerSocket == null) {
            return false;
        }

        closeQuietly(null, mServerSocket);
        mServerSocket = null;

        mLocalServer = null;

        /* FIXME: track all client and server channels
        for (Client client : mClients) {
            client.disconnect();
        }

        mClients.clear();
        */
        return true;
    }

    private void execute(Runnable task) {
        if (!mScheduler.execute(task)) {
            stop();
        }
    }

    private void schedule(Runnable task, long delayMillis) {
        if (!mScheduler.schedule(task, delayMillis)) {
            stop();
        }
    }

    /**
     * Immediately return a shared channel to the given peer, which is connected in the
     * background.
     *
     * @throws IllegalStateException if peer is already connected to a different address
     */
    Channel connect(Peer peer, Channel localServer) {
        long remoteMemberId = peer.mMemberId;
        SocketAddress remoteAddress = peer.mAddress;

        if (remoteMemberId == 0 || remoteAddress == null || localServer == null) {
            throw new IllegalArgumentException();
        }

        ClientChannel client = new ClientChannel(peer, localServer);

        synchronized (this) {
            if (mLocalMemberId == 0) {
                throw new IllegalStateException("Local member id isn't set");
            }

            if (mServerSocket == null) {
                throw new IllegalStateException("Not started");
            }

            if (mLocalMemberId == remoteMemberId) {
                throw new IllegalArgumentException("Cannot connect to self");
            }

            Long key = peer.mMemberId;
            Peer existing = mPeerMap.get(key);

            if (existing != null && !existing.mAddress.equals(peer.mAddress)) {
                throw new IllegalStateException("Already connected with a different address");
            }

            mPeerMap.put(key, peer);
        }

        execute(client::connect);

        return client;
    }

    /**
     * Immediately connect to the given address and return an unregistered channel. Any failure
     * to write over the channel triggers an automatic reconnect in the background.
     */
    Channel connectAnonymous(SocketAddress remoteAddress, Channel localServer) throws IOException {
        // FIXME: connectAnonymous
        throw null;
    }

    /**
     * Disconnect any channel for the given remote member, and disallow any new connections
     * from it.
     */
    void disconnect(long remoteMemberId) {
        /* FIXME
        if (remoteMemberId == 0) {
            throw new IllegalArgumentException();
        }

        Client client = new Client(remoteMemberId);

        synchronized (this) {
            client = mClients.floor(client); // findLe
            if (client == null || client.mRemoteMemberId != remoteMemberId) {
                return;
            }
            mClients.remove(client);
        }

        client.disconnect();
        */
    }

    /**
     * Iterates over all the channels and passes the remote member id to the given tester to
     * decide if the channel should be disconnected.
     */
    void disconnect(LongPredicate tester) {
        // FIXME: disconnect
        throw null;
    }

    private void acceptLoop() {
        ServerSocket ss;
        Channel localServer;
        synchronized (this) {
            ss = mServerSocket;
            localServer = mLocalServer;
        }

        if (ss == null) {
            return;
        }

        while (true) {
            try {
                doAccept(ss, localServer);
            } catch (Throwable e) {
                synchronized (this) {
                    if (ss != mServerSocket) {
                        return;
                    }
                }
                uncaught(e);
                Thread.yield();
            }
        }
    }

    private void doAccept(final ServerSocket ss, final Channel localServer) throws IOException {
        Socket s = ss.accept();

        try {
            byte[] header = readHeader(s);
            if (header == null) {
                return;
            }

            long remoteMemberId = decodeLongLE(header, 16);
            int connectionType = decodeIntLE(header, 24);

            if (connectionType != 0) {
                // Connection type field is unused.
                closeQuietly(null, s);
                return;
            }

            Peer peer;
            synchronized (this) {
                if (remoteMemberId == 0) {
                    peer = new Peer(0, s.getRemoteSocketAddress());
                } else {
                    peer = mPeerMap.get(remoteMemberId);
                    if (peer == null) {
                        // Unknown member.
                        closeQuietly(null, s);
                        return;
                    }
                }

                encodeLongLE(header, 16, mLocalMemberId);
            }

            ServerChannel server = new ServerChannel(peer, localServer);
            encodeHeaderCrc(header);

            execute(() -> {
                try {
                    s.getOutputStream().write(header);
                    server.connected(s);
                } catch (IOException e) {
                    closeQuietly(null, s);
                }
            });
        } catch (Throwable e) {
            closeQuietly(null, s);
            throw e;
        }
    }

    static void encodeHeaderCrc(byte[] header) {
        Checksum crc = CRC32C.newInstance();
        crc.update(header, 0, header.length - 4);
        encodeIntLE(header, header.length - 4, (int) crc.getValue());
    }

    /**
     * @return null if invalid
     */
    byte[] readHeader(Socket s) {
        check: try {
            s.setSoTimeout(INITIAL_READ_TIMEOUT_MILLIS);

            byte[] header = new byte[INIT_HEADER_SIZE];
            readFully(s.getInputStream(), header, 0, header.length);

            if (decodeLongLE(header, 0) != MAGIC_NUMBER) {
                break check;
            }

            if (decodeLongLE(header, 8) != mAuth) {
                break check;
            }

            Checksum crc = CRC32C.newInstance();
            crc.update(header, 0, header.length - 4);
            if (decodeIntLE(header, header.length - 4) != (int) crc.getValue()) {
                break check;
            }

            s.setSoTimeout(0);
            s.setTcpNoDelay(true);

            return header;
        } catch (IOException e) {
            // Ignore and close socket.
        }

        closeQuietly(null, s);
        return null;
    }

    abstract class SocketChannel extends Latch implements Channel {
        final Peer mPeer;
        private Channel mLocalServer;
        protected Socket mSocket;
        private OutputStream mOut;
        private ChannelInputStream mIn;

        SocketChannel(Peer peer, Channel localServer) {
            mPeer = peer;
            mLocalServer = localServer;
        }

        void connect() {
            SocketAddress remoteAddress;
            InputStream in;
            synchronized (this) {
                remoteAddress = mPeer.mAddress;
                if (remoteAddress == null) {
                    return;
                }
                in = mIn;
            }

            Socket s = new Socket();

            doConnect: try {
                s.connect(remoteAddress, CONNECT_TIMEOUT_MILLIS);

                byte[] header = new byte[INIT_HEADER_SIZE];
                encodeLongLE(header, 0, MAGIC_NUMBER);
                encodeLongLE(header, 8, mAuth);
                encodeLongLE(header, 16, getLocalMemberId());

                encodeHeaderCrc(header);

                s.getOutputStream().write(header);

                header = readHeader(s);
                if (header == null) {
                    break doConnect;
                }

                if (decodeLongLE(header, 16) != mPeer.mMemberId) {
                    break doConnect;
                }

                int connectionType = decodeIntLE(header, 24);

                if (connectionType != 0) {
                    // Connection type field is unused.
                    break doConnect;
                }

                connected(s);
                return;
            } catch (IOException e) {
                // Ignore and close socket.
            }

            closeQuietly(null, s);
            reconnect(in);
        }

        void reconnect(InputStream existing) {
            Channel localServer;
            Socket s;
            synchronized (this) {
                if (existing != mIn) {
                    // Already reconnected or in progress.
                    return;
                }
                localServer = mLocalServer;
                s = mSocket;
                mSocket = null;
                mOut = null;
                mIn = null;
            }
            
            closeQuietly(null, s);

            if (localServer != null) {
                schedule(this::connect, RECONNECT_DELAY_MILLIS);
            }
        }

        // FIXME: Rename to close.
        void disconnect() {
            Socket s;
            synchronized (this) {
                mLocalServer = null;
                s = mSocket;
                mSocket = null;
                mOut = null;
                mIn = null;
            }

            closeQuietly(null, s);
        }

        synchronized boolean connected(Socket s) {
            OutputStream out;
            ChannelInputStream in;
            try {
                out = s.getOutputStream();
                in = new ChannelInputStream(s.getInputStream(), 8192);
            } catch (Throwable e) {
                closeQuietly(null, s);
                return false;
            }

            closeQuietly(null, mSocket);

            acquireExclusive();
            mSocket = s;
            mOut = out;
            mIn = in;
            releaseExclusive();

            execute(this::inputLoop);
            return true;
        }

        private void inputLoop() {
            Channel localServer;
            ChannelInputStream in;
            synchronized (this) {
                localServer = mLocalServer;
                in = mIn;
            }

            if (localServer == null || in == null) {
                return;
            }

            try {
                while (true) {
                    long header = in.readLongLE();

                    // FIXME: Process the read delta such that Channels can be ordered by
                    // number of outstanding bytes, in a PriorityQueue.
                    long readDelta = header >>> 32;

                    int opAndLength = (int) header;
                    int commandLength = (opAndLength >> 8) & 0xffffff;
                    int op = opAndLength & 0xff;

                    switch (op) {
                    case OP_NOP:
                        localServer.nop(this);
                        break;
                    case OP_REQUEST_VOTE:
                        localServer.requestVote(this, in.readLongLE(), in.readLongLE(),
                                                in.readLongLE(), in.readLongLE());
                        commandLength -= 8 * 4;
                        break;
                    case OP_REQUEST_VOTE_REPLY:
                        localServer.requestVoteReply(this, in.readLongLE());
                        commandLength -= 8;
                        break;
                    case OP_QUERY_TERMS:
                        localServer.queryTerms(this, in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 2);
                        break;
                    case OP_QUERY_TERMS_REPLY:
                        localServer.queryTermsReply(this, in.readLongLE(),
                                                    in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 3);
                        break;
                    case OP_QUERY_DATA:
                        localServer.queryData(this, in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 2);
                        break;
                    case OP_QUERY_DATA_REPLY:
                        long prevTerm = in.readLongLE();
                        long term = in.readLongLE();
                        long index = in.readLongLE();
                        commandLength -= (8 * 3);
                        byte[] data = new byte[commandLength];
                        readFully(in, data, 0, data.length);
                        localServer.queryDataReply(this, prevTerm, term, index, data);
                        commandLength = 0;
                        break;
                    case OP_WRITE_DATA:
                        prevTerm = in.readLongLE();
                        term = in.readLongLE();
                        index = in.readLongLE();
                        long highestIndex = in.readLongLE();
                        long commitIndex = in.readLongLE();
                        commandLength -= (8 * 5);
                        data = new byte[commandLength];
                        readFully(in, data, 0, data.length);
                        localServer.writeData(this, prevTerm, term, index,
                                              highestIndex, commitIndex, data);
                        commandLength = 0;
                        break;
                    case OP_WRITE_DATA_REPLY:
                        localServer.writeDataReply(this, in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 2);
                        break;
                    default:
                        System.out.println("unknown op: " + op);
                        break;
                    }

                    in.skipFully(commandLength);
                }
            } catch (IOException e) {
                // Ignore.
            } catch (Throwable e) {
                uncaught(e);
            }

            reconnect(in);
        }

        @Override
        public Peer peer() {
            return mPeer;
        }

        @Override
        public boolean nop(Channel from) {
            acquireExclusive();
            try {
                if (mOut == null) {
                    return false;
                }
                byte[] command = new byte[8];
                prepareCommand(command, OP_NOP, 0, 0);
                return writeCommand(command, 0, command.length);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean requestVote(Channel from, long term, long candidateId,
                                   long highestTerm, long highestIndex)
        {
            acquireExclusive();
            try {
                if (mOut == null) {
                    return false;
                }
                byte[] command = new byte[8 + 8 * 4];
                prepareCommand(command, OP_REQUEST_VOTE, 0, 8 * 4);
                encodeLongLE(command, 8, term);
                encodeLongLE(command, 16, candidateId);
                encodeLongLE(command, 24, highestTerm);
                encodeLongLE(command, 32, highestIndex);
                return writeCommand(command, 0, command.length);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean requestVoteReply(Channel from, long term) {
            acquireExclusive();
            try {
                if (mOut == null) {
                    return false;
                }
                byte[] command = new byte[8 + 8];
                prepareCommand(command, OP_REQUEST_VOTE_REPLY, 0, 8);
                encodeLongLE(command, 8, term);
                return writeCommand(command, 0, command.length);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean queryTerms(Channel from, long startIndex, long endIndex) {
            return query(OP_QUERY_TERMS, startIndex, endIndex);
        }

        private boolean query(int op, long startIndex, long endIndex) {
            acquireExclusive();
            try {
                if (mOut == null) {
                    return false;
                }
                byte[] command = new byte[8 + 8 * 2];
                prepareCommand(command, op, 0, 8 * 2);
                encodeLongLE(command, 8, startIndex);
                encodeLongLE(command, 16, endIndex);
                return writeCommand(command, 0, command.length);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean queryTermsReply(Channel from, long prevTerm, long term, long startIndex) {
            acquireExclusive();
            try {
                if (mOut == null) {
                    return false;
                }
                byte[] command = new byte[8 + 8 * 3];
                prepareCommand(command, OP_QUERY_TERMS_REPLY, 0, 8 * 3);
                encodeLongLE(command, 8, prevTerm);
                encodeLongLE(command, 16, term);
                encodeLongLE(command, 24, startIndex);
                return writeCommand(command, 0, command.length);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean queryData(Channel from, long startIndex, long endIndex) {
            return query(OP_QUERY_DATA, startIndex, endIndex);
        }

        @Override
        public boolean queryDataReply(Channel from, long prevTerm, long term, long index,
                                      byte[] data)
        {
            if (data.length > ((1 << 24) - (8 * 3))) {
                // FIXME: break it up into several commands
                throw new IllegalArgumentException("Too large");
            }

            acquireExclusive();
            try {
                if (mOut == null) {
                    return false;
                }
                byte[] command = new byte[(8 + 8 * 3) + data.length];
                prepareCommand(command, OP_QUERY_DATA_REPLY, 0, command.length - 8);
                encodeLongLE(command, 8, prevTerm);
                encodeLongLE(command, 16, term);
                encodeLongLE(command, 24, index);
                System.arraycopy(data, 0, command, 32, data.length);
                return writeCommand(command, 0, command.length);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean writeData(Channel from, long prevTerm, long term, long index,
                                 long highestIndex, long commitIndex, byte[] data)
        {
            if (data.length > ((1 << 24) - (8 * 5))) {
                // FIXME: break it up into several commands
                throw new IllegalArgumentException("Too large");
            }

            acquireExclusive();
            try {
                if (mOut == null) {
                    return false;
                }
                byte[] command = new byte[(8 + 8 * 5) + data.length];
                prepareCommand(command, OP_WRITE_DATA, 0, command.length - 8);
                encodeLongLE(command, 8, prevTerm);
                encodeLongLE(command, 16, term);
                encodeLongLE(command, 24, index);
                encodeLongLE(command, 32, highestIndex);
                encodeLongLE(command, 40, commitIndex);
                System.arraycopy(data, 0, command, 48, data.length);
                return writeCommand(command, 0, command.length);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean writeDataReply(Channel from, long term, long highestIndex) {
            acquireExclusive();
            try {
                if (mOut == null) {
                    return false;
                }
                byte[] command = new byte[8 + 8 * 2];
                prepareCommand(command, OP_WRITE_DATA_REPLY, 0, 8 * 2);
                encodeLongLE(command, 8, term);
                encodeLongLE(command, 16, highestIndex);
                return writeCommand(command, 0, command.length);
            } finally {
                releaseExclusive();
            }
        }

        /**
         * Caller must hold exclusive latch.
         *
         * @param command must have at least 8 bytes, used for the header
         * @param length max allowed is 16,777,216 bytes
         */
        private void prepareCommand(byte[] command, int op, int offset, int length) {
            ChannelInputStream in = mIn;
            long bytesRead = in == null ? 0 : in.resetReadAmount();

            if (Long.compareUnsigned(bytesRead, 1L << 32) >= 0) {
                // Won't fit in the field, so send a bunch of nops.
                encodeIntLE(command, offset, 0);
                encodeIntLE(command, offset + 4, (int) ((1L << 32) - 1));
                do {
                    bytesRead -= ((1L << 32) - 1);
                    writeCommand(command, offset, 8);
                } while (Long.compareUnsigned(bytesRead, 1L << 32) >= 0);
            }

            encodeIntLE(command, offset, (length << 8) | (byte) op);
            encodeIntLE(command, offset + 4, (int) bytesRead);
        }

        /**
         * Caller must hold exclusive latch and have verified that mOut isn't null.
         */
        private boolean writeCommand(byte[] command, int offset, int length) {
            try {
                mOut.write(command, offset, length);
                return true;
            } catch (IOException e) {
                mOut = null;
                // Reconnect will be attempted by inputLoop.
                closeQuietly(null, mSocket);
                return false;
            }
        }
    }

    final class ClientChannel extends SocketChannel {
        ClientChannel(Peer peer, Channel localServer) {
            super(peer, localServer);
        }

        @Override
        public String toString() {
            return "ClientChannel: {peer=" + mPeer + ", socket=" + mSocket + '}';
        }
    }

    final class ServerChannel extends SocketChannel {
        ServerChannel(Peer peer, Channel localServer) {
            super(peer, localServer);
        }

        @Override
        void reconnect(InputStream existing) {
            disconnect();
        }

        @Override
        public String toString() {
            return "ServerChannel: {peer=" + mPeer + ", socket=" + mSocket + '}';
        }
    }
}
