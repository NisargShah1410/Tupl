/*
 *  Copyright (C) 2022 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.remote;

import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.NoReply;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteCompactionObserver extends Remote {
    public boolean indexBegin(long indexId) throws RemoteException;

    public boolean indexComplete(long indexId) throws RemoteException;

    /**
     * Writes a node id down the pipe for each index node.
     *
     * The client can stop verification by closing the pipe.
     */
    public Pipe indexNodeVisited(Pipe pipe) throws RemoteException;

    @NoReply
    @Disposer
    public void finished() throws RemoteException;
}
