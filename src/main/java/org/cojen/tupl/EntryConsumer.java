/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents an operation that accepts a key and value, just like a {@link
 * java.util.function.BiConsumer BiConsumer}.
 *
 * @author Brian S O'Neill
 * @see EntryScanner
 */
@FunctionalInterface
public interface EntryConsumer {
    void accept(byte[] key, byte[] value) throws IOException;

    default EntryConsumer andThen(EntryConsumer after) {
        Objects.requireNonNull(after);

        return (key, value) -> {
            accept(key, value);
            after.accept(key, value);
        };
    }
}
