/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.filter;

import java.math.BigInteger;

/**
 * Thrown when dnf/cnf transformation isn't possible. The filter expression is too complex.
 *
 * @author Brian S O'Neill
 */
public class ComplexFilterException extends IllegalStateException {
    final BigInteger mNumTerms;
    final long mLimit;

    /**
     * @param numTerms pass null to report the limit param; else report numTerms squared
     * @param limit limit to report; ignored if numTerms isn't null
     */
    ComplexFilterException(BigInteger numTerms, long limit) {
        mNumTerms = numTerms;
        mLimit = limit;
    }

    @Override
    public String getMessage() {
        var bob = new StringBuilder("limit: ");
        if (mNumTerms == null) {
            bob.append(mLimit);
        } else {
            bob.append(mNumTerms.multiply(mNumTerms));
        }
        return bob.toString();
    }
}
