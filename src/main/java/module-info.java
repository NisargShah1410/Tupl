
/**
 * See {@link org.cojen.tupl.Database} to get started with Tupl.
 */
module org.cojen.tupl {
    exports org.cojen.tupl;
    exports org.cojen.tupl.diag;
    exports org.cojen.tupl.ext;
    exports org.cojen.tupl.io;
    exports org.cojen.tupl.repl;
    exports org.cojen.tupl.tools;
    exports org.cojen.tupl.util;

    requires jdk.unsupported;

    requires com.sun.jna;
    requires com.sun.jna.platform;

    requires org.cojen.maker;
    requires org.cojen.dirmi;

    // Could be transitive because it's part of the public API, but only in EventListener.
    // I doubt that the java.logging module is used much.
    requires static java.logging;

    requires static java.management;

    requires static org.lz4.java;

    requires static org.slf4j;
}
