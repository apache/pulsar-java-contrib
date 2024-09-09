package org.apache.pulsar.rpc.contrib.common;

public class PulsarRpcClientException extends Exception {

    /**
     * Constructs an {@code PulsarRpcClientException} with the specified detail message.
     *
     * @param message
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     */
    public PulsarRpcClientException(String message) {
        super(message);
    }

    /**
     * Constructs an {@code PulsarRpcClientException} with the specified cause.
     *
     * @param cause
     *        The cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A null value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     */
    public PulsarRpcClientException(Throwable cause) {
        super(cause);
    }
}
