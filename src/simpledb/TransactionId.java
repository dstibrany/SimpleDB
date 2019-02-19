package simpledb;

import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId {
    static AtomicLong counter = new AtomicLong(0);
    long myid;

    public TransactionId() {
        myid = counter.getAndIncrement();
    }

    public long getId() {
        return myid;
    }

    public boolean equals(Object tid) {
        if (this == tid) return true;
        if (tid == null || getClass() != tid.getClass()) return false;
        TransactionId that = (TransactionId) tid;
        return myid == that.myid;
    }

    public int hashCode() {
        return (int) myid;
    }
}
