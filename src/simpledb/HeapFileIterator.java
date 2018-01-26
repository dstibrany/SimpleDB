package simpledb;
import java.util.*;

/**
 * HeapFileIterator implements DbFileIterator and is used to
 * iterate through all tuples in a DbFile.
 */
public class HeapFileIterator extends AbstractDbFileIterator {
    private HeapFile heapFile;
    private TransactionId tid;
    private int nextPageNo;
    private Iterator<Tuple> tupleIterator;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.nextPageNo = 0;
        this.tupleIterator = null;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() 
        throws DbException, TransactionAbortedException {
        this.tupleIterator = this.getNextPageIterator();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind()
        throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        super.close();
        this.nextPageNo = 0;
        this.tupleIterator = null;
    }

    protected Tuple readNext()
            throws DbException, TransactionAbortedException {
        if (this.tupleIterator == null) return null;

        if (this.tupleIterator.hasNext()) {
            return this.tupleIterator.next();
        } else if (this.nextPageNo < this.heapFile.numPages()) {
            this.tupleIterator = this.getNextPageIterator();
            if (this.tupleIterator.hasNext()) {
                return this.tupleIterator.next();
            }
        }

        return null;
    }

    private Iterator<Tuple> getNextPageIterator() throws DbException, TransactionAbortedException {
        return getNextPage().iterator();
    }

    private HeapPage getNextPage() throws DbException, TransactionAbortedException {
        HeapPageId pageId = new HeapPageId(this.heapFile.getId(), this.nextPageNo);
        this.nextPageNo++;
        return (HeapPage) Database.getBufferPool().getPage(this.tid, pageId, Permissions.READ_ONLY);
    }
}
