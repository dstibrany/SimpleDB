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
    private Iterator<Tuple> pageIterator;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.nextPageNo = 0;
        this.pageIterator = null;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() 
        throws DbException, TransactionAbortedException {
        this.pageIterator = this.getNextPageIterator();
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
        this.pageIterator = null;
    }

    protected Tuple readNext()
            throws DbException, TransactionAbortedException {
        if (this.pageIterator == null) return null;

        if (this.pageIterator.hasNext()) {
            return this.pageIterator.next();
        } else if (this.nextPageNo < this.heapFile.numPages()) {
            this.pageIterator = this.getNextPageIterator();
            return this.pageIterator.next();
        } else {
            return null;
        }
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
