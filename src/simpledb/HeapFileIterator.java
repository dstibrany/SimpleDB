package simpledb;
import java.util.*;

/**
 * HeapFileIterator implements DbFileIterator and is used to
 * iterate through all tuples in a DbFile.
 */
public class HeapFileIterator implements DbFileIterator {
    private HeapFile heapFile;
    private TransactionId tid;
    private HeapPage currentPage;
    private Iterator<Tuple> currentPageIterator;

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.currentPage = null;
        this.currentPageIterator = null;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() 
        throws DbException, TransactionAbortedException {
        this.currentPage = this.getNextPageFromBuffer();
        this.currentPageIterator = this.currentPage.iterator();
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        if (this.currentPageIterator == null) return false;

        if (this.currentPageIterator.hasNext()) {
            return true;
        } else {
            return this.hasNextPage();
        }
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
        if (this.currentPageIterator == null) {
            throw new NoSuchElementException("Iterator has not been opened");
        }

        if (this.currentPageIterator.hasNext()) {
            return this.currentPageIterator.next();
        } else {
            // get the next page, if there is one
            if (this.hasNextPage()) {
                this.currentPage = this.getNextPageFromBuffer();
                this.currentPageIterator = this.currentPage.iterator();
                return this.currentPageIterator.next();
            } else {
                throw new NoSuchElementException("There are no more tuples in the file");
            }
        }
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
        this.currentPage = null;
        this.currentPageIterator = null;
    }

    // Returns true if there is still another page to read from the heapFile
    private boolean hasNextPage() {
        return this.currentPage.getId().pageno() < (this.heapFile.numPages() - 1);
    }

    // Get the next page from the buffer pool
    private HeapPage getNextPageFromBuffer() 
        throws TransactionAbortedException, DbException {
        HeapPageId nextPid;

        // get the first (0th) page
        if (this.currentPage == null) {
            nextPid = new HeapPageId(this.heapFile.getId(), 0);
        // increment the current page count by 1
        } else {
            nextPid = new HeapPageId(this.heapFile.getId(), this.currentPage.getId().pageno() + 1);
        }

        return (HeapPage) Database.getBufferPool().getPage(this.tid, nextPid, Permissions.READ_ONLY);
    }
}
