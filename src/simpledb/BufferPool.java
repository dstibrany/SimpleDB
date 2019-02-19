package simpledb;

import com.dstibrany.lockmanager.DeadlockException;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    private int numPages;
    private Map<PageId, Page> pagePool = new ConcurrentHashMap<>();
    private Map<TransactionId, Set<PageId>> transactionPageMap  = new ConcurrentHashMap<>();

    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        try {
            Database.getLockManager().lock(pid.hashCode(), tid.hashCode(), perm.adaptForLockManager());
        } catch (DeadlockException e) {
            throw new TransactionAbortedException();
        }

        Set<PageId> transactionPages = transactionPageMap.getOrDefault(tid, new HashSet<>());
        transactionPages.add(pid);
        transactionPageMap.put(tid, transactionPages);

        if (pagePool.containsKey(pid)) {
            Page page = this.pagePool.get(pid);
            page.updateLastAccessTimestamp();
            return page;
        }

        if (pagePool.size() >= this.numPages) {
            evictPage();
        }

        Page newPage = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);

        pagePool.put(pid, newPage);
            
        return newPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        Database.getLockManager().unlock(pid.hashCode(), tid.hashCode());
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException, TransactionAbortedException, DbException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return Database.getLockManager().hasLock(tid.hashCode(), pid.hashCode());
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {

        Set<PageId> transactionPageIds = transactionPageMap.getOrDefault(tid, Collections.emptySet());
        for (PageId pid: transactionPageIds) {
            if (commit) {
                flushPage(pid);
            } else {
                if (pagePool.get(pid) != null && tid.equals(pagePool.get(pid).isDirty())) {
                    Page restoredPage = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
                    pagePool.put(pid, restoredPage);
                }
            }
        }
        Database.getLockManager().removeTransaction(tid.hashCode());
        transactionPageMap.remove(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> dirtiedPages = file.addTuple(tid, t);
        for (Page dirtiedPage: dirtiedPages) {
            dirtiedPage.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        if (t.getRecordId() == null) {
            System.out.printf("NULL: %s\n", t.toString());
        }
        DbFile file = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
        Page dirtiedPage = file.deleteTuple(tid, t);
        dirtiedPage.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid: this.pagePool.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = this.pagePool.get(pid);

        if (page != null && page.isDirty() != null) {
            DbFile tableFile = Database.getCatalog().getDbFile(pid.getTableId());
            tableFile.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Page lruPage = findLRUPage();

        if (lruPage == null) throw new DbException("There are no pages to evict in the buffer pool");

        try {
            this.flushPage(lruPage.getId());
            this.pagePool.remove(lruPage.getId());
        } catch (IOException e) {
            throw new DbException("Page could not be flushed");
        }
    }

    /**
     * Implements LRU: Finds the page with the oldest timestamp. Note that timestamps are updated each time a page
     * is grabbed from the buffer pool.
     * @return The least recently used page
     */
    Page findLRUPage() {
        Page lruPage = null;
        for (Page page: this.pagePool.values()) {
            // Skip dirty pages for NO STEAL
            if (page.isDirty() != null) {
                continue;
            }

            if (lruPage == null || page.getLastAccessTimestamp() < lruPage.getLastAccessTimestamp()) {
                lruPage = page;
            }
        }

        return lruPage;
    }
}
