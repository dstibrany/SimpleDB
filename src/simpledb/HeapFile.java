package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private TupleDesc td;
    private File file;
    private int tableid;
    private int numPages;
    
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.td = td;
        this.file = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.numPages = (int) f.length() / BufferPool.PAGE_SIZE;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return this.tableid;
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    // Using the file handle, we read a page of bytes at a specific offset
    // then create a new HeapPage using that data, and return it.
    public Page readPage(PageId pid) throws NoSuchElementException {
        long pageOffset = pid.pageno() * BufferPool.PAGE_SIZE;
        byte[] data = new byte[BufferPool.PAGE_SIZE];

        if (pid.pageno() > this.numPages()) {
            throw new NoSuchElementException();
        }

        try {
            // add a new blank page to the HeapFile
            if (pid.pageno() == this.numPages()) {
                this.numPages++;
                return new HeapPage((HeapPageId) pid, HeapPage.createEmptyPageData());
            // read the existing page from disk
            } else {
                RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
                randomAccessFile.seek(pageOffset);
                randomAccessFile.read(data);
                randomAccessFile.close();
                return new HeapPage((HeapPageId) pid, data);
            }
        } catch (IOException e) {
            throw new NoSuchElementException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();
        long pageOffset = pid.pageno() * BufferPool.PAGE_SIZE;
        RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
        randomAccessFile.seek(pageOffset);
        randomAccessFile.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> modifiedPages = new ArrayList<>();
        HeapPage pageWithSpace = null;

        // find a page that has an space for a new tuple
        for (int currentPageNo = 0; currentPageNo < this.numPages(); currentPageNo++) {
            HeapPageId pageId = new HeapPageId(this.getId(), currentPageNo);
            HeapPage currentPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (currentPage.getNumEmptySlots() > 0) {
                pageWithSpace = currentPage;
                break;
            }
        }

        // add tuple to a page with space
        if (pageWithSpace != null) {
            pageWithSpace.addTuple(t);
            modifiedPages.add(pageWithSpace);
        //  create a new blank page and add the tuple
        } else {
            HeapPageId newPageId = new HeapPageId(this.getId(), this.numPages());
            HeapPage newPage = (HeapPage)Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
            newPage.addTuple(t);
            modifiedPages.add(newPage);
        }

        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();

        HeapPage pageToDeleteFrom = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        pageToDeleteFrom.deleteTuple(t);

        return pageToDeleteFrom;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }
    
}

