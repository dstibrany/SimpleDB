package simpledb;

import org.junit.After;
import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class BufferPoolTest extends SimpleDbTestBase {
    private HeapFile hf;
    private TransactionId tid;
    private TupleDesc td;

    /**
     * Set up initial resources for each unit test.
     */
    @Before public void setUp() throws Exception {
        hf = SystemTestUtil.createRandomHeapFile(2, 1512, null, null);
        td = Utility.getTupleDesc(2);
        tid = new TransactionId();
    }

    @After public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for BufferPool.findLRUPage()
     */
    @Test public void testFindLRUPage() throws Exception {
        BufferPool pool = new BufferPool(3);

        Page lruPage = pool.getPage(tid, new HeapPageId(hf.getId(), 0), Permissions.READ_ONLY);
        for (int i = 1; i < hf.numPages(); i++) {
            pool.getPage(tid, new HeapPageId(hf.getId(), i), Permissions.READ_ONLY);
        }

        assertTrue(lruPage.equals(pool.findLRUPage()));
    }

    /**
     * Unit test for BufferPool.findLRUPage() with the first page being evicted
     *
     */
    @Test public void testFindLRUPageWithEviction() throws Exception {
        BufferPool pool = new BufferPool(2);
        Page lruPage = null;

        for (int i = 0; i < hf.numPages(); i++) {
            Page page = pool.getPage(tid, new HeapPageId(hf.getId(), i), Permissions.READ_ONLY);
            if (i == 1) lruPage = page;
        }

        assertTrue(lruPage.equals(pool.findLRUPage()));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(BufferPoolTest.class);
    }
}

