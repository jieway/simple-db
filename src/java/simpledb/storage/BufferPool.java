package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

    private  LRUCache<PageId, Page> lruCache;

    private final LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.lruCache = new LRUCache<>(numPages);
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // 先上锁
        int lockType = perm == Permissions.READ_ONLY ? 0 : 1;
        int timeout  = new Random().nextInt(2000) + 1000;
        if (!this.lockManager.tryAcquireLock(pid , tid, lockType, timeout)) {
            throw new TransactionAbortedException();
        }
        // 先看缓存中有没有
        Page page = this.lruCache.get(pid);
        if (page != null) {
            return page;
        }
        // 缓存中没有就从磁盘中获取
        return loadPageAndCache(pid);
    }

    private Page loadPageAndCache(PageId pid) throws DbException {
        // 缓存中没有那么就去 DbFile 取并存入缓存中，然后更新该页的优先级
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page page = file.readPage(pid);
        // 将新 page 加入缓存中，如果缓存空间不够
        if (this.lruCache.getSize() == this.lruCache.getMaxSize()) {
            evictPage();
        }
        this.lruCache.put(pid, page);
        return page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            if (commit) {
                // 将所有页面写入磁盘
                flushPages(tid);
            } else {
                // 重新加载所有页
                reLoadPages(tid);
            }
            // 最后, 释放持有的锁
            this.lockManager.releaseLockByTxn(tid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  ReLoad all pages of the specified transaction from disk.
     *  @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void reLoadPages(TransactionId tid) throws IOException, DbException {
        // some code goes here
        // not necessary for lab1|lab2
        final Iterator<Page> pageIterator = this.lruCache.valueIterator();
        while (pageIterator.hasNext()) {
            final Page page = pageIterator.next();
            if (page.isDirty() == tid) {
                discardPage(page.getId());
                loadPageAndCache(page.getId());
            }
        }
    }
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pgs = dbFile.insertTuple(tid, t);
        for (Page page : pgs) {
            this.lruCache.put(page.getId(), page);
            page.markDirty(true, tid);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        if (recordId == null) {
            throw new DbException("recordId is null when delete tuple");
        }
        DbFile dbFile = Database.getCatalog().getDatabaseFile(recordId.getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for (Page page : pages) {
            dbFile.writePage(page);
            page.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.lruCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param page an page to flush
     */
    private synchronized void flushPage(Page page) {
        // some code goes here
        // not necessary for lab1
        try {
            final DbFile tableFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            tableFile.writePage(page);
            page.markDirty(false, null);
        } catch (IOException e) {
            // Todo: add logger
            System.out.println("Error happen when flush page to disk:" + e.getMessage());
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        final Iterator<Page> pageIterator = this.lruCache.valueIterator();
        while (pageIterator.hasNext()) {
            final Page page = pageIterator.next();
            if (page.isDirty() == tid) {
                flushPage(page);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<Page> pageIterator = this.lruCache.reverseIterator();
        while (pageIterator.hasNext()) {
            Page page = pageIterator.next();
            if (page.isDirty() == null) {
                discardPage(page.getId());
                return;
            }
        }
        throw new DbException("All pages are dirty in buffer pool");
    }

}
