package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;

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

    /** the maximum time to wait a lock which is occupied by another transaction. 
    if the time is exceeded, rollback the transaction. */
    private static final long LOCK_WAIT_TIMEOUT = 2000L;

    private final int numPages;
    private final ConcurrentHashMap<PageId, Node> pageStore;

    private class PageLock {
        public static final int SHARE = 0;
        public static final int EXCLUSIVE = 1;
        private TransactionId tid;
        private int type;

        public PageLock(TransactionId tid, int type) {
            this.tid = tid;
            this.type = type;
        }

        public TransactionId getTid() {
            return this.tid;
        }

        public int getType() {
            return this.type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }

    private class LockManager {
        ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap = new ConcurrentHashMap<>();

        public synchronized boolean acquiredLock(PageId pageId, TransactionId tid, int requiredLockType) 
            throws TransactionAbortedException {
            // if there are no locks on this page
            if (!lockMap.containsKey(pageId)) {
                PageLock pageLock = new PageLock(tid, requiredLockType);
                ConcurrentHashMap<TransactionId, PageLock> pageLocks = new ConcurrentHashMap<>();
                pageLocks.put(tid, pageLock);
                lockMap.put(pageId, pageLocks);
                return true;
            }

            ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pageId);
            if (!pageLocks.containsKey(tid)) {
                if (pageLocks.size() > 1) {
                    if (requiredLockType == PageLock.SHARE) {
                        PageLock pageLock = new PageLock(tid, requiredLockType);
                        pageLocks.put(tid, pageLock);
                        lockMap.put(pageId, pageLocks);
                    }else if (requiredLockType == PageLock.EXCLUSIVE) {
                        // tid try to acquire a exclusive-lock, refuse
                        return false;
                    }
                }else if (pageLocks.size() == 1) {
                    PageLock currLock = null;
                    for (PageLock lock : pageLocks.values()) {
                        currLock = lock;
                    }
                    if (currLock.getType() == PageLock.SHARE) {
                        if (requiredLockType == PageLock.SHARE) {
                            PageLock pageLock = new PageLock(tid, requiredLockType);
                            pageLocks.put(tid, pageLock);
                            lockMap.put(pageId, pageLocks);
                        }else if (requiredLockType == PageLock.EXCLUSIVE) {
                            return false;
                        }
                    }else if (currLock.getType() == PageLock.EXCLUSIVE) {
                        return false;
                    }
                }
            // if the tid has some locks in this page
            }else {
                PageLock currLock = pageLocks.get(tid);
                if (currLock.getType() == PageLock.SHARE) {
                    if (requiredLockType == PageLock.SHARE) {
                        return true;
                    }else if (requiredLockType == PageLock.EXCLUSIVE) {
                        // if the page hold only one lock which is this transaction's share-lock
                        if (pageLocks.size() == 1) {
                            currLock.setType(PageLock.EXCLUSIVE);
                            pageLocks.put(tid, currLock);
                            return true;
                        // other transactions hold locks and only share-locks(because current transaction hold the exclusive-lock)
                        }else if (pageLocks.size() > 1) {
                            throw new TransactionAbortedException();
                        }
                    }
                // current transaction hold a exclusive-lock
                }else if (currLock.getType() == PageLock.EXCLUSIVE){
                    return true;
                }
            }
            return false;
        }

        public synchronized boolean isHoldLock(TransactionId tid, PageId pid) {
            ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pid);
            if (pageLocks == null) {
                return false;
            }
            PageLock pageLock = pageLocks.get(tid);
            if (pageLock == null) {
                return false;
            }
            return true;
        }

        public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
            if (isHoldLock(tid, pid)) {
                ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pid);
                pageLocks.remove(tid);
                if (pageLocks.size() == 0) {
                    lockMap.remove(pid);
                }
                return true;
            }
            return false;
        }

        public synchronized void completeTransaction(TransactionId tid) {
            for (PageId pid : lockMap.keySet()) {
                releaseLock(tid, pid);
            }
        }
    }

    private static class Node {
        PageId pageId;
        Page page;
        Node prev;
        Node next;
        public Node() {}

        public Node(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }

        public PageId getPageId() {
            return this.pageId;
        }

        public Page getPage() {
            return this.page;
        }
    } 

    private Node head;
    private Node tail;

    public void addToHead(Node node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    public void remove(Node node) {
        node.next.prev = node.prev;
        node.prev.next = node.next;
    }

    public void moveToHead(Node node) {
        remove(node);
        addToHead(node);
    }

    public Node removeTail() {
        Node node = tail.prev;
        remove(node);
        return node;
    }

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageStore = new ConcurrentHashMap<>();
        this.head = new Node(new HeapPageId(-1, -1), null);
        this.tail = new Node(new HeapPageId(-1, -1), null);
        this.head.next = tail;
        this.tail.prev = head;
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
        // some code goes here
        int requiredLockType = (perm == Permissions.READ_ONLY) ? PageLock.SHARE : PageLock.EXCLUSIVE;
        long startTime = System.currentTimeMillis();
        boolean isAcquired = false;
        // keep on acquiring the lock
        while (!isAcquired) {
            isAcquired = lockManager.acquiredLock(pid, tid, requiredLockType);
            long currTime = System.currentTimeMillis();
            // a timeout indicates a deadlock occurred
            // XXX:can use graph to detect deadlock
            if (currTime - startTime > LOCK_WAIT_TIMEOUT) {
                throw new TransactionAbortedException();
            }
        }

        if (!pageStore.containsKey(pid)) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            Node node = new Node(pid, page);
            if (pageStore.size() > numPages) {
                evictPage();
            }
            pageStore.put(pid, node);
            addToHead(node);
        }else {
            moveToHead(pageStore.get(pid));
        }
        return pageStore.get(pid).getPage();
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
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(tid, p);
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
        if (commit) {
            try {
                flushPages(tid);
            }catch (IOException e) {
                e.printStackTrace();
            }
        }else {
            transactionRollback(tid);
        }
        
        // release all locks this transaction hold
        lockManager.completeTransaction(tid);
    }

    public synchronized void transactionRollback(TransactionId tid) {
        for (Node node : pageStore.values()) {
            PageId pid = node.getPageId();
            Page page = node.getPage();
            if (tid.equals(page.isDirty())) {
                int tableId = pid.getTableId();
                DbFile table = Database.getCatalog().getDatabaseFile(tableId);
                Page oldPageFromDisk = table.readPage(pid);
                node.page = oldPageFromDisk;
                pageStore.put(pid, node);
                moveToHead(node);
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
        DbFile heapFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = heapFile.insertTuple(tid, t);
        for (Page modifiedPage : pages) {
            PageId pageId = modifiedPage.getId();
            modifiedPage.markDirty(true, tid);
            if (!pageStore.containsKey(pageId)) {
                Node node = new Node(pageId, modifiedPage);
                if (pageStore.size() > numPages) {
                    evictPage();
                }
                addToHead(node);
                pageStore.put(pageId, node);
            }else {
                Node node = pageStore.get(pageId);
                moveToHead(node);
                node.page = modifiedPage;
                pageStore.put(pageId, node);
            }
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
        // is necessary for lab2
        DbFile heapFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = heapFile.deleteTuple(tid, t);
        for (Page modifiedPage : pages) {
            PageId pageId = modifiedPage.getId();
            modifiedPage.markDirty(true, tid);
            if (!pageStore.containsKey(pageId)) {
                Node node = new Node(pageId, modifiedPage);
                if (pageStore.size() >= numPages) {
                    evictPage();
                }
                addToHead(node);
                pageStore.put(pageId, node);
            }else {
                Node node = pageStore.get(pageId);
                moveToHead(node);
                node.page = modifiedPage;
                pageStore.put(pageId, node);
            }
        }
    }

    /**
     * Flush all dirty pages to disk whitout committing transaction
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : pageStore.keySet()) {
            flushPage(pid);
        }
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
        pageStore.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // is necessary for lab2
        Page page = pageStore.get(pid).getPage();
        if (page.isDirty() != null) {
            HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
            heapFile.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Node node : pageStore.values()) {
            PageId pageId = node.getPageId();
            Page page = node.getPage();
            if (tid.equals(page.isDirty())) {
                flushPage(pageId);
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
        // is necessary for lab2
        for (int i = 0; i < numPages; ++i) {
            Node tail = removeTail();
            Page evictedPage = tail.getPage();
            if (evictedPage.isDirty() != null) {
                addToHead(tail);
            }else {
                // it is not allowed to flush dirty pages back to disk
                // before a transaction commits.
                // flushPage(evictedPage.getId());
                discardPage(evictedPage.getId());
                return;
            }
        }
        throw new DbException("All pages are dirty.");
    }

}
