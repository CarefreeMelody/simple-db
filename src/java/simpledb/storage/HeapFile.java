package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(file, "r");
            if ((pgNo + 1) * BufferPool.getPageSize() > f.length()) {
                f.close();
                throw new IllegalArgumentException(String.format("table %d's page %d is invalid", tableId, pgNo));
            }
            byte[] data = new byte[BufferPool.getPageSize()];
            f.seek(pgNo * BufferPool.getPageSize());
            int len = f.read(data, 0, BufferPool.getPageSize());
            if (len != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d's page %d read %d bytes from dist", tableId, pgNo, len));
            }
            HeapPageId id = new HeapPageId(tableId, pgNo);
            return new HeapPage(id, data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d's page %d is invalid", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = page.getId();
        int pgNo = pageId.getPageNumber();

        final int pageSize = BufferPool.getPageSize();
        byte[] pgData = page.getPageData();

        RandomAccessFile f = new RandomAccessFile(file, "rws");
        f.seek(pgNo * pageSize);
        f.write(pgData);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     * numPages is dynamic change, depends of size of corresponding file on the disk
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(file.length() * 1.0D / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        // traverse each page on the BufferPool
        for (int pgNo = 0; pgNo < numPages(); ++pgNo) {
            HeapPageId pageId = new HeapPageId(getId(), pgNo);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() == 0) {
                Database.getBufferPool().unsafeReleasePage(tid, pageId);
            }else {
                heapPage.insertTuple(t);
                pages.add(heapPage);
                return pages;
            }
        }

        //if there is no empty slot in all pages, then allocate a new empty page
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        bos.write(emptyPageData);
        bos.close();

        HeapPageId newPageId = new HeapPageId(getId(), numPages() - 1);
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
        newPage.insertTuple(t);
        pages.add(newPage);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        pages.add(heapPage);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private final int numPages;
        private final int tableId;
        private int whichPage;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.numPages = numPages();
            this.tableId = getId();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.whichPage = 0;
            this.it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException{
            if (pageNumber >= 0 && pageNumber < numPages) {
                HeapPageId pid = new HeapPageId(tableId, pageNumber);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException(String.format("heapfile %d don't have page %d!", tableId, pageNumber));
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException, DbException{
            if (it == null) {
                return false;
            }

            while (!it.hasNext()) {
                if (whichPage + 1 < numPages) {
                    ++whichPage;
                    it = getPageTuples(whichPage);
                    //return it.hasNext();
                }else {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Tuple next() throws TransactionAbortedException, DbException {
            if (it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws TransactionAbortedException, DbException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }
}
