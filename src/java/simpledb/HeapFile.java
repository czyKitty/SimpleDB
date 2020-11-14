package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private File hpfile;
	private TupleDesc td;
	private int tbid; //table id associated with this heap

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.hpfile = f;
    	this.td = td;
    	this.tbid = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.hpfile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return this.tbid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	try {
    		RandomAccessFile raf = new RandomAccessFile(this.hpfile,"r");
    		byte[] page = new byte[BufferPool.getPageSize()];
    		raf.seek(pid.pageNumber() * BufferPool.getPageSize());
    		raf.read(page);
    		raf.close();
        	HeapPage hpage = new HeapPage (new HeapPageId (pid.getTableId(),pid.pageNumber()),page);
        	return hpage;
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(this.hpfile, "rw");
        raf.seek(BufferPool.getPageSize() * page.getId().pageNumber());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(hpfile.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (!t.getTupleDesc().equals(td)) throw new DbException("wrong tupledesc");
        int i=0;
        for (i = 0; i < numPages(); i++) {
            HeapPage hp = (HeapPage)(Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), i),Permissions.READ_ONLY));
            if (hp.getNumEmptySlots() > 0) break;
        }
        if (i == numPages()) {
            HeapPage newPage = new HeapPage(new HeapPageId(getId(), i), HeapPage.createEmptyPageData());
            writePage(newPage);
        }
        HeapPage hp = (HeapPage)(Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), i),Permissions.READ_WRITE));
        hp.insertTuple(t);
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(hp);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
		if (tbid != t.getRecordId().getPageId().getTableId()) 
			throw new DbException("wrong tableid");
        if (t.getRecordId().getPageId().pageNumber() < 0 || t.getRecordId().getPageId().pageNumber() >= numPages()) 
        	throw new DbException("wrong page number");
        HeapPage hp = (HeapPage)(Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),
                Permissions.READ_WRITE));
        hp.deleteTuple(t);
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(hp);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HfIterator(tid,tbid);
    }

    public class HfIterator implements DbFileIterator{


    	private Iterator<Tuple> tupleiterator; //iterator of a tuples on a heap page
    	private int pointer;
    	private TransactionId transid;
    	private int tbid;	
    	public HfIterator(TransactionId tid, int id) {
    		this.transid = tid;
    		this.tbid = id;
       	}
    	
		public void open() throws DbException, TransactionAbortedException {
			pointer = 0;
			try {
			HeapPageId hpid = new HeapPageId (tbid,pointer);
			HeapPage hp = (HeapPage) Database.getBufferPool().getPage(transid, hpid, Permissions.READ_ONLY);
			tupleiterator = hp.iterator();
			}
			catch(Exception e) {
				throw new DbException("Problems opening/accessing the database");
			}
		}
		
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if (tupleiterator == null) return false;
			if (tupleiterator.hasNext()) return true;
			while ((pointer+1)< numPages()) {
				pointer++;
				HeapPageId hpid = new HeapPageId (tbid,pointer);
				HeapPage hp = (HeapPage) Database.getBufferPool().getPage(transid, hpid, Permissions.READ_ONLY);
				tupleiterator = hp.iterator();
				if(tupleiterator.hasNext()) {
					return true;
				}
			}
			return false;
		}
		

		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (!hasNext()) throw new NoSuchElementException();
			return tupleiterator.next();
			
		}

		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
		}

		public void close() {
			pointer = -1;
			tupleiterator = null;
		}
    }
}

