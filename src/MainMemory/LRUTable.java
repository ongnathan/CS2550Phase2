package MainMemory;


import java.util.ArrayList;
import java.util.LinkedList;
import disk.DiskOperation;

/**
 * this class represents the LRU table replacement to keep track of the pages in memory
 * and to decide what pages should be replaced in case the buffers in the memory are full and 
 * some pages have to be evicted 
 */

public class LRUTable
{
	ArrayList<DiskOperation>[] commands; 
	boolean[] dirtybits;
	int[] diskIDs;

	
    //LRU replacement queue 
	private LinkedList<Integer> tLRUQueue;
    
    // Create LRUTable with the number of pages that the table should hold.
    @SuppressWarnings("unchecked")
	public LRUTable(int numOfPages)
    {
    	dirtybits = new boolean[numOfPages];
        tLRUQueue = new LinkedList<Integer>();
        commands = (ArrayList<DiskOperation>[])new ArrayList[numOfPages];
        diskIDs = new int[numOfPages];
        for(int i = 0; i < numOfPages; i++)
        {
        	commands[i] = new ArrayList<DiskOperation>();
        	diskIDs[i] = -1;
        }

    }
    
    /**
     * this method is used to insert a new page into the LRU Table to keep track of pages in the memory, 
     * with the assumption that the LRU still has space.
     * @param pageID the id of the page in memory
     * @param diskID the block id in disk
     */
    
    public void insertNewPageToLRU (int pageID, int diskID)
    {
    	//prep the index for a new page
    	if(this.diskIDs[pageID] != -1)
    	{
    		System.out.println("Cannot use insert into new page, must use evict first");
    		return;
    	}
    	this.tLRUQueue.add(pageID);
    	this.diskIDs[pageID] = diskID;
    	this.dirtybits[pageID] = false;

    }

    /**
     * here if the page in the buffer has been used it will be listed as touched. So, it can
     * be removed to the back of the queue so it won't be the first one to replace
     * @param pageID the page in memory 
     */
    
    public void touchPage(int pageID)
    {
    	if(pageID < 0)
    	{
    		throw new IllegalArgumentException();
    	}
    	
    	//move page to end of queue
    	
    	tLRUQueue.remove((Integer)pageID);
    	tLRUQueue.add(pageID);
    }
    
    /**
    * this is used to identify the page to be evicted from the memory.
    * it returns the pageID to the memory so the memory can evict it to disk
    * @return pageID
    */
    
    public int[] pageToEvict()
    {
    	Integer pageID = tLRUQueue.peek();
    	return new int[] {pageID, this.diskIDs[pageID]};
    }
    
    public Integer[] forceEvict(Integer[] indexes)
    {
    	ArrayList<Integer> diskBlockEvictions = new ArrayList<Integer>();
    	for(Integer i : indexes)
    	{
    		tLRUQueue.remove(i);
    		commands[i] = new ArrayList<DiskOperation>();
    		dirtybits[i] = false;
    		diskBlockEvictions.add(diskIDs[i]);
    		diskIDs[i] = -1;
    	}
    	return diskBlockEvictions.toArray(new Integer[0]);
    }
    
    /**
     * this is to call the operation done on a specific page in the memory so when the page is evicted to 
     * disk the updates on the page can take place
     * @param pageID the id of the page in memory
     * @return the operations done on that given page
     */
    
    public ArrayList<DiskOperation> evict(int pageID)
    {
    	tLRUQueue.remove((Integer)pageID);
    	ArrayList<DiskOperation> operations = commands[pageID];
		commands[pageID] = new ArrayList<DiskOperation>();
		dirtybits[pageID] = false;
		diskIDs[pageID] = -1;
		return operations;
    }
    
    /**
     * to add the disk operation into the index of that page and to 
     * figure if the page is dirty by setting it to dirty
     * @param index
     * @param op
     */
    
    public void addDiskOperation(int index, DiskOperation op)
    {
    	//place op into index
    	commands[index].add(op);
    	//set dirty bit
    	dirtybits[index] = true;
    }
}
