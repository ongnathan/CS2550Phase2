package memoryManager.mainMemory;

public class PageID {
	

	//wrapper class to keep track of dirtiness of a pageid in memory



	public boolean dirty;
	public PageID pid;
	
	
    public PageID(PageID pageid) {
        this.pid = pageid;
        dirty = false;
    } 
	
	public PageID getPageId() {
		// TODO Auto-generated method stub
		return pid;
	}
	
}
