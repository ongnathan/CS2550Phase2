package disk;

import data.Attribute;
import data.IDNumber;

public class SlottedPage<T extends Attribute>
{
	public final int diskPageNum;
	public final T[] pageObjects;
	public final IDNumber[] associatedIDs;
	public final SlottedPagePointer nextSlottedPagePointer;
	public final int numObjects;
	public final int maxNumObjects;
	
	public SlottedPage(int diskPageNum, T[] pageObjects, IDNumber[] associatedIDs, String tableName, String attributeName, int maxNumObjects, int hashValue, int nextSlottedPagePointer)
	{
		this.diskPageNum = diskPageNum;
		this.pageObjects = pageObjects;
		this.associatedIDs = associatedIDs;
		this.nextSlottedPagePointer = new SlottedPagePointer(tableName, attributeName, hashValue, nextSlottedPagePointer);
		this.numObjects = this.pageObjects.length;
		this.maxNumObjects = maxNumObjects;
	}
}
