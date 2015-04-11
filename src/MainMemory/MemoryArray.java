package MainMemory;

import java.util.ArrayList;
import java.util.LinkedList;

import data.Attribute;
import data.IDNumber;
import disk.SlottedPage;

public class MemoryArray
{
	private final SlottedPage<? extends Attribute>[] memoryArray;
	private final LinkedList<Integer> openSpace;
//	private int indexCounter;
	
	@SuppressWarnings("unchecked")
	public MemoryArray(int numPages)
	{
		this.memoryArray = (SlottedPage<? extends Attribute>[])new SlottedPage[numPages];
//		this.indexCounter = 0;
		this.openSpace = new LinkedList<Integer>();
		for(int i = 0; i < numPages; i++)
		{
			this.openSpace.add(i);
		}
	}
	
	public boolean availableSpace()
	{
//		if(this.indexCounter >= this.memoryArray.length)
//		{
//			return false;
//		}
//		return true;
		return !this.openSpace.isEmpty();
	}
	
	//inserts a slotted page into an open location.  Returns -1 if there is no open location.
	public int insert(SlottedPage<? extends Attribute> page)
	{
		if(!this.availableSpace())
		{
			return -1;
		}
		int openLoc = this.openSpace.poll();
		this.memoryArray[openLoc] = page;
//		this.indexCounter++;
//		return this.indexCounter-1;
		return openLoc;
	}
	
	public SlottedPage<? extends Attribute>[] forceEvict(Integer[] indexes)
	{
		SlottedPage<? extends Attribute>[] pagesToReturn = (SlottedPage<? extends Attribute>[])new SlottedPage[indexes.length];
		for(int i = 0; i < indexes.length; i++)
		{
			pagesToReturn[i] = this.memoryArray[indexes[i]];
			this.memoryArray[indexes[i]] = null;
			this.openSpace.add(indexes[i]);
		}
		return pagesToReturn;
	}
	
	public SlottedPage<? extends Attribute> replace(int index, SlottedPage<? extends Attribute> page)
	{
		if(index < 0 || index >= this.memoryArray.length)
		{
			throw new ArrayIndexOutOfBoundsException("Index paramter out of bounds");
		}
		SlottedPage<? extends Attribute> pageToReturn = this.memoryArray[index];
		this.memoryArray[index] = page;
		return pageToReturn;
	}
	
	//attribute name should be referenced by Class.class.getName()
	public SlottedPage<? extends Attribute> getLastestAttributePage(String tableName, IDNumber id, String attributeName)
	{
		SlottedPage<? extends Attribute> latestPage = null;
		for(int i = 0; i < this.memoryArray.length; i++)
		{
			if(this.memoryArray[i] == null)
			{
				continue;
			}
			if(this.memoryArray[i].nextSlottedPagePointer.tableName.equals(tableName) && this.memoryArray[i].nextSlottedPagePointer.attributeName.equals(attributeName) && this.memoryArray[i].nextSlottedPagePointer.hashValue == MemoryArray.computeHash(id) && (latestPage == null  || latestPage.nextSlottedPagePointer.pagePointer < this.memoryArray[i].nextSlottedPagePointer.pagePointer || this.memoryArray[i].nextSlottedPagePointer.pagePointer == -1))
			{
				if(this.memoryArray[i].nextSlottedPagePointer.pagePointer == -1)
				{
					return this.memoryArray[i];
				}
				latestPage = this.memoryArray[i];
			}
		}
		return latestPage;
	}
	
	public static boolean isLastAttributePage(SlottedPage<? extends Attribute> sp)
	{
		return sp.nextSlottedPagePointer.pagePointer == -1;
	}
	
	public SlottedPage<? extends Attribute>[] getAllAttributePages(String tableName, IDNumber id, String attributeName)
	{
		ArrayList<SlottedPage<? extends Attribute>> listOfPages = new ArrayList<SlottedPage<? extends Attribute>>();
		for(int i = 0; i < this.memoryArray.length; i++)
		{
			if(this.memoryArray[i] == null)
			{
				continue;
			}
			if(this.memoryArray[i].nextSlottedPagePointer.tableName.equals(tableName) && this.memoryArray[i].nextSlottedPagePointer.attributeName.equals(attributeName) && this.memoryArray[i].nextSlottedPagePointer.hashValue == MemoryArray.computeHash(id))
			{
				listOfPages.add(this.memoryArray[i]);
			}
		}
		@SuppressWarnings("unchecked")
		SlottedPage<? extends Attribute>[] placeHolder = (SlottedPage<? extends Attribute>[])new SlottedPage[0];
		return listOfPages.toArray(placeHolder);
	}
	
	public Integer[] getAllIndexesOfTablePages(String tableName)
	{
		ArrayList<Integer> indexList = new ArrayList<Integer>();
		for (int i = 0; i < this.memoryArray.length; i++)
		{
			if (this.memoryArray[i] == null)
			{
				continue;
			}
			if (this.memoryArray[i].nextSlottedPagePointer.tableName.equals(tableName))
			{
				indexList.add(i);
			}
		}
		return indexList.toArray(new Integer[0]);
	}
	
	public int getIndexOfPage(SlottedPage<? extends Attribute> page)
	{
		for(int i = 0; i < this.memoryArray.length; i++)
		{
			if(this.memoryArray[i] == null)
			{
				continue;
			}
			if(this.memoryArray[i].equals(page))
			{
				return i;
			}
		}
		return -1;
	}
	
	public SlottedPage<? extends Attribute> getPage(int index)
	{
		return this.memoryArray[index];
	}
	
	//TODO FIXME
	private static int computeHash(IDNumber primaryKey)
	{
		return (int)(primaryKey.value % 16);
	}
}
