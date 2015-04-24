package disk;

import java.util.ArrayList;
import java.util.HashMap;

import data.Attribute;
import data.ClientName;
import data.IDNumber;
import data.PhoneNumber;
import data.Record;

/**
 * this is the Disk object it represents the different attributes as column storage. three different attributes 
 * IDNumber, ClientName and PhoneNumber. each attribute is a file which is a set of slotted pages. Each slotted page
 * is of size 512. each slotted page in a given file has a pointer to the next slotted page in that file if it exists. 
 *
 */

public class Disk
{
	private final HashMap<String, Table> tables;
	private final ArrayList<String> insertionOrder;
	private final ArrayList<Integer> tablePageNumStart;
	private int newPageCounter;
	
	private static final int maxObjectID = 512/IDNumber.LENGTH - 1;
	private static final int maxObjectCN = 512/ClientName.LENGTH - 1;
	private static final int maxObjectPN = 512/PhoneNumber.LENGTH - 1;
	
	private static final int NUM_HASH_VALUES = 16;
	
	public Disk()
	{
		this.tables = new HashMap<String, Table>();
		this.insertionOrder = new ArrayList<String>();
		this.tablePageNumStart = new ArrayList<Integer>();
		this.newPageCounter = Integer.MAX_VALUE;
	}
	
	/**
	 * this method is used to insert a new record. if the table doesn't exist it creates a 
	 * new table to insert the new record. if the insertion is correct then it compute the hash based on the 
	 * record id number and will insert the record to the disk, or if not correct it will return false.  
	 * @param tableName it takes the name of the table and
	 * @param r the record that needed to be inserted.
	 * @return
	 */
	public boolean insertRecord(String tableName, Record r)
	{
		this.createNewTable(tableName);
		Table t = this.tables.get(tableName);
		int oldNumPages = t.getNumSlottedPages();
		boolean success = t.insertRecord(Disk.computeHash(r.id), r);
		if(!success)
		{
			return false;
		}
		if(oldNumPages < t.getNumSlottedPages())
		{
			int binSearchIndex = this.insertionOrder.indexOf(tableName);
			if(binSearchIndex < 0)
			{
				throw new IllegalStateException("weird error.");
			}
			for(int i = binSearchIndex; i < this.insertionOrder.size(); i++)
			{
				if(i == 0)
				{
					continue;
				}
				this.tablePageNumStart.set(i, this.tablePageNumStart.get(i-1) + 
						this.tables.get(this.insertionOrder.get(i-1)).getNumSlottedPages());
			}
		}
		return true;
	}
	
	/**
	 * this method is used to create a new table if the table doesn't already exist. it takes
	 * @param tableName the name of the table to be created. and prompt table is created or 
	 * @return it will return false if the fall in crating table.
	 */
	
	public boolean createNewTable(String tableName)
	{
		if(!this.tables.containsKey(tableName))
		{
			System.out.println("New table created");
			Table newTable = new Table(tableName, NUM_HASH_VALUES);
			int startOfNewPages = 0;
			for(Table t : this.tables.values())
			{
				startOfNewPages += t.getNumSlottedPages();
			}
			this.tables.put(tableName, newTable);
			this.insertionOrder.add(tableName);
			this.tablePageNumStart.add(startOfNewPages);
			return true;
		}
		return false;
	}
	
	/**
	 * this is used to delete a given record in a specific table. it takes the table name and if the table 
	 * name doesn't exist it will return false. if the table exist ill compute the hash for the id value for 
	 * that specific record to be deleted. 
	 * @param tableName the table name that has the record and
	 * @param r the record to be deleted 
	 * @return
	 */
	public boolean deleteRecord(String tableName, Record r)
	{
		if(!this.tables.containsKey(tableName))
		{
			System.out.println("Table does not exist");
			return false;
		}
		Table t = this.tables.get(tableName);
		int oldNumPages = t.getNumSlottedPages();
		boolean success = t.deleteRecord(Disk.computeHash(r.id), r);
		if(!success)
		{
			return false;
		}
		if(oldNumPages > t.getNumSlottedPages())
		{
			int binSearchIndex = this.insertionOrder.indexOf(tableName);
			if(binSearchIndex < 0)
			{
				throw new IllegalStateException("weird error.");
			}
			for(int i = binSearchIndex; i < this.insertionOrder.size(); i++)
			{
				if(i == 0)
				{
					continue;
				}
				this.tablePageNumStart.set(i, this.tablePageNumStart.get(i-1) + this.tables.get(this.insertionOrder.get(i-1)).getNumSlottedPages());
			}
		}
		return true;
	}
	
	/**
	 * this is used to delete an entire table. if the table doesn't, print table doesn't exist and return false.
	 * if table exist then get the table name and remove it.
	 * @param tableName the table name to be deleted 
	 * @return true if the table is deleted 
	 */
	public boolean deleteTable(String tableName)
	{
		if(!this.tables.containsKey(tableName))
		{
			System.out.println("Table does not exist");
			return false;
		}
		Table removed = this.tables.remove(tableName);
		if(removed == null)
		{
			return false;
		}
		int numPagesRemoved = removed.getNumSlottedPages();
//		int binSearchIndex = Collections.binarySearch(this.insertionOrder, tableName);
		int binSearchIndex = this.insertionOrder.indexOf(tableName);
		if(binSearchIndex < 0)
		{
			throw new IllegalStateException("weird error.");
		}
		for(int i = binSearchIndex+1; i < this.insertionOrder.size(); i++)
		{
			this.tablePageNumStart.set(i, this.tablePageNumStart.get(i) - numPagesRemoved);
		}
		this.insertionOrder.remove(binSearchIndex);
		this.tablePageNumStart.remove(binSearchIndex);
		return true;
	}
	
	/**
	 * this is to retrieve a slotted page. it takes 
	 * @param tableName the table name of that record and 
	 * @param id IDNumber of the client 
	 * @return table name 
	 */
	public SlottedPage<? extends Attribute>[] retrieve(String tableName, IDNumber id)
	{
		if(!this.tables.containsKey(tableName))
		{
			System.out.println("Table does not exist");
			return null;
		}
//		int binSearchIndex = Collections.binarySearch(this.insertionOrder, tableName);
//		int binSearchIndex = this.insertionOrder.indexOf(tableName);
//		if(binSearchIndex < 0)
//		{
//			throw new IllegalStateException("weird error.");
//		}
		return this.tables.get(tableName).getSlottedPages(this.tablePageNumStart.get(this.insertionOrder.indexOf(tableName)), 
				Disk.computeHash(id));
	}
	
	/**
	 * this method is used to retrieve the next slotted page. it takes  
	 * @param pointer the slotted page pinter and 
	 * @return pointer for the next slotted page 
	 */
	
	public SlottedPage<? extends Attribute> retrieveNext(SlottedPagePointer pointer)
	{
		if(!this.tables.containsKey(pointer.tableName))
		{
			System.out.println("Table does not exist");
			return null;
		}
		return this.tables.get(pointer.tableName).getNextSlottedPage(this.tablePageNumStart.get(this.insertionOrder.indexOf(pointer.tableName)), pointer);
	}
	
	public SlottedPage<? extends Attribute> makeNewEmptyPage(String tableName, IDNumber id, String attributeName)
	{
		if(!this.tables.containsKey(tableName))
		{
			this.createNewTable(tableName);
		}
		this.newPageCounter--;
		switch(attributeName)
		{
			case "data.IDNumber":
				return new SlottedPage<IDNumber>(this.newPageCounter, new IDNumber[0], new IDNumber[0], tableName, IDNumber.class.getName(), maxObjectID, Disk.computeHash(id), -1);
			case "data.ClientName":
				return new SlottedPage<ClientName>(this.newPageCounter, new ClientName[0], new IDNumber[0], tableName, ClientName.class.getName(), maxObjectCN, Disk.computeHash(id), -1);
			case "data.PhoneNumber":
				return new SlottedPage<PhoneNumber>(this.newPageCounter, new PhoneNumber[0], new IDNumber[0], tableName, PhoneNumber.class.getName(), maxObjectPN, Disk.computeHash(id), -1);
			default:
				throw new UnsupportedOperationException("Attribute not supported");
		}
	}
	
	private static int computeHash(IDNumber primaryKey)
	{
		return (int)(primaryKey.value % NUM_HASH_VALUES);
	}
}
