package MemoryManager;


import java.util.ArrayList;

import parser.Command;
import disk.*;
import data.*;
import MainMemory.*;
import Transaction.TransactionManager.transaction;
import disk.DiskOperation;

public class MemoryManager {

	private int maxNumPages;		// The  buffer size in maximum 
	MemoryArray memory_array;

	int SIZE_ID_NUMBER = 4;
	int SIZE_CLIENT_NAME = 16;
	int SIZE_PHONE_NUMBER = 12;
	int SIZE_RECORD = SIZE_ID_NUMBER + SIZE_CLIENT_NAME + SIZE_PHONE_NUMBER;
	int SIZE_OF_PAGE = 512;
	
	String lookup_type;
	Disk disk = new Disk();
	RowColumnStorage memory = new RowColumnStorage();
	
	LRUTable lru;
	
	private StringBuilder lastActionString;
	
	public MemoryManager (int _buffer_size, Disk _disk, RowColumnStorage _memory)
	{
		maxNumPages = _buffer_size/1024;
		this.disk = _disk;
		this.memory = _memory;
		memory_array = new MemoryArray(maxNumPages);
		lru = new LRUTable(maxNumPages);
		this.lastActionString = new StringBuilder();
		System.out.println("maxNumPages:"+maxNumPages);
	}
	
	public String getLog()
	{
		String logString = this.lastActionString.toString();
		this.lastActionString = new StringBuilder();
		return logString;
	}


	//Insert new record to memory array(named 'memory_array') and RCSObject(named 'memory')
	public boolean insertToMemory(String table_name, Record record, boolean transactionType)
	{
		if(memory.retrieve(record.id, table_name) != null)
		{
			if(memory.checkInStorage(record,table_name))
			{
				//if the record is in RCSObject, there is no need to insert. Abort it.
				System.out.println("The record has duplicate ID that already exists in the database: " + record);
				return false;
			}
			//else find ID
			this.bringInAttributePage(IDNumber.class.getName(), record, table_name, transactionType);
		}
		else
		{
			/**	if the record is NOT in RCSObject, then the goal is like following;
				Goal: have each of three attributes has a page in memory array (MA) that is of Last page & have available Space & LRU allows it
			 	and write the new record to the corresponding pages and let LRU knows that
			*/
			String[] attributes = {IDNumber.class.getName(),ClientName.class.getName(),PhoneNumber.class.getName()};
			int pageForIDN = -1;
			int pageForCN = -1;
			int pageForPN = -1;
			
			//for each attribute
			for (String attribute:attributes)
			{
				int page_id = 0;
				//Get the latest page of the attribute from memory array 
				SlottedPage<? extends Attribute> page = memory_array.getLastestAttributePage(table_name,  record.id, attribute);
				if(page == null)
				{
					SlottedPage<? extends Attribute>[] firstPages = disk.retrieve(table_name, record.id);
					if(firstPages != null)
					{
						//get first page, bring to memory
						switch(attribute)
						{
							case "data.IDNumber":
								page = firstPages[0];
								break;
							case "data.ClientName":
								page = firstPages[1];
								break;
							case "data.PhoneNumber":
								page = firstPages[2];
								break;
							default:
								throw new UnsupportedOperationException("Attribute " + attribute + " not yet supported");
						}
						//add page to mem
						if(!memory_array.availableSpace())
						{
							page_id = evictPage(transactionType);
							memory_array.replace(page_id, page);//replace new file to memory_array
						}
						else
						{
							//If there's empty space in the page, just put it there.
							page_id = memory_array.insert(page);
						}
						lru.insertNewPageToLRU(page_id, page.diskPageNum);//Tell LRU the new page_id
						lastActionString.append("SWAP IN T-" + table_name + " P-" + page_id + " B-" + page.nextSlottedPagePointer.hashValue + "\n");
					}
				}
				else
				{
					lru.touchPage(memory_array.getIndexOfPage(page));
				}
				
				//check the page is last page, otherwise, it will bring the last page from disk of the attribute
				while(page != null && !MemoryArray.isLastAttributePage(page)){
					//if NOT the last page: keep bring it from disk until it reaches last page
						if(!memory_array.availableSpace())
						{
							page_id = evictPage(transactionType);
							memory_array.replace(page_id, page);
							lru.insertNewPageToLRU(page_id, page.diskPageNum);
							lastActionString.append("SWAP IN T-" + table_name + " P-" + page_id + " B-" + page.nextSlottedPagePointer.hashValue + "\n");
						}
						page = (SlottedPage<? extends Attribute>) disk.retrieveNext(page.nextSlottedPagePointer);
				}
				
				//check the page has empty space
				if (page == null || page.maxNumObjects < page.numObjects)
				{
					// if there is no space, we will create new EMPTY page 
					page = (SlottedPage<? extends Attribute>)  disk.makeNewEmptyPage(table_name, record.id, attribute);
					lastActionString.append("CREATE T-" + table_name + " P-" + page_id + " B-" + page.nextSlottedPagePointer.hashValue + "\n");
					if(!memory_array.availableSpace())
					{
						page_id = evictPage(transactionType);
						memory_array.replace(page_id, page);//replace new file to memory_array
					}
					else
					{
						//If there's empty space in the page, just put it there.
						page_id = memory_array.insert(page);
					}
					lru.insertNewPageToLRU(page_id, page.diskPageNum);//Tell LRU the new page_id
					lastActionString.append("SWAP IN T-" + table_name + " P-" + page_id + " B-" + page.nextSlottedPagePointer.hashValue + "\n");
				}
				
				//check if LRU says it is okay (if not, we need to get new page)
				
				//Save page before going to next loop
				switch(attribute)
				{
					case "data.IDNumber":
						pageForIDN = page_id;
						break;
					case "data.ClientName":
						pageForCN = page_id;
						break;
					case "data.PhoneNumber":
						pageForPN = page_id;
						break;
					default:
						throw new UnsupportedOperationException("Attribute " + attribute + " not yet supported");
				}
				
			}
			
			//At this point, there exist available pages in memory_array to put new record's each attributes
			memory.insert(record, table_name);
			memory.alterCacheStatus(record, table_name, true); // change status on RCSObject

			//update LRU about putting new record into new page 
			lru.addDiskOperation(pageForIDN, new DiskOperation(Command.INSERT, record, table_name));
			lru.addDiskOperation(pageForCN, new DiskOperation(Command.INSERT, record, table_name));
			lru.addDiskOperation(pageForPN, new DiskOperation(Command.INSERT, record, table_name));
			
		}
		return true;
	}
	
	//This will bring up the attribute's page until it reaches last page of the disk.
	private void bringInAttributePage(String attribute, Record record, String tableName, boolean transactionType)
	{
		Attribute value = null;
		switch(attribute)
		{
			case "data.IDNumber":
				value = record.id;
				break;
			case "data.ClientName":
				value = record.clientName;
				break;
			case "data.PhoneNumber":
				value = record.phoneNumber;
				break;
			default:
				throw new UnsupportedOperationException("The attribute type is not supported.");
		}
		if(memory.checkInStorage(value, record.id, tableName))
		{
			return;
		}
		//for each attributes 
		int page_id;
		int type_of_attribute=0;
		switch(attribute)
		{
			case "data.IDNumber":
				type_of_attribute = 0;
				break;
			case "data.ClientName":
				type_of_attribute = 1;
				break;
			case "data.PhoneNumber":
				type_of_attribute = 2;
				break;
			default:
				throw new UnsupportedOperationException("The attribute type is not supported.");
		}
					
		SlottedPage<? extends Attribute>[] pages_disk = (SlottedPage<? extends Attribute>[]) disk.retrieve(tableName, record.id);
		
		SlottedPage<? extends Attribute> page_disk = pages_disk[type_of_attribute];
		
		SlottedPage<? extends Attribute>[] pages_memory = memory_array.getAllAttributePages(tableName, record.id, attribute);
		boolean isFound=false;
		//check ID Number is in the page
		out:
		while(!isFound)
		{
			//if NOT in the page: keep bring it from disk until it reaches last page
			for (SlottedPage<? extends Attribute> page_memory : pages_memory)
			{
				if(page_memory.equals(page_disk))
				{
					lru.touchPage(memory_array.getIndexOfPage(page_memory));
					continue out;
				}
			}
			
			if(!memory_array.availableSpace())
			{
				page_id = evictPage(transactionType);
				memory_array.replace(page_id, page_disk);
			}
			else
			{
				page_id = memory_array.insert(page_disk);
			}
			
			lru.insertNewPageToLRU(page_id, page_disk.diskPageNum);
			lastActionString.append("SWAP IN T-" + tableName + " P-" + page_id + " B-" + page_disk.nextSlottedPagePointer.hashValue + "\n");
			
			if(checkAttributeInPage(page_disk,tableName,record,attribute))
			{
				isFound = true;
			}
			page_disk = (SlottedPage<? extends Attribute>) disk.retrieveNext(page_disk.nextSlottedPagePointer);
		}
	}
	
	//For given page (slotted page), this will check whether the page has the specific attribute of the record.
	private boolean checkAttributeInPage(SlottedPage<? extends Attribute> sp, String table_name, Record record, String attribute)
	{
		boolean return_val = false;
		int num_iteration = sp.pageObjects.length;
		
		for(int i=0; i<num_iteration;i++)
		{
			switch(attribute)
			{
				case "data.IDNumber":
					IDNumber idnum = (IDNumber) sp.pageObjects[i];
					if (record.id.equals(idnum)) 
						return_val = true;
					break;
				case "data.ClientName":
					ClientName clientname = (ClientName) sp.pageObjects[i];
					if (record.clientName.equals(clientname)) 
						return_val = true;
					break;
				case "data.PhoneNumber":
					PhoneNumber phoennum = (PhoneNumber) sp.pageObjects[i];
					if (record.phoneNumber.equals(phoennum)) 
						return_val = true;
					break;
				default:
					throw new UnsupportedOperationException("The attribute type is not supported.");
			}
		}
		return return_val;
	}
	
	//Retrieve record from memory given id of the record
	public Record readRecord(String table_name, IDNumber record_id, boolean transactionType)
	{
		Record record = memory.retrieve(record_id, table_name); //If RCSObject has the Record ID, then it will return record object of the ID.
		
		//first check RCSOBject that any of record has such table_name AND record_id EXIST IN OUR DATABASE
		if (record == null)
		{
			this.insertAllIDPages(table_name, record_id, transactionType);
			System.out.println("The record not exist in the our Database. Abort for ID: " + record_id);
			return null;
		}
		
		// check RCSOBject that any of record has such table_name AND record_id EXIST IN MAIN MEMORY
		if (memory.checkInStorage(record,table_name)){
			return record;
		}
		
		//if NOT EXISTS, then it's time to look through all disks
		String[] attributes = {IDNumber.class.getName(),ClientName.class.getName(),PhoneNumber.class.getName()};
		
		for (String attribute : attributes)
		{
			this.bringInAttributePage(attribute, record, table_name, transactionType);
		}
		memory.alterCacheStatus(record, table_name, true);
		//At this point, all three pages for all three attributes are in Main memory (memory_array)		
		return record;
	}
	
	//Using readRecord method, ReadAreaCode can easily retrieve record of that area code.
	public Record[] readAreaCode(PhoneNumber phone_number, String table_name, boolean transactionType)
	{
		AreaCode area_code = new AreaCode(phone_number.areaCode);
		//first check RCSOBject that any of record has such table_name AND record_id EXIST IN OUR DATABASE
		if (null == memory.getRange(area_code,table_name)){
			System.out.println("The area code does not exist in the our Database. Abort.");
			return new Record[0];
		}
		
		Record[] records = memory.getRange(area_code,table_name); // this is complete list of records that has the area_code in our database
		for(Record record:records)
		{
			readRecord(table_name,record.id, transactionType);
		}
		
		return records;
	}


	//This will bring up the number of records by area code
	public int countAreaCode(PhoneNumber phone_number, String table_name, boolean transactionType)
	{
		return readAreaCode(phone_number,table_name, transactionType).length;
	}
	
	//This deletes a table
	public boolean deleteTable(String tableName, boolean transactionType)
	{
		if(!memory.deleteTable(tableName))
		{
			return false;
		}
		Integer[] indexes = memory_array.getAllIndexesOfTablePages(tableName);

		//To delete table, we need to 1)evict page from LRU, 2) delete table from disk.
		lru.forceEvict(indexes);
		if(transactionType){
			//if it is a transaction delete table will be store in the buffer.
		} else {
			disk.deleteTable(tableName);
		}
		//delete from memory_array
		memory_array.forceEvict(indexes);
		lastActionString.append("Deleted: " + tableName + "\n");
		return true;
	}

	//insertToDisk triggered when the record in memory is swapped out
	public boolean insertToDisk(String table_name, Record record) 
	{
		// insert to disk
		if(!disk.insertRecord(table_name, record)) 
		{
			System.out.println("Failed: "+"Insert into Disk " + table_name + " " + record);
			return false;
		}
		System.out.println("Insert into Disk " + table_name + " "+ record);
		this.memory.insert(record, table_name);
		return true;
	}
	
    //evictPage is triggered when there is no available space is in memory array
	//returns the index of the evicted page
	private int evictPage(boolean transactionType)
	{
		//If there's no space in memory then,
		int page_ids[] = lru.pageToEvict();
		SlottedPage<? extends Attribute> evicted = memory_array.getPage(page_ids[0]);
		lastActionString.append("SWAP OUT T-" + evicted.nextSlottedPagePointer.tableName + " P-" + page_ids[0] + " B-" + evicted.nextSlottedPagePointer.hashValue + "\n");
		ArrayList<DiskOperation> ops = lru.evict(page_ids[0]);

		//reflect changes of record in the page evicted into the disk 
		if(transactionType){
			//if it is transaction  then store the command into buffer
		} else {
			
			for(DiskOperation op : ops)
			{
				switch(op.command)
				{
					case INSERT:
						disk.insertRecord(op.tableName, op.getRecord());
						break;
					default:
						throw new UnsupportedOperationException("Command not supported.");
				}
			}
		}
		SlottedPage<? extends Attribute> pageToEvict = this.memory_array.getPage(page_ids[0]);
		for(int i = 0; i < pageToEvict.pageObjects.length; i++)
		{
			this.memory.alterCacheStatus(pageToEvict.pageObjects[i], pageToEvict.associatedIDs[i], pageToEvict.nextSlottedPagePointer.tableName, false);
		}
		return page_ids[0];
	}
	
	//meant to be done when a failure is determined
	@SuppressWarnings("unchecked")
	private void insertAllIDPages(String tableName, IDNumber id, boolean transactionType)
	{
		SlottedPage <?extends Attribute>[] temp = disk.retrieve(tableName, id);
		if(temp == null)
			return;
		SlottedPage<IDNumber> idNumPage = (SlottedPage<IDNumber>)temp[0];
		while(idNumPage != null)
		{
			if(memory_array.getIndexOfPage(idNumPage) == -1)
			{
				int pageID = -1;
				if(!memory_array.availableSpace())
				{
					pageID = evictPage(transactionType);
					memory_array.replace(pageID, idNumPage);//replace new file to memory_array
				}
				else
				{
					//There's space. Just put it there.
					pageID = memory_array.insert(idNumPage);
				}
				lru.insertNewPageToLRU(pageID, idNumPage.diskPageNum);//Tell LRU the new page_id
				lastActionString.append("SWAP IN T-" + tableName + " P-" + pageID + " B-" + idNumPage.nextSlottedPagePointer.hashValue + "\n");
			}
			SlottedPage <? extends Attribute>temp2 = disk.retrieveNext(idNumPage.nextSlottedPagePointer);
			if (temp2 == null)
				break;
			idNumPage = (SlottedPage<IDNumber>)temp2;
		}
	}
	
	public Record[][] getCurrentPreImage()
	{
		return this.memory.getPreImage();
	}

	public boolean commitToTransaction(ArrayList<transaction> opBuffer) {
		// TODO Auto-generated method stub
		return false;
	}
}