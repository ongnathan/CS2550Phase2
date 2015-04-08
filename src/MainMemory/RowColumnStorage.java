package MainMemory;

import java.util.ArrayList;
import java.util.Collections;

import data.AreaCode;
import data.Attribute;
import data.ClientName;
import data.IDNumber;
import data.PhoneNumber;
import data.Record;

public class RowColumnStorage
{
	//need to have multiple tables implemented
	private final ArrayList<String> tableNames;
	
	private final ArrayList<ArrayList<Record>> rowStorage;	//kept in sorted order for searching ease
	
	private final ArrayList<ArrayList<IDNumber>> columnIDNumbers;
	private final ArrayList<ArrayList<ClientName>> columnClientNames;
	private final ArrayList<ArrayList<PhoneNumber>> columnPhoneNumbers;
	
	private final ArrayList<ArrayList<Boolean>> inMemoryRow;
	
	private final ArrayList<ArrayList<Boolean>> inMemoryColumnIDN;
	private final ArrayList<ArrayList<Boolean>> inMemoryColumnCN;
	private final ArrayList<ArrayList<Boolean>> inMemoryColumnPN;
	
//	private final ArrayList<ArrayList<Integer>> inMemoryPageNumColumnIDN;
//	private final ArrayList<ArrayList<Integer>> inMemoryPageNumColumnCN;
//	private final ArrayList<ArrayList<Integer>> inMemoryPageNumColumnPN;
	
	public RowColumnStorage()
	{
		this.tableNames = new ArrayList<String>();
		
		this.rowStorage = new ArrayList<ArrayList<Record>>();
		this.columnIDNumbers = new ArrayList<ArrayList<IDNumber>>();
		this.columnClientNames = new ArrayList<ArrayList<ClientName>>();
		this.columnPhoneNumbers = new ArrayList<ArrayList<PhoneNumber>>();
		
		this.inMemoryRow = new ArrayList<ArrayList<Boolean>>();
		
		this.inMemoryColumnIDN = new ArrayList<ArrayList<Boolean>>();
		this.inMemoryColumnCN = new ArrayList<ArrayList<Boolean>>();
		this.inMemoryColumnPN = new ArrayList<ArrayList<Boolean>>();
	}
	
	public void populate(Record[] records, String[] tableNames)
	{
		if(records.length != tableNames.length)
		{
			throw new IllegalArgumentException("The number of records and their corresponding tables must be the same size");
		}
		for(int i = 0; i < records.length; i++)
		{
			this.insert(records[i], tableNames[i]);
		}
	}
	
	public boolean insert(Record r, String tableName)
	{
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			//make new table if necessary
			tableSearchIndex = -tableSearchIndex - 1;
			this.tableNames.add(tableSearchIndex, tableName);
			
			this.rowStorage.add(tableSearchIndex, new ArrayList<Record>());
			this.columnIDNumbers.add(tableSearchIndex, new ArrayList<IDNumber>());
			this.columnClientNames.add(tableSearchIndex, new ArrayList<ClientName>());
			this.columnPhoneNumbers.add(tableSearchIndex, new ArrayList<PhoneNumber>());
			
			this.inMemoryRow.add(tableSearchIndex, new ArrayList<Boolean>());
			this.inMemoryColumnIDN.add(tableSearchIndex, new ArrayList<Boolean>());
			this.inMemoryColumnCN.add(tableSearchIndex, new ArrayList<Boolean>());
			this.inMemoryColumnPN.add(tableSearchIndex, new ArrayList<Boolean>());
		}
		int binSearchIndex = -Collections.binarySearch(this.rowStorage.get(tableSearchIndex), r) - 1;
		if(binSearchIndex < 0)
		{
			//skip records with same ID
			return false;
		}
		this.rowStorage.get(tableSearchIndex).add(binSearchIndex, r);
		this.inMemoryRow.get(tableSearchIndex).add(binSearchIndex, new Boolean(false));
		
		this.columnIDNumbers.get(tableSearchIndex).add(binSearchIndex, r.id);
		this.inMemoryColumnIDN.get(tableSearchIndex).add(binSearchIndex, new Boolean(false));
		this.columnClientNames.get(tableSearchIndex).add(binSearchIndex, r.clientName);
		this.inMemoryColumnCN.get(tableSearchIndex).add(binSearchIndex, new Boolean(false));
		this.columnPhoneNumbers.get(tableSearchIndex).add(binSearchIndex, r.phoneNumber);
		this.inMemoryColumnPN.get(tableSearchIndex).add(binSearchIndex, new Boolean(false));
		return true;
	}
	
	public boolean delete(Record r, String tableName)
	{
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			return false;
		}
		int binSearchIndex = Collections.binarySearch(this.rowStorage.get(tableSearchIndex), r);
		if(binSearchIndex < 0)
		{
			return false;
		}
		this.rowStorage.get(tableSearchIndex).remove(binSearchIndex);
		this.columnIDNumbers.get(tableSearchIndex).remove(binSearchIndex);
		this.columnClientNames.get(tableSearchIndex).remove(binSearchIndex);
		this.columnPhoneNumbers.get(tableSearchIndex).remove(binSearchIndex);
		this.inMemoryRow.get(tableSearchIndex).remove(binSearchIndex);
		this.inMemoryColumnIDN.get(tableSearchIndex).remove(binSearchIndex);
		this.inMemoryColumnCN.get(tableSearchIndex).remove(binSearchIndex);
		this.inMemoryColumnPN.get(tableSearchIndex).remove(binSearchIndex);
		return true;
	}
	
	public boolean deleteTable(String tableName)
	{
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			return false;
		}
		this.rowStorage.remove(tableSearchIndex);
		this.columnIDNumbers.remove(tableSearchIndex);
		this.columnClientNames.remove(tableSearchIndex);
		this.columnPhoneNumbers.remove(tableSearchIndex);
		this.inMemoryRow.remove(tableSearchIndex);
		this.inMemoryColumnIDN.remove(tableSearchIndex);
		this.inMemoryColumnCN.remove(tableSearchIndex);
		this.inMemoryColumnPN.remove(tableSearchIndex);
		this.tableNames.remove(tableSearchIndex);
		return true;
	}
	
	public Record retrieve(IDNumber id, String tableName)
	{
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			return null;
		}
		int binSearchIndex = Collections.binarySearch(this.columnIDNumbers.get(tableSearchIndex), id);
		if(binSearchIndex < 0)
		{
			return null;
		}
		return this.rowStorage.get(tableSearchIndex).get(binSearchIndex);
	}
	
	public boolean checkInStorage(Record r, String tableName)
	{
//		return this.checkInStorage(r.id, tableName) && this.checkInStorage(r.clientName, tableName) && this.checkInStorage(r.phoneNumber, tableName);
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			return false;
		}
		int binSearchIndex = Collections.binarySearch(this.rowStorage.get(tableSearchIndex), r);
		if(binSearchIndex < 0)
		{
			return false;
		}
		return this.inMemoryRow.get(tableSearchIndex).get(binSearchIndex);
	}
	
	public boolean checkInStorage(Attribute value, IDNumber recordID, String tableName)
	{
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			return false;
		}
		int binSearchIndex = Collections.binarySearch(this.columnIDNumbers.get(tableSearchIndex), recordID);
//		switch(value.getClass().getName())
//		{
//			case "data.IDNumber":
//				binSearchIndex = Collections.binarySearch(this.columnIDNumbers.get(tableSearchIndex), (IDNumber)value); 
//				break;
//			case "data.ClientName":
//				binSearchIndex = Collections.binarySearch(this.columnClientNames.get(tableSearchIndex), (ClientName)value); 
//				break;
//			case "data.PhoneNumber":
//				binSearchIndex = Collections.binarySearch(this.columnPhoneNumbers.get(tableSearchIndex), (PhoneNumber)value); 
//				break;
//			default:
//				throw new UnsupportedOperationException("The attribute type is not supported.");
//		}
		if(binSearchIndex < 0)
		{
			return false;
		}
		switch(value.getClass().getName())
		{
			case "data.IDNumber":
				return this.inMemoryColumnIDN.get(tableSearchIndex).get(binSearchIndex);
			case "data.ClientName":
				return this.inMemoryColumnCN.get(tableSearchIndex).get(binSearchIndex);
			case "data.PhoneNumber":
				return this.inMemoryColumnPN.get(tableSearchIndex).get(binSearchIndex);
			default:
				throw new UnsupportedOperationException("The attribute type is not supported.");
		}
	}
	
	private boolean alterCacheStatusRecordOnly(Record r, String tableName, boolean status)
	{
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			return false;
		}
		int binSearchIndex = Collections.binarySearch(this.rowStorage.get(tableSearchIndex), r);
		if(binSearchIndex < 0)
		{
			return false;
		}
		this.inMemoryRow.get(tableSearchIndex).set(binSearchIndex, status);
		return true;
	}
	
	//Returns the index of the location of the value in the array (-1 means not found)
	private int alterCacheStatusAttributeOnly(Attribute value, IDNumber id, String tableName, boolean status)
	{
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			return -1;
		}
		int binSearchIndex = Collections.binarySearch(this.columnIDNumbers.get(tableSearchIndex), id);
//		switch(value.getClass().getName())
//		{
//			case "data.IDNumber":
//				binSearchIndex = Collections.binarySearch(this.columnIDNumbers.get(tableSearchIndex), (IDNumber)value); 
//				break;
//			case "data.ClientName":
//				binSearchIndex = Collections.binarySearch(this.columnClientNames.get(tableSearchIndex), (ClientName)value); 
//				break;
//			case "data.PhoneNumber":
//				binSearchIndex = Collections.binarySearch(this.columnPhoneNumbers.get(tableSearchIndex), (PhoneNumber)value); 
//				break;
//			default:
//				throw new UnsupportedOperationException("The attribute type is not supported.");
//		}
		if(binSearchIndex < 0)
		{
			return -1;
		}
		switch(value.getClass().getName())
		{
			case "data.IDNumber":
				this.inMemoryColumnIDN.get(tableSearchIndex).set(binSearchIndex, status);
				break;
			case "data.ClientName":
				this.inMemoryColumnCN.get(tableSearchIndex).set(binSearchIndex, status);
				break;
			case "data.PhoneNumber":
				this.inMemoryColumnPN.get(tableSearchIndex).set(binSearchIndex, status);
				break;
			default:
				throw new UnsupportedOperationException("The attribute type is not supported.");
		}
		return binSearchIndex;
	}
	
	public boolean alterCacheStatus(Record r, String tableName, boolean status)
	{
		return this.alterCacheStatusRecordOnly(r, tableName, status) && this.alterCacheStatusAttributeOnly(r.id, r.id, tableName, status) >= 0 && this.alterCacheStatusAttributeOnly(r.clientName, r.id, tableName, status) >= 0 && this.alterCacheStatusAttributeOnly(r.phoneNumber, r.id, tableName, status) >= 0;
	}
	
	public boolean alterCacheStatus(Attribute value, IDNumber id, String tableName, boolean status)
	{
		int binSearchIndex = this.alterCacheStatusAttributeOnly(value, id, tableName, status);
		if(binSearchIndex < 0)
		{
			return false;
		}
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			throw new IllegalStateException("Something weird happened...");
		}
		this.alterCacheStatusRecordOnly(new Record(this.columnIDNumbers.get(tableSearchIndex).get(binSearchIndex), this.columnClientNames.get(tableSearchIndex).get(binSearchIndex), this.columnPhoneNumbers.get(tableSearchIndex).get(binSearchIndex)), tableName, this.inMemoryColumnIDN.get(tableSearchIndex).get(binSearchIndex) && this.inMemoryColumnCN.get(tableSearchIndex).get(binSearchIndex) && this.inMemoryColumnPN.get(tableSearchIndex).get(binSearchIndex));
		return true;
	}
	
	public Record[] getRange(AreaCode areaCode, String tableName)
	{
		int tableSearchIndex = Collections.binarySearch(this.tableNames, tableName);
		if(tableSearchIndex < 0)
		{
			return null;
		}
		ArrayList<Integer> locations = new ArrayList<Integer>();
		ArrayList<PhoneNumber> phoneNumbers = this.columnPhoneNumbers.get(tableSearchIndex);
		for(int i = 0; i < phoneNumbers.size(); i++)
		{
			if(phoneNumbers.get(i).equals(areaCode))
			{
				locations.add(i);
			}
		}
		
		Record[] areaCodeRecords = new Record[locations.size()];
		ArrayList<Record> records = this.rowStorage.get(tableSearchIndex);
		int i = 0;
		for(Integer index : locations)
		{
			areaCodeRecords[i] = records.get(index.intValue());
			i++;
		}
		return areaCodeRecords;
	}
	
//	public int getRowStorageSize()
//	{
//		return this.rowStorage.size();
//	}
//	
//	public int getColumnIDNumbersSize()
//	{
//		return this.columnIDNumbers.size();
//	}
//	
//	public int getColumnPhoneNumbersSize()
//	{
//		return this.columnPhoneNumbers.size();
//	}
//	
//	public int getColumnClientNamesSize()
//	{
//		return this.columnClientNames.size();
//	}
	
	public Record[][] getPreImage()
	{
		Record[][] wholeDisk = new Record[this.tableNames.size()][];
		for(int i = 0; i < wholeDisk.length; i++)
		{
			wholeDisk[i] = columnIDNumbers.get(i).toArray(new Record[0]);
		}
		return wholeDisk;
	}
}
