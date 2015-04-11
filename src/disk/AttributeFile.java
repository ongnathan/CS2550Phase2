package disk;

import java.util.ArrayList;
import java.util.Collections;

import data.Attribute;
import data.IDNumber;

/**
 * this is the attribute file which is represented in the disk for the three different attributes 
 * it extends the ByteLengthClass to specify the length, the minimum length of the attribute 
 * (minLength), and the maximum length (maxLength)
 * @param <T> the attribute object
 */

public class AttributeFile<T extends Attribute>
{
	private final ArrayList<T>[] hashedObjects;
	private final ArrayList<IDNumber>[] correspondingIDs;
	private final int[] numPagesPerHash;
	
	private final String parentTableName;
	private final String attributeName;
//	private final int objectLength;
//	private final int slottedPageSize;
	private final int numHashValues;
	private final int numObjsInOneSlottedPage;
	
	private int numSlottedPages;
	
	private static final int SLOTTED_PAGE_SIZE = 512;		// the size of the slotted page int the disk
	
	@SuppressWarnings("unchecked")
//	public AttributeFile(int numHashValues, int objectLength, int slottedPageSize)
	
	/**
	 * The constructor
	 * it takes the name of the table that the attribute belongs to, the name of the attribute 
	 * the number of the hash value of the given attribute and the length of the attribute. it then 
	 * hashes the different IDNumber into the corresponding hash value   
	 * @param parentTableName the table name 
	 * @param attributeName the attribute name 
	 * @param numHashValues the hash value 
	 * @param objectLength the length of the attribute 
	 */
	
	public AttributeFile(String parentTableName, String attributeName, int numHashValues, int objectLength)
	{
		this.parentTableName = parentTableName;
		this.attributeName = attributeName;
		this.numHashValues = numHashValues;
		this.hashedObjects = (ArrayList<T>[]) new ArrayList[this.numHashValues];
		this.correspondingIDs = (ArrayList<IDNumber>[]) new ArrayList[this.numHashValues];
		for(int i = 0; i < this.hashedObjects.length; i++)
		{
			this.hashedObjects[i] = new ArrayList<T>();
			if(attributeName.equals(IDNumber.class.getName()))
			{
				this.correspondingIDs[i] = (ArrayList<IDNumber>)this.hashedObjects[i];
			}
			else
			{
				this.correspondingIDs[i] = new ArrayList<IDNumber>();
			}
		}
//		this.objectLength = objectLength;
//		this.slottedPageSize = slottedPageSize;
		this.numObjsInOneSlottedPage = (SLOTTED_PAGE_SIZE / objectLength) - 1; //we leave one additional space for the linked list pointer to the next slotted page
		this.numSlottedPages = numHashValues;
		this.numPagesPerHash = new int[this.numHashValues];
		for(int i = 0; i < this.numPagesPerHash.length; i++)
		{
			this.numPagesPerHash[i] = 1;
		}
	}
	
	/**
	 * it takes the value of the hash and will return false if the hash value is less than 0
	 * or equal or more than the numHashValues otherwise it returns true as a valid hash value
	 * @param hashValue the value of the hash
	 * @return true or false 
	 */
	
	private final boolean isValidHashValue(int hashValue)
	{
		if(hashValue < 0 || hashValue >= this.numHashValues)
		{
			return false;
		}
		return true;
	}
	
	/**
	 * this method is to  insert a hashed object it takes the hash value of the attribute and if the hashValue is not equal 
	 * !isValidHashValue(hashValue) it will return false invalid hash. if the hashedObjects Modulus 
	 * numObjsInOneSlottedPage = numObjsInOneSlottedPage - 1 then add new slotted page to hash the value in.
	 * @param hashValue the value of the hash
	 * @param attributeValue the attribute value 
	 * @param id the IDnumber of the client 
	 * @return returns the corresponding hashed value for a given id
	 */
	
	public boolean insert(int hashValue, T attributeValue, IDNumber id)
	{
		if(!isValidHashValue(hashValue))
		{
			System.err.println("Invalid hash value when inserting.");
			return false;
		}
		if(this.hashedObjects[hashValue].size() % this.numObjsInOneSlottedPage == this.numObjsInOneSlottedPage - 1)
		{
			this.numSlottedPages++;
			this.numPagesPerHash[hashValue]++;
		}
		return this.correspondingIDs[hashValue].add(id) && this.hashedObjects[hashValue].add(attributeValue);
	}
	
	/**
	 * this is to delete a hash value it takes the hash value and if the value isn't valid it
	 *  will return false.... 
	 * @param hashValue the value of the hash 
	 * @param attributeValue 
	 * @param id the IDNumber number of the client 
	 * @return
	 */
	
	public boolean delete(int hashValue, T attributeValue, IDNumber id)
	{
		if(!isValidHashValue(hashValue))
		{
			System.err.println("Invalid hash value when searching.");
			return false;
		}
		if(this.hashedObjects[hashValue].size() % this.numHashValues == 0 && this.correspondingIDs[hashValue].contains(id) 
					&& this.hashedObjects[hashValue].contains(attributeValue))
		{
			this.numSlottedPages--;
			this.numPagesPerHash[hashValue]--;
		}
		int binSearchIndex = Collections.binarySearch(this.correspondingIDs[hashValue], id);
		if(binSearchIndex < 0)
		{
			System.err.println("Invalid hash value when searching.");
			return false;
		}
		return this.correspondingIDs[hashValue].remove(binSearchIndex) != null && this.hashedObjects[hashValue].remove(binSearchIndex) != null;
	}
	
	/**
	 * this method is to get a slotted page. it takes the hash value and it starts of the file page number 
	 * it will return the next slotted page in the file.
	 * @param startOfFilePageNum the start of the page numbers 
	 * @param hashValue and the hash value 
	 * @return the slotted page 
	 */
	
	public SlottedPage<T> getSlottedPage(int startOfFilePageNum, int hashValue)
	{
		return this.getNextSlottedPage(startOfFilePageNum, new SlottedPagePointer(null,null,hashValue,0));
	}
	
	/**
	 * this method is to get the next slotted page in a given file. if the pointer of the slotted page is not valid
	 * then return error invalid hash value.... 
	 * @param startOfFilePageNum
	 * @param pointer
	 * @return
	 */
	
	@SuppressWarnings("unchecked")
	public SlottedPage<T> getNextSlottedPage(int startOfFilePageNum, SlottedPagePointer pointer)
	{
		if(!isValidHashValue(pointer.hashValue))
		{
			System.err.println("Invalid hash value when retrieving slotted page.");
			return null;
		}
		else if(pointer.pagePointer == -1)
		{
//			System.out.println("No more values");
			return null;
		}
		
		int endOfList = pointer.pagePointer + this.numObjsInOneSlottedPage;
		int nextSlottedPagePointer = endOfList;
		if(endOfList >= this.hashedObjects[pointer.hashValue].size())
		{
			endOfList = this.hashedObjects[pointer.hashValue].size();
			nextSlottedPagePointer = -1;
		}
		
		int pageNumAdd = 0;
		for(int i = 0; i <= pointer.hashValue; i++)
		{
			pageNumAdd += this.numPagesPerHash[i];
		}
		
		T[] placeHolder = (T[]) new Attribute[0];
		return new SlottedPage<T>(startOfFilePageNum + pageNumAdd, this.hashedObjects[pointer.hashValue].subList(pointer.pagePointer, endOfList).toArray(placeHolder), this.correspondingIDs[pointer.hashValue].subList(pointer.pagePointer, endOfList).toArray(new IDNumber[0]), this.parentTableName, this.attributeName, this.numObjsInOneSlottedPage, pointer.hashValue, nextSlottedPagePointer);
	}
	
	public int getNumSlottedPages()
	{
		return this.numSlottedPages;
	}
}
