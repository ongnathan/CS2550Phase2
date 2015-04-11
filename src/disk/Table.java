package disk;

import data.Attribute;
import data.ClientName;
import data.IDNumber;
import data.PhoneNumber;
import data.Record;

public class Table
{
	public final String name;
	private final AttributeFile<IDNumber> idFile;
	private final AttributeFile<ClientName> clientNameFile;
	private final AttributeFile<PhoneNumber> phoneNumberFile;
	private int numSlottedPages;
	
	public Table(String name, int numHashValues)
	{
		this.name = name;
		this.idFile = new AttributeFile<IDNumber>(name, IDNumber.class.getName(), numHashValues, IDNumber.LENGTH);
		this.clientNameFile = new AttributeFile<ClientName>(name, ClientName.class.getName(), numHashValues, ClientName.LENGTH);
		this.phoneNumberFile = new AttributeFile<PhoneNumber>(name, PhoneNumber.class.getName(), numHashValues, PhoneNumber.LENGTH);
		this.updateNumSlottedPages();
	}
	
	@SuppressWarnings("unchecked")
	public SlottedPage<? extends Attribute>[] getSlottedPages(int startOfTablePageNum, int hashValue)
	{
		SlottedPage<? extends Attribute>[] trial = (SlottedPage<? extends Attribute>[]) new SlottedPage[] {this.idFile.getSlottedPage(startOfTablePageNum, hashValue), this.clientNameFile.getSlottedPage(startOfTablePageNum + this.idFile.getNumSlottedPages(), hashValue), this.phoneNumberFile.getSlottedPage(startOfTablePageNum + this.idFile.getNumSlottedPages() + this.clientNameFile.getNumSlottedPages(), hashValue)};
//		((SlottedPage<IDNumber>)trial[0]).arrayOfObjects;
		return trial;
	}
	
	public SlottedPage<? extends Attribute> getNextSlottedPage(int startOfTablePageNum, SlottedPagePointer pointer)
	{
		if(pointer.attributeName.equals(IDNumber.class.getName()))
		{
			return this.idFile.getNextSlottedPage(startOfTablePageNum, pointer);
		}
		else if(pointer.attributeName.equals(ClientName.class.getName()))
		{
			return this.clientNameFile.getNextSlottedPage(startOfTablePageNum + this.idFile.getNumSlottedPages(), pointer);
		}
		else if(pointer.attributeName.equals(PhoneNumber.class.getName()))
		{
			return this.phoneNumberFile.getNextSlottedPage(startOfTablePageNum + this.idFile.getNumSlottedPages() + this.clientNameFile.getNumSlottedPages(), pointer);
		}
		else
		{
			throw new IllegalArgumentException("Invalid attribute name");
		}
	}
	
	public boolean insertRecord(int hashValue, Record r)
	{
		if(!(this.idFile.insert(hashValue, r.id, r.id) && this.clientNameFile.insert(hashValue, r.clientName, r.id) && this.phoneNumberFile.insert(hashValue, r.phoneNumber, r.id)))
		{
			this.idFile.delete(hashValue, r.id, r.id);
			this.clientNameFile.delete(hashValue, r.clientName, r.id);
			return false;
		}
		this.updateNumSlottedPages();
		return true;
	}
	
	public boolean deleteRecord(int hashValue, Record r)
	{
		boolean b = this.idFile.delete(hashValue, r.id, r.id) && this.clientNameFile.delete(hashValue, r.clientName, r.id) && this.phoneNumberFile.delete(hashValue, r.phoneNumber, r.id);
		this.updateNumSlottedPages();
		return b;
	}
	
	public int getNumSlottedPages()
	{
		return this.numSlottedPages;
	}
	
	private final void updateNumSlottedPages()
	{
		this.numSlottedPages = this.idFile.getNumSlottedPages() + this.clientNameFile.getNumSlottedPages() + this.phoneNumberFile.getNumSlottedPages();
	}
}
