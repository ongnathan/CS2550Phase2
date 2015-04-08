package disk;

public class SlottedPagePointer
{
	public final String tableName;
	public final String attributeName;
	public final int hashValue;
	public final int pagePointer;
	
	public SlottedPagePointer(String tableName, String attributeName, int hashValue, int pagePointer)
	{
		this.tableName = tableName;
		this.attributeName = attributeName;
		this.hashValue = hashValue;
		this.pagePointer = pagePointer;
	}
}
