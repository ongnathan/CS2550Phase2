package disk;

import data.IDNumber;
import data.Record;
import parser.Command;

 /**
  * this class is used to keep track of the changes in command to change the state of the disk
  * when a page is evicted from the memory.
  * only to commands are insert or delete 
  *
  */

public class DiskOperation
{
	public final Command command;	// the command type 
	private final Object value;		// may be record or IDNumber
	public final String tableName;	// the name of the table that the value belongs to 
	
	
	/**
	 * The constructor
	 * it takes the command that can be issued by a script mainly (either insert or delete) because 
	 * these two commands are the only commands that can change the state of the disk. 
	 * @param command type of command 
	 * @param value the value which can be a record or an id number 
	 * @param tableName the table name 
	 */
	public DiskOperation(Command command, Object value, String tableName)
	{
		switch(command)
		{
			case INSERT:
				if(!(value instanceof Record))
				{
					throw new IllegalArgumentException("This operation must have a record associated with it.");
				}
				break;
			case DELETE_TABLE:
				if(!(value instanceof IDNumber))
				{
					throw new IllegalArgumentException("This operation must have an IDNumber associated with it.");
				}
				break;
			default:
				throw new IllegalArgumentException("Disk does not handle this command");
		}
		this.command = command;
		this.value = value;
		this.tableName = tableName;
	}
	
	/**
	 * this is used to get the record that needed to be inserted to the disk.
	 * @return it returns the record to be inserted 
	 */
	
	public Record getRecord()
	{
		if(this.command != Command.INSERT)
		{
			return null;
		}
		return (Record)this.value;
	}
	
	/**
	 * to get the id number of the record that needed to be deleted
	 * @return the in nmber
	 */
	
	public IDNumber getIDNumber()
	{
		if(this.command != Command.DELETE_TABLE)
		{
			return null;
		}
		return (IDNumber)this.value;
	}
}
