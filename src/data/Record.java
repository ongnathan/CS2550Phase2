package data;

/**
 * The class that represents a record that can be stored in the database or issued by the script.
 * @author Nathan Ong and Jeongmin Lee
 */
public class Record implements Comparable<Record>
{
	//TODO check if null or empty values are allowed
	
	/**
	 * The id number of the record.
	 * The id number must be between 0 and 2^32-1.
	 */
	public final IDNumber id;
	
	/**
	 * The name of the client of the record.
	 * The length of the String must be between 1 and 16.
	 */
	public final ClientName clientName;
	
	/**
	 * The phone number of the record, stored as a String.
	 * The length of the String must be between 1 and 12.
	 */
	public final PhoneNumber phoneNumber;
	
//	private static final int ID_NUM_SIZE = 4;			//The size in bytes of the ID number
//	private static final int MAX_PHONE_LEN = 12;		//The maximum length of the phone number attribute
//	private static final int MIN_PHONE_LEN = 1;			//The minimum length of the phone number attribute
	
	/**
	 * The fixed record size.
	 */
	public static final int LENGTH = IDNumber.LENGTH + ClientName.LENGTH + PhoneNumber.LENGTH;
	
	/**
	 * The constructor.
	 * Takes a string representing a record and converts it into a Record object.
	 * The String must be in the format <code>(id,name,phone)</code>, where id can be converted into a number.
	 * The values must also conform to the restrictions set by the fields.
	 * @param record The String representing the record.
	 * @throws IllegalArgumentException if the String is malformed or violates the value restrictions.
	 */
	public Record(String record)
	{
		String[] split = record.substring(1,record.length()-1).split(",");
		if(split.length != 3)
		{
			throw new IllegalArgumentException("Invalid record.");
		}
		
//		long id = -1L;
//		try
//		{
//			id = Long.parseLong(split[0]);
//		}
//		catch(NumberFormatException e)
//		{
//			throw new IllegalArgumentException("ID number format invalid.");
//		}
//		if(split[1].length() > MAX_NAME_LEN || split[1].length() < MIN_NAME_LEN)
//		{
//			throw new IllegalArgumentException("Invalid record.");
//		}
		
		this.id = new IDNumber(split[0]);
		this.clientName = new ClientName(split[1]);
		this.phoneNumber = new PhoneNumber(split[2]);
	}//end constructor(String)
	
	/**
	 * The constructor.
	 * Takes the values of a Record object and creates a Record from them
	 * The values must also conform to the restrictions set by the fields.
	 * @param id The long representing the id number.
	 * @param clientName The String representing the client name.
	 * @param phoneNumber The String representing the phone number.
	 * @throws IllegalArgumentException if the String is malformed or violates the value restrictions.
	 */
	public Record(IDNumber id, ClientName clientName, PhoneNumber phoneNumber)
	{
//		if(id < MIN_ID_NUM || id > MAX_ID_NUM || clientName == null || clientName.length() > MAX_NAME_LEN || clientName.length() < MIN_NAME_LEN || phoneNumber == null)
//		{
//			throw new IllegalArgumentException("Invalid arguments.");
//		}
		if(id == null || clientName == null || phoneNumber == null)
		{
			throw new IllegalArgumentException("Invalid arguments.");
		}
		
		this.id = id;
		this.clientName = clientName;
		this.phoneNumber = phoneNumber;
	}
	
	/**
	 * {@inheritDoc}
	 * Compares the id numbers.
	 */
	@Override
	public int compareTo(Record arg0)
	{
		return this.id.compareTo(arg0.id);
	}
	
	/**
	 * {@inheritDoc}
	 * The String is in the form <code>(ID,ClientName,PhoneNumber)</code>.
	 */
	@Override
	public String toString()
	{
		return "(" + this.id.toString() + "," + this.clientName.toString() + "," + this.phoneNumber.toString() + ")";
	}
	
	public boolean equals(Object o)
	{
		if(!(o instanceof Record))
		{
			return false;
		}
		return this.id.equals(((Record)o).id);
	}
}//end class Record
