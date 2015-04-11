package data;

public class IDNumber extends Attribute implements Comparable<IDNumber>
{
	public final long value;
	public static final int LENGTH = 4;
	private static final long MAX_ID_NUM = 4294967295L;	//The maximum id number, 2^32-1
	private static final long MIN_ID_NUM = -1L;			//The minimum id number
	
	public IDNumber(String value)
	{
		this(Long.parseLong(value));
	}
	
	/**
	 * The constructor
	 * it takes the value of a given IDNumber and if the id is more than MAX_ID_NUM
	 * or less than MIN_ID_NUM it will through IllegalArgumentException
	 * @param value the value of the IDNumber
	 */
	
	public IDNumber(long value)
	{
		super(LENGTH, LENGTH, LENGTH);
		if(value < MIN_ID_NUM || value > MAX_ID_NUM)
		{
			throw new IllegalArgumentException("ID number out of bounds.");
		}
		this.value = value;
	}

	@Override
	public int compareTo(IDNumber arg0)
	{
		return new Long(this.value).compareTo(new Long(arg0.value));
	}
	
	@Override
	public String toString()
	{
		return String.valueOf(this.value);
	}
	
	public boolean equals(Object o)
	{
		if(!(o instanceof IDNumber))
		{
			return false;
		}
		return this.value == ((IDNumber)o).value;
	}
}
