package data;

public class ClientName extends Attribute implements Comparable<ClientName>
{
	public final String name;
	public static final int LENGTH = 12;
	private static final int MAX_NAME_LEN = 16;			//The maximum length of the name attribute
	private static final int MIN_NAME_LEN = 1;			//The minimum length of the name attribute
	
	/**
	 * The constructor.
	 * it takes a client name as a string and if the name is null or less than the minimum length
	 * or more than the max length it throws an Exception
	 * @param name the name to be specified
	 */
	
	public ClientName(String name)
	{
		super(LENGTH, MIN_NAME_LEN, MAX_NAME_LEN);
		if(name == null || name.length() < MIN_NAME_LEN || name.length() > MAX_NAME_LEN)
		{
			throw new IllegalArgumentException("Invalid client name length");
		}
		this.name = name;
	}

	@Override
	
	public int compareTo(ClientName arg0)
	{
		return this.name.compareTo(arg0.name);
	}
	
	@Override
	public String toString()
	{
		return this.name;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if(!(o instanceof ClientName))
		{
			return false;
		}
		return this.name.equals(((ClientName)o).name);
	}
}
