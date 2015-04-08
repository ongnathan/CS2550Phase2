package data;

public abstract class Attribute
{
	public final int length;
	public final int minLength;
	public final int maxLength;
	
	public Attribute(int length, int minLength, int maxLength)
	{
		this.length = length;
		this.maxLength = maxLength;
		this.minLength = minLength;
	}
}
