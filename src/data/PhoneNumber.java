package data;

/**
 * The object representing the phone number.
 * The phone number can be divided into two parts: the area code, and the remaining phone number.
 * The phone number is solely compared based on the area code.
 * The area code is of length 3, and the whole phone number is of length 12.
 * @author Nathan Ong and Jeongmin Lee
 *
 */
public class PhoneNumber extends Attribute implements Comparable<PhoneNumber>
{
	/**
	 * The area code of the phone number.
	 */
	public final String areaCode;
	
	/**
	 * The remaining portion of the phone number (excluding the area code).
	 */
	public final String remainder;
	
	private static final int AREA_CODE_LENGTH = 3;		//The length of the area code
	private static final int PHONE_NUMBER_LENGTH = 12;	//The length of the whole phone number
	
	public static final int LENGTH = PHONE_NUMBER_LENGTH;
	
	/**
	 * The constructor.
	 * Takes in a String representation of the 10-digit phone number with two line breaks.
	 * @param phoneNumber The String representing the phone number.
	 */
	public PhoneNumber(String phoneNumber)
	{
		super(LENGTH, LENGTH, LENGTH);
		if(phoneNumber == null || phoneNumber.length() != PHONE_NUMBER_LENGTH)
		{
			throw new IllegalArgumentException("The phone number is not in the correct format");
		}
		this.areaCode = phoneNumber.substring(0,AREA_CODE_LENGTH);
		this.remainder = phoneNumber.substring(AREA_CODE_LENGTH,PHONE_NUMBER_LENGTH);
	}
	
	/**
	 * {@inheritDoc}
	 * NOTE: COMPARES THE AREA CODES ONLY, NOT THE WHOLE STRING
	 */
	@Override
	public int compareTo(PhoneNumber arg0)
	{
		return this.areaCode.compareTo(arg0.areaCode);
	}
	
	public boolean equals(Object o)
	{
		if(!(o instanceof PhoneNumber))
		{
			return false;
		}
		return this.areaCode.equals(((PhoneNumber)o).areaCode);
	}
	
	/**
	 * {@inheritDoc}
	 * Returns the whole phone number.
	 */
	@Override
	public String toString()
	{
		return this.areaCode + this.remainder;
	}
}//end class PhoneNumber
