package org.sunbird.workflow.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;


public class ValidationUtil {


	public static final String DEFAULT_BULK_UPLOAD_VERIFICATION_REGEX = "^[a-zA-Z\\s,]+$";

	public static boolean isStringNullOREmpty(String value) {
		return (value == null || "".equals(value.trim()));
	}

	public static Boolean validateEmailPattern(String email) {
		String emailRegex = "^[a-zA-Z0-9_+&*-]+(?:\\." + "[a-zA-Z0-9_+&*-]+)*@" + "(?:[a-zA-Z0-9-]+\\.)+[a-z"
				+ "A-Z]{2,7}$";
		Pattern pat = Pattern.compile(emailRegex);
		if (pat.matcher(email).matches()) {
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	public static Boolean validateContactPattern(String contactNumber) {
		String contactNumberRegex = "^\\d{10}$";
		Pattern pat = Pattern.compile(contactNumberRegex);
		if (pat.matcher(contactNumber).matches()) {
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}

	public static Boolean validateDate(String dateString){
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
		dateFormat.setLenient(false);
		try {
			Date todaysDate = new Date();
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.YEAR, -65);
			Date pastDate = calendar.getTime();
			Date date = dateFormat.parse(dateString);
			return date.after(pastDate) && (date.before(todaysDate) || date.equals(todaysDate));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static Boolean validateExternalSystemId(String externalSystemId) {
		return externalSystemId.matches("^(?=.{1,30}$)[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*$");
	}

	public static Boolean validateExternalSystem(String externalSystem) {
		return externalSystem.matches("^(?=.*[a-zA-Z .-])[a-zA-Z0-9 .-]{1,255}$"); // Allow only alphanumeric, alphabets and restrict if only numeric character
	}

	public static Boolean validateFullName(String firstName) {
		return firstName.matches("^(?!.*\\n)[a-zA-Z]+(?:['\\s][a-zA-Z]+)*(?<!\\.|\\s)$");
	}

	public static Boolean validateTag(List<String> tags) {
		String regEx = DEFAULT_BULK_UPLOAD_VERIFICATION_REGEX;
		for (String tag : tags) {
			if (!tag.matches(regEx)) {
				return false;
			}
		}
		return true;
	}

	public static Boolean validateEmployeeId(String employeeId) {
		return employeeId.matches("^[a-zA-Z0-9]{1,30}$"); // Allow alphabetic, alphanumeric, and numeric character(s).
	}

	public static Boolean validateRegexPatternWithNoSpecialCharacter(String regex) {
		return regex.matches("^[a-zA-Z0-9 -()]*$");
	}

	public static Boolean validatePinCode(String regex) {
		return regex.matches("^[0-9]{6}$");
	}
}