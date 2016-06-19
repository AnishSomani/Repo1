package com.aavishkar.news.ingest;

public class StringUtils {

	public static String capitalize(String name) {
		if (name == null) {
			return null;
		}
		name = name.toLowerCase();
		StringBuilder name1 = new StringBuilder();
		for (int x = 0; x < name.length(); x++) {
			if (x == 0 || name.charAt(x - 1) == ' ' || name.charAt(x - 1) == '\t' || name.charAt(x - 1) == '\n'
					|| name.charAt(x - 1) == '_') {
				name1.append(Character.toUpperCase(name.charAt(x)));
			} else {
				name1.append(name.charAt(x));
			}
			;
		}
		;
		return name1.toString();
	}

}
