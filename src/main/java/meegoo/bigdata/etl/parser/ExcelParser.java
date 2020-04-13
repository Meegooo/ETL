package meegoo.bigdata.etl.parser;

import meegoo.bigdata.etl.persistence.Column;
import meegoo.bigdata.etl.persistence.PersistenceUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.*;
import java.util.*;

public class ExcelParser {
	private static DataFormatter formatter = new DataFormatter();

	public static <T> List<T> parse(File file, Class<T> clazz) throws IOException {
		//Получаем имя листа из класса с помощью рефлексии
		String sheetName = PersistenceUtil.getTableName(clazz);
		//Открываем файл и необходимый лист.
		FileInputStream fis = new FileInputStream(file);
		XSSFWorkbook workbook = new XSSFWorkbook(fis);
		XSSFSheet sheet = workbook.getSheet(sheetName);
		Iterator<Row> rowIterator = sheet.iterator();

		//Из первой строки читаем имена колонок и сохраняем в список.
		List<String> columnNames = new ArrayList<>();
		if (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			for (Cell cell : row) {
				columnNames.add(formatter.formatCellValue(cell).toLowerCase());
			}
		}

		//Пока есть следующие строки, читаем из них данные и добавляем полученный объект в список
		List<T> list = new ArrayList<>();
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			T t = parseRow(row, columnNames, clazz);
			list.add(t);
		}

		System.out.println("[XLSX] Read " + list.size() + " rows for " + clazz.getName());
		return list;
	}

	private static <T> T parseRow(Row row, List<String> columns, Class<T> clazz) {
		try {
			//С помощью рефлексии, создаем объект переданного в параметрах класса.
			T t = clazz.getDeclaredConstructor(new Class[]{}).newInstance();
			//Проходим по всем колонкам и заполняем новый объект значениями из текущей строки
			Iterator<String> columnsIterator = columns.iterator(); //Итератор по именам колонок
			for (int i = 0; i < columns.size(); i++) {
				Cell cell = row.getCell(i);
				if (columnsIterator.hasNext()) {
					String column = columnsIterator.next();
					//Пытаемся прочитать дату, так как восстановить ее из текста не тривиально
					java.util.Date date = null;
					try {
						date = cell.getDateCellValue();
					} catch (Exception ignored) {
					}
					//Также читаем строкое представление данных
					String value = formatter.formatCellValue(cell);
					try {
						//Данный метод принимает на вход прочитанное значение в виде строки и как дату,
						//имя текущего столбца, класс объекта и сам объект
						//После чего присваивает полю объекта, соответствующему данной колонке, значение из результата запроса.
						insertToField(value, date, column, clazz, t);
					} catch (Exception e) {
						System.err.printf("[XSLX] Error parsing field %s. \t%s\n", column, e.getMessage());
					}
				}
			}
			return t;
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	private static <T> void insertToField(String value, java.util.Date dateValue, String column, Class<T> clazz, T t)
			throws ReflectiveOperationException {
		//Получаем все поля в классе
		for (Field declaredField : clazz.getDeclaredFields()) {
			Column annotation = declaredField.getAnnotation(Column.class);
			//Если у поля есть аннотация Column и ее имя равно искомому
			if (annotation != null && annotation.name().toLowerCase().equals(column)) {
				//Делаем поле доступным для записи из рефлексии
				declaredField.setAccessible(true);
				//Пытаемся прочитать целочисленное значение и значение с плавающей точкой из ячейки.
				//Если не получается, игнорируем.
				Double doubleValue = null;
				Long longValue = null;
				try {
					doubleValue = Double.parseDouble(value);
					longValue = doubleValue.longValue();
				} catch (NumberFormatException ignored) {
				}

				//В зависимости от типа присваеваем полю соответствующее значение.
				if (declaredField.getType().equals(boolean.class) || declaredField.getType().equals(Boolean.class)) {
					Boolean bool = longValue == null ? null : (longValue != 0);
					if (bool == null) {
						bool = Boolean.parseBoolean(value);
						declaredField.set(t, bool);
					}
				} else if (declaredField.getType().equals(byte.class) || declaredField.getType().equals(Byte.class)) {
					if (longValue == null || longValue < Byte.MIN_VALUE || longValue > Byte.MAX_VALUE) {
						throw new NumberFormatException("Can't safely cast " + value + " to byte");
					} else {
						declaredField.set(t, longValue.byteValue());
					}
				} else if (declaredField.getType().equals(short.class) || declaredField.getType().equals(Short.class)) {
					if (longValue == null || longValue < Short.MIN_VALUE || longValue > Short.MAX_VALUE) {
						throw new NumberFormatException("Can't safely cast " + value + " to short");
					} else {
						declaredField.set(t, longValue.shortValue());
					}
				} else if (declaredField.getType().equals(char.class) || declaredField.getType().equals(Character.class)) {
					if ((longValue == null || longValue < Character.MIN_VALUE || longValue > Character.MAX_VALUE)
							&& value.length() != 1) {
						throw new NumberFormatException("Can't safely cast " + value + " to char");
					} else if (longValue != null) {
						declaredField.set(t, (char) longValue.longValue());
					} else {
						declaredField.set(t, value.charAt(0));
					}
				} else if (declaredField.getType().equals(int.class) || declaredField.getType().equals(Integer.class)) {
					if (longValue == null || longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
						throw new NumberFormatException("Can't safely cast " + value + " to int");
					} else {
						declaredField.set(t, longValue.intValue());
					}
				} else if (declaredField.getType().equals(long.class) || declaredField.getType().equals(Long.class)) {
					if (longValue == null) {
						throw new NumberFormatException("Can't safely cast " + value + " to long");
					} else {
						declaredField.set(t, longValue);
					}
				} else if (declaredField.getType().equals(float.class) || declaredField.getType().equals(Float.class)) {
					if (doubleValue == null) {
						throw new NumberFormatException("Can't safely cast " + value + " to float");
					} else {
						declaredField.set(t, doubleValue.floatValue());
					}
				} else if (declaredField.getType().equals(double.class) || declaredField.getType().equals(Double.class)) {
					if (doubleValue == null) {
						throw new NumberFormatException("Can't safely cast " + value + " to double");
					} else {
						declaredField.set(t, doubleValue);
					}
				} else if (declaredField.getType().equals(String.class)) {
					declaredField.set(t, value);
				} else if (declaredField.getType().equals(java.sql.Date.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					java.sql.Date date = new java.sql.Date(dateValue.getTime());
					declaredField.set(t, date);
				} else if (declaredField.getType().equals(java.sql.Time.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					java.sql.Time date = new java.sql.Time(dateValue.getTime());
					declaredField.set(t, date);
				} else if (declaredField.getType().equals(java.sql.Timestamp.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					java.sql.Timestamp date = new java.sql.Timestamp(dateValue.getTime());
					declaredField.set(t, date);
				} else if (declaredField.getType().equals(java.util.Date.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					declaredField.set(t, dateValue);
				} else if (declaredField.getType().equals(java.util.Calendar.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
					calendar.setTime(dateValue);
					declaredField.set(t, calendar);
				} else if (declaredField.getType().equals(java.time.Instant.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					Instant instant = dateValue.toInstant();
					declaredField.set(t, instant);
				} else if (declaredField.getType().equals(java.time.LocalDate.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					LocalDate localDate = dateValue.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
					declaredField.set(t, localDate);
				} else if (declaredField.getType().equals(java.time.LocalTime.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					LocalTime localTime = dateValue.toInstant().atZone(ZoneOffset.UTC).toLocalTime();
					declaredField.set(t, localTime);
				} else if (declaredField.getType().equals(java.time.LocalDateTime.class)) {
					if (dateValue == null) {
						throw new IllegalArgumentException("Date is invalid");
					}
					LocalDateTime localTime = dateValue.toInstant().atZone(ZoneOffset.UTC).toLocalDateTime();
					declaredField.set(t, localTime);
				} else {
					throw new ReflectiveOperationException("Unsupported field type " + declaredField.getType().toString());
				}
			}
		}
	}
}
