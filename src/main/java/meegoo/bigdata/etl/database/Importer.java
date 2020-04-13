package meegoo.bigdata.etl.database;

import meegoo.bigdata.etl.persistence.Column;
import meegoo.bigdata.etl.persistence.PersistenceUtil;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Importer {

	public static <T> Set<T> importFromPostgres(Class<T> clazz, List<String> fields) {
		try {
			//Получаем имя таблицы из класса с помощью рефлексии.
			String table = "bigdata." + PersistenceUtil.getTableName(clazz);

			//Проверяем, что все запрошенные поля существуют в переданном классе.
			Set<String> clazzFieldNames = PersistenceUtil.getFieldMap(clazz).keySet();
			if (!clazzFieldNames.containsAll(fields)) {
				fields = new ArrayList<>(fields);
				fields.removeAll(clazzFieldNames);
				throw new IllegalArgumentException("Unknown fields " + String.join(", ", fields));
			}
			//Создаем соединение
			PreparedStatement s = ConnectionManager.getConnection()
					.prepareStatement("SELECT " + String.join(", ", fields) + " FROM " + table);
			ResultSet rs = s.executeQuery();
			//Читаем имена колонок
			List<String> columnNames = new ArrayList<>();
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				columnNames.add(rs.getMetaData().getColumnName(i).toLowerCase());
			}
			Set<T> output = new HashSet<>();
			//Пока есть следующие строки, читаем из них данные и добавляем полученный объект в список
			while (rs.next()) {
				T t = parseRow(rs, columnNames, clazz);
				output.add(t);
			}
			System.out.println("[PSQL] Read " + output.size() + " rows for " + clazz.getName());
			return output;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private static <T> T parseRow(ResultSet resultSet, List<String> columnNames, Class<T> clazz) {
		try {
			//С помощью рефлексии, создаем объект переданного в параметрах класса.
			T t = clazz.getDeclaredConstructor(new Class[]{}).newInstance();
			//Проходим по всем колонкам и заполняем новый объект значениями из текущей строки
			for (int i = 1; i <= columnNames.size(); i++) {
				String columnName = columnNames.get(i - 1);
				try {
					//Данный метод принимает на вход результат запроса, имя текущего столбца, класс объекта и сам объект
					//После чего присваивает полю объекта, соответствующему данной колонке, значение из результата запроса.
					Importer.insertToField(resultSet, columnName, clazz, t);
				} catch (Exception e) {
					System.err.printf("[PSQL] Error parsing field %s. \t%s\n", columnName, e.getMessage());
				}
			}
			return t;
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> void insertToField(ResultSet resultSet, String columnName, Class<T> clazz, T t)
			throws ReflectiveOperationException, SQLException {
		//Получаем все поля в классе
		for (Field declaredField : clazz.getDeclaredFields()) {
			Column annotation = declaredField.getAnnotation(Column.class);
			//Если у поля есть аннотация Column и ее имя равно искомому
			if (annotation != null && annotation.name().toLowerCase().equals(columnName)) {
				//Делаем поле доступным для записи из рефлексии
				declaredField.setAccessible(true);
				//И, в зависимости от типа поля, присваиваем ему значение.
				if (declaredField.getType().equals(boolean.class) || declaredField.getType().equals(Boolean.class)) {
					declaredField.set(t, resultSet.getBoolean(columnName));

				} else if (declaredField.getType().equals(byte.class) || declaredField.getType().equals(Byte.class)) {
					declaredField.set(t, resultSet.getByte(columnName));

				} else if (declaredField.getType().equals(short.class) || declaredField.getType().equals(Short.class)) {
					declaredField.set(t, resultSet.getShort(columnName));

				} else if (declaredField.getType().equals(char.class) || declaredField.getType().equals(Character.class)) {
					String string = resultSet.getString(columnName);
					Character result = null;
					if (string != null && string.length() == 1) {
						result = string.charAt(0);
					}
					declaredField.set(t, result);

				} else if (declaredField.getType().equals(int.class) || declaredField.getType().equals(Integer.class)) {
					declaredField.set(t, resultSet.getInt(columnName));

				} else if (declaredField.getType().equals(long.class) || declaredField.getType().equals(Long.class)) {
					declaredField.set(t, resultSet.getLong(columnName));

				} else if (declaredField.getType().equals(float.class) || declaredField.getType().equals(Float.class)) {
					declaredField.set(t, resultSet.getFloat(columnName));

				} else if (declaredField.getType().equals(double.class) || declaredField.getType().equals(Double.class)) {
					declaredField.set(t, resultSet.getDouble(columnName));

				} else if (declaredField.getType().equals(String.class)) {
					declaredField.set(t, resultSet.getString(columnName).trim());

				} else if (declaredField.getType().equals(java.sql.Date.class)) {
					declaredField.set(t, resultSet.getDate(columnName));

				} else if (declaredField.getType().equals(java.sql.Time.class)) {
					declaredField.set(t, resultSet.getTime(columnName));

				} else if (declaredField.getType().equals(java.util.Date.class)) {
					declaredField.set(t, new java.util.Date(resultSet.getTimestamp(columnName).toInstant().toEpochMilli()));

				} else if (declaredField.getType().equals(java.util.Calendar.class)) {
					Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
					declaredField.set(t, resultSet.getDate(columnName, calendar));

				} else if (declaredField.getType().equals(java.time.Instant.class)) {
					declaredField.set(t, resultSet.getTimestamp(columnName).toInstant());

				} else if (declaredField.getType().equals(java.time.LocalDate.class)) {
					declaredField.set(t, resultSet.getTimestamp(columnName).toLocalDateTime().toLocalDate());

				} else if (declaredField.getType().equals(java.time.LocalTime.class)) {
					declaredField.set(t, resultSet.getTimestamp(columnName).toLocalDateTime().toLocalTime());

				} else if (declaredField.getType().equals(java.time.LocalDateTime.class)) {
					declaredField.set(t, resultSet.getTimestamp(columnName).toLocalDateTime());

				} else {
					throw new ReflectiveOperationException("Unsupported field type " + declaredField.getType().toString());
				}
			}
		}
	}
}
