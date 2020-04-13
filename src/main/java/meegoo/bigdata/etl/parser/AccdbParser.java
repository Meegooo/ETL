package meegoo.bigdata.etl.parser;

import meegoo.bigdata.etl.database.Importer;
import meegoo.bigdata.etl.persistence.PersistenceUtil;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AccdbParser {

	public static <T> List<T> parse(File file, Class<T> clazz) throws SQLException {
		//Получаем имя таблицы из класса с помощью рефлексии.
		String table = PersistenceUtil.getTableName(clazz);

		//Подключаемся к базе данных
		try (Connection conn = DriverManager.getConnection("jdbc:ucanaccess://" + file.getAbsolutePath())) {
			//Достаем все данные из таблицы
			PreparedStatement s = conn.prepareStatement("SELECT * FROM " + table);
			ResultSet rs = s.executeQuery();
			//Читаем список колонок в результате
			List<String> columnNames = new ArrayList<>();
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				columnNames.add(rs.getMetaData().getColumnName(i).toLowerCase());
			}
			List<T> output = new ArrayList<>();
			//Пока есть следующие строки, читаем из них данные и добавляем полученный объект в список
			while (rs.next()) {
				T t = parseRow(rs, columnNames, clazz);
				output.add(t);
			}
			System.out.println("[ACCDB] Read " + output.size() + " rows for " + clazz.getName());
			return output;
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
					System.err.printf("[ACCDB] Error parsing field %s. \t%s\n", columnName, e.getMessage());
				}
			}
			return t;
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}
}