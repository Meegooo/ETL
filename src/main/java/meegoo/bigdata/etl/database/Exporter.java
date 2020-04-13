package meegoo.bigdata.etl.database;

import meegoo.bigdata.etl.persistence.PersistenceUtil;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Exporter {
	public static <T> void exportToPostgres(Collection<T> collection, Class<T> clazz) {
		try {
			//Получаем имя таблицы
			String table = "bigdata." + PersistenceUtil.getTableName(clazz);
			//Получаем список имен столбцов.
			List<Map.Entry<String, Field>> fieldMap = new ArrayList<>(PersistenceUtil.getFieldMap(clazz).entrySet());
			//Получаем подключение.
			Connection connection = ConnectionManager.getConnection();
			//Создаем строку вида (name1, name2, name3) с именами всех столбцов.
			String names = fieldMap.stream()
					.map(Map.Entry::getKey)
					.collect(Collectors.joining(", ", "(", ")"));
			//Создаем строку вида (?, ?, ?) с количеством вопросов равным количеству столбцов
			String parameters = IntStream.range(0, fieldMap.size())
					.mapToObj(it -> "?")
					.collect(Collectors.joining(", ", "(", ")"));
			//Создаем запрос. Полученный запрос будет выглядеть как
			// "INSERT INTO bigdata.table (name1, name2, name3) VALUES (?, ?, ?)"
			StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ")
					.append(table)
					.append(names)
					.append(" VALUES ")
					.append(parameters);
			//Создаем PreparedStatement
			PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder.toString());

			//Добавляем все записи из переданной коллекции к пакету в PreparedStatement
			for (T t : collection) {
				for (int i = 0; i < fieldMap.size(); i++) {
					Map.Entry<String, Field> entry = fieldMap.get(i);
					preparedStatement.setObject(i + 1, entry.getValue().get(t));
				}
				preparedStatement.addBatch();
			}
			//Отправляем запрос
			preparedStatement.executeBatch();

			System.out.println("Exported " + collection.size() + " rows to postgres for " + clazz.getName());
		} catch (SQLException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
