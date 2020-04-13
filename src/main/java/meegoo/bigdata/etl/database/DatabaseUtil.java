package meegoo.bigdata.etl.database;

import meegoo.bigdata.etl.persistence.PersistenceUtil;

import java.sql.*;

public class DatabaseUtil {
	public static <T> int getNextKeyValue(Class<T> clazz) {
		try {
			//Получаем имя таблицы и название столбца с первичным ключом.
			String tableName = "bigdata." + PersistenceUtil.getTableName(clazz);
			String keyName = PersistenceUtil.getKeyName(clazz);
			//Получаем максимальное значение в столбце с первичным ключом заданной таблицы
			Statement statement = ConnectionManager.getConnection().createStatement();
			ResultSet resultSet = statement.executeQuery(
					String.format("SELECT COALESCE(MAX(%s), 0) as max_id FROM %s", keyName, tableName));
			resultSet.next();
			//Возвращаем значение на 1 больше - номер следующего ключа.
			int anInt = resultSet.getInt(1);
			return anInt+1;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> void updateNextKeyValue(Class<T> clazz) {
		try {
			//Получаем имя таблицы и название столбца с первичным ключом.
			String tableName = "bigdata." + PersistenceUtil.getTableName(clazz);
			String keyName = PersistenceUtil.getKeyName(clazz);

			//Получаем максимальное значение в столбце с первичным ключом заданной таблицы,
			//присваиваем его к текущему значениию первичного ключа
			Statement statement = ConnectionManager.getConnection().createStatement();
			ResultSet resultSet = statement.executeQuery(
				String.format("SELECT setval(pg_get_serial_sequence('%s', '%s'), (SELECT COALESCE(MAX(%s), 1) FROM %s));",
						tableName, keyName.toLowerCase(), keyName, tableName));
			resultSet.next();
			System.out.println("Updated postgres serial value for " + clazz.getName() +
					". New value is " + resultSet.getInt(1));
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
