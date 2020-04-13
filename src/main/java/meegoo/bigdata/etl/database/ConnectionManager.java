package meegoo.bigdata.etl.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {

	private static final Properties properties = new Properties();
	private static Connection currentConnection;

	static {
		properties.setProperty("user", "test");
		properties.setProperty("password", "password");
	}

	public static Connection getConnection() {
		try {
			if (currentConnection == null || !currentConnection.isValid(2)) {
				currentConnection = DriverManager.getConnection("jdbc:postgresql://10.8.0.1/testdb", properties);
			}
			return currentConnection;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
