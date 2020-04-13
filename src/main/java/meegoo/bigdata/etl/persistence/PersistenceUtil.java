package meegoo.bigdata.etl.persistence;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class PersistenceUtil {
	public static <T> String getKeyName(Class<T> clazz) {
		//Получаем все поля в классе
		for (Field declaredField : clazz.getDeclaredFields()) {
			//Получаем аннотацию @Key у каждого поля
			Key key = declaredField.getAnnotation(Key.class);
			//Если она найдена
			if (key != null) {
				//Получаем аннотацию @Column у данного поля
				Column column = declaredField.getAnnotation(Column.class);
				//Если она существует, то возвращаем имя столбца, иначе вызываем исключение
				if (column != null) {
					return column.name();
				} else {
					throw new IllegalArgumentException("No name found for key column");
				}
			}
		}
		throw new IllegalArgumentException("No key column found");
	}

	public static <T> String getTableName(Class<T> clazz) {
		//Получаем значение аннотации @Table из класса clazz
		Table annotation = clazz.getAnnotation(Table.class);
		//Если аннотация найдена, возвращаем ее имя, иначе вызываем исключение
		if (annotation == null)
			throw new IllegalArgumentException("Passed class type does not contain required @Table annotation");
		return annotation.name();
	}

	public static <T> Map<String, Field> getFieldMap(Class<T> clazz) {
		//Получаем все поля в классе
		Map<String, Field> fieldMap = new HashMap<>();
		//Проходим по всем полям
		for (Field declaredField : clazz.getDeclaredFields()) {
			//Делаем поле доступным для записи с помощью рефлексии
			declaredField.setAccessible(true);
			//Если у поля есть аннотация Column,
			//добавляем соотношение между именем столбца и самим полем в Map
			Column column = declaredField.getAnnotation(Column.class);
			if (column != null) {
				fieldMap.put(column.name(), declaredField);
			}
		}
		return fieldMap;
	}
}
