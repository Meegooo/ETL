package meegoo.bigdata.etl.etl;

import meegoo.bigdata.etl.model.PartModel;
import meegoo.bigdata.etl.model.SupplierModel;
import meegoo.bigdata.etl.model.SupplyModel;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Cleaner {
	public static List<SupplierModel> cleanSupplierModel(List<SupplierModel> suppliers, Set<SupplierModel> suppliersPostgres) {
		//Убираем дубликаты по ограничению UNIQUE (SName,Address, SCity)
		//Set не позволяет хранить в себе два одинаковых объекта,
		//а так как два объекта SupplierModel считаются равными если равны поля Name, Address, City
		//то созданием Set дубликаты автоматически убираются
		Set<SupplierModel> suppliersSet = new HashSet<>(suppliers);

		//Удаляем записи с пустым именем
		suppliersSet = suppliersSet.stream()
				.filter(s -> s.getName() != null && !s.getName().isBlank())
				.collect(Collectors.toSet());

		//Ищем самый часто встречаемый город
		Optional<Map.Entry<String, Long>> cityOptional = suppliersSet.stream()
				.map(SupplierModel::getCity) //из каждого поставщика получаем город
				.filter(s -> s != null && !s.isBlank()) //убираем ошибочные города
				.collect(Collectors.groupingBy(it -> it, Collectors.counting())) //считаем сколько встретился каждый город
				.entrySet().stream()
				.max(Map.Entry.comparingByValue()); //берем город, который встречается чаще всего

		//Если город найден заменяем ошибочные записи, иначе удаляем
		if (cityOptional.isEmpty()) {
			suppliersSet = suppliersSet.stream()
					.filter(it -> it.getCity() != null && !it.getCity().isBlank())
					.collect(Collectors.toSet());
		} else {
			suppliersSet = suppliersSet.stream().peek(it -> {
				if (it.getCity() == null || it.getCity().isBlank()) it.setCity(cityOptional.get().getKey());
			}).collect(Collectors.toSet());
		}

		//Ищем cамый часто встречаемый риск для каждого города
		Map<String, Integer> riskForCity = suppliersSet.stream()
				.filter(it -> it.getRisk() != null && it.getRisk() >= 1 && it.getRisk() <= 3) //убираем ошибочные риски
				.collect(Collectors.groupingBy(SupplierModel::getCity)) //группируем по городу
				.entrySet().stream()
				.collect(Collectors.toMap(
						Map.Entry::getKey, //ключ = город, оставляем на месте
						it -> it.getValue().stream()
								.map(SupplierModel::getRisk) //из каждого поставщика получем риск
								//считаем, сколько встретился каждый риск
								.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
								.entrySet().stream()
								.max(Map.Entry.comparingByValue()) //берем риск с максимальным количеством попаданий
								.orElse(new AbstractMap.SimpleEntry<>(null, null))
								.getKey() // берем значение риска
				)) //получаем отношение город->риск
				.entrySet().stream()
				.filter(it -> it.getValue() != null) //убираем неверные риски
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		//Заменяем ошибочные риски если найдены, иначе удаляем запись
		suppliersSet = suppliersSet.stream().peek((it) -> {
			if ((it.getRisk() == null || it.getRisk() <= 0 || it.getRisk() >= 4) && riskForCity.containsKey(it.getCity())) {
				it.setRisk(riskForCity.get(it.getCity()));
			}
		}).filter(it -> it.getRisk() != null).collect(Collectors.toSet());

		System.out.println("Cleaned " + suppliersSet.size() + " rows for " + SupplierModel.class.getName());

		//Создаем отношение, в котором каждому объекту из Postgres соответствует этот же объект.
		//Используется для получения дубликатов заданной записи
		Map<SupplierModel, SupplierModel> suppliersPostgresMap = suppliersPostgres.stream()
				.collect(Collectors.toMap(it -> it, it -> it));

		//Создаем список дубликатов между импортируемыми данными и данными в БД.
		Set<SupplierModel> duplicates = new HashSet<>(suppliers);
		duplicates.retainAll(suppliersPostgres);

		//Для дубликатов с postgresql присваиваем полю duplicate ссылку на объект-дубликат.
		for (SupplierModel duplicate : duplicates) {
			duplicate.setDuplicate(suppliersPostgresMap.get(duplicate));
			suppliersPostgresMap.get(duplicate).setDuplicate(duplicate);
		}

		return new ArrayList<>(suppliersSet);
	}

	public static List<PartModel> cleanPartModel(List<PartModel> parts, Set<PartModel> partsPostgres) {
		//Убираем дубликаты по ограничению UNIQUE (PName, PCity, Color)
		//Set не позволяет хранить в себе два одинаковых объекта,
		//а так как два объекта SupplierModel считаются равными если равны поля Name, Address, City
		//то созданием Set дубликаты автоматически убираются
		Set<PartModel> partsSet = new HashSet<>(parts);

		//Убираем записи с пустым именем
		partsSet = partsSet.stream()
				.filter(s -> s.getName() != null && !s.getName().isBlank())
				.collect(Collectors.toSet());

		//Ищем самый часто встречаемый город
		Optional<Map.Entry<String, Long>> cityOptional = partsSet.stream()
				.map(PartModel::getCity)//из каждой детали получаем город
				.filter(s -> s != null && !s.isBlank())//убираем ошибочные города
				//считаем сколько встретился каждый город
				.collect(Collectors.groupingBy(it -> it, Collectors.counting()))
				.entrySet().stream()
				.max(Map.Entry.comparingByValue());//берем город, который встречается чаще всего

		//Если город найден заменяем ошибочные записи, иначе удаляем
		if (cityOptional.isEmpty()) {
			partsSet = partsSet.stream()
					.filter(it -> it.getCity() != null && !it.getCity().isBlank())
					.collect(Collectors.toSet());
		} else {
			partsSet = partsSet.stream().peek(it -> {
				if (it.getCity() == null || it.getCity().isBlank()) it.setCity(cityOptional.get().getKey());
			}).collect(Collectors.toSet());
		}

		//Ищем средний вес для каждого города
		Map<String, Double> averageWeightForCity = partsSet.stream()
				.filter(it -> it.getWeight() != null && it.getWeight() > 0) //убираем ошибочные веса
				//Группируем по городу, а как значение берем средний вес.
				.collect(Collectors.groupingBy(PartModel::getCity, Collectors.averagingDouble(PartModel::getWeight)));

		//заменяем ошибочные веса если найдены, иначе удаляем запись
		partsSet = partsSet.stream().peek(it -> {
			if ((it.getWeight() == null || it.getWeight() <= 0) && averageWeightForCity.containsKey(it.getCity())) {
				it.setWeight(averageWeightForCity.get(it.getCity()));
			}
		}).filter(it -> it.getWeight() != null).collect(Collectors.toSet());

		System.out.println("Cleaned " + partsSet.size() + " rows for " + PartModel.class.getName());

		//Создаем отношение, в котором каждому объекту из Postgres соответствует этот же объект.
		//Используется для получения дубликатов заданной записи
		Map<PartModel, PartModel> partsPostgresMap = partsPostgres.stream().
				collect(Collectors.toMap(it -> it, it -> it));

		//Создаем список дубликатов между импортируемыми данными и данными в БД.
		Set<PartModel> duplicates = new HashSet<>(parts);
		duplicates.retainAll(partsPostgres);

		//Для дубликатов присваиваем полю duplicate ссылку на объект-дубликат.
		for (PartModel duplicate : duplicates) {
			duplicate.setDuplicate(partsPostgresMap.get(duplicate));
			partsPostgresMap.get(duplicate).setDuplicate(duplicate);
		}

		return new ArrayList<>(partsSet);
	}

	public static List<SupplyModel> cleanSupplyModel(List<SupplyModel> supplies,
	                                                 List<PartModel> parts, List<SupplierModel> suppliers) {
		//Создаем отношение PID->Weight. Если объект является дубликатом, то берем вес из уже существующей записи
		Map<Integer, Double> weightMap = parts.stream().collect(Collectors.toMap(PartModel::getPid, partModel -> {
			if (partModel.getDuplicate() == null) {
				return partModel.getWeight();
			} else {
				return partModel.getDuplicate().getWeight();
			}
		}));
		//Список ID поставщиков
		Set<Integer> suppliersIds = suppliers.stream().map(SupplierModel::getSid).collect(Collectors.toSet());

		supplies = supplies.stream()
				.filter(it -> it.getShipDate() != null) //Убираем записи с неверной датой
				.filter(it -> weightMap.containsKey(it.getPid())) //Убираем записи с несуществующими деталями
				.filter(it -> suppliersIds.contains(it.getSid())) //Убираем записи с несуществующими поставщиками
				.collect(Collectors.toList());

		//Ищем среднюю цену каждой детали
		Map<Integer, Double> priceForParts = supplies.stream()
				.filter(it -> it.getPrice() != null && it.getPrice() > 0) //убираем неверные цены
				//Группируем по номеру детали, как значение берем среднюю цену
				.collect(Collectors.groupingBy(SupplyModel::getPid, Collectors.averagingDouble(SupplyModel::getPrice)));

		//Самое часто встречаемое количество детали
		Map<Integer, Integer> quantityForParts = supplies.stream()
				.filter(it -> it.getQuantity() != null && it.getQuantity() > 0) //Фильтруем ошибочные количества
				.collect(Collectors.groupingBy(SupplyModel::getPid)) //Группируем по номеру детали
				.entrySet().stream()
				.collect(Collectors.toMap(
						Map.Entry::getKey, //ID детали остается как ключ
						it -> it.getValue().stream()
								.map(SupplyModel::getQuantity)
								.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
								.entrySet().stream()
								.max(Map.Entry.comparingByValue())
								.orElse(new AbstractMap.SimpleEntry<>(null, null))
								.getKey()
				))
				.entrySet().stream()
				.filter(it -> it.getValue() != null)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		//Заменяем ошибочные цены и количества
		supplies = supplies.stream()
				.peek(it -> {
					if ((it.getPrice() == null || it.getPrice() <= 0) && priceForParts.containsKey(it.getPid())) {
						it.setPrice(priceForParts.get(it.getPid()));
					}
				})
				.filter(it -> it.getPrice() != null) //Если замена не найдена удаляем
				.peek(it -> {
					if ((it.getQuantity() == null || it.getQuantity() <= 0)
							&& quantityForParts.containsKey(it.getPid())) {
						it.setQuantity(quantityForParts.get(it.getPid()));
					}
				})
				.filter(it -> it.getQuantity() != null) //Если замена не найдена удаляем
				.collect(Collectors.toList());

		//Убирам строки с весом поставки больше 1500
		supplies = supplies.stream()
				.filter(it -> it.getQuantity() * weightMap.get(it.getPid()) <= 1500)
				.collect(Collectors.toList());
		System.out.println("Cleaned " + supplies.size() + " rows for " + SupplyModel.class.getName());
		return supplies;
	}
}
