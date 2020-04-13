package meegoo.bigdata.etl.etl;

import meegoo.bigdata.etl.database.DatabaseUtil;
import meegoo.bigdata.etl.model.PartModel;
import meegoo.bigdata.etl.model.SupplierModel;
import meegoo.bigdata.etl.model.SupplyModel;

import java.util.*;
import java.util.stream.Collectors;

public class PrimaryKeyUpdater {

	public static <T> void fixPrimaryKey(List<SupplierModel> cleanedS,
	                                     List<PartModel> cleanedP, List<SupplyModel> cleanedSp) {
		//Получаем следующие значения первичных ключей для всех таблиц
		int supplierKey = DatabaseUtil.getNextKeyValue(SupplierModel.class);
		int partKey = DatabaseUtil.getNextKeyValue(PartModel.class);
		int supplyKey = DatabaseUtil.getNextKeyValue(SupplyModel.class);

		//Создаем отношение PID->PartModel и SID->SupplierModel
		Map<Integer, PartModel> partMap = cleanedP.stream()
				.collect(Collectors.toMap(PartModel::getPid, it -> it));
		Map<Integer, SupplierModel> supplierMap = cleanedS.stream()
				.collect(Collectors.toMap(SupplierModel::getSid, it -> it));

		//Обновляем ключ для записей таблицы S
		for (SupplierModel entry : cleanedS) {
			entry.setSid(entry.getSid() + supplierKey);
		}

		//Обновляем ключ для записей таблицы P
		for (PartModel entry : cleanedP) {
			entry.setPid(entry.getPid() + partKey);
		}

		//Обновляем ключи для записей таблицы SP
		for (SupplyModel entry : cleanedSp) {
			Integer pid = entry.getPid();
			Integer sid = entry.getSid();
			//Проверяем, принадлежит ли прочитанный PID дубликату
			PartModel partDuplicate = partMap.get(pid).getDuplicate();
			//Проверяем, принадлежит ли прочитанный SID дубликату
			SupplierModel supplierDuplicate = supplierMap.get(sid).getDuplicate();
			//Если найден дубликат, то используем PID/SID из дубликата
			//Иначе просто прибавляем следующие значения первичных ключей для таблиц P и S
			if (partDuplicate != null) {
				entry.setPid(partDuplicate.getPid());
			} else {
				entry.setPid(entry.getPid() + partKey);
			}
			if (supplierDuplicate != null) {
				entry.setSid(supplierDuplicate.getSid());
			} else {
				entry.setSid(entry.getSid() + supplierKey);
			}
			//Обновляем первичный ключ для записей таблицы SP
			entry.setSpid(entry.getSpid() + supplyKey);
		}
		System.out.println("Updated primary keys for cleaned data");
	}
}
