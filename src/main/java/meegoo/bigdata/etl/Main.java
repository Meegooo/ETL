package meegoo.bigdata.etl;

import meegoo.bigdata.etl.database.Importer;
import meegoo.bigdata.etl.etl.Cleaner;
import meegoo.bigdata.etl.etl.PrimaryKeyUpdater;
import meegoo.bigdata.etl.model.PartModel;
import meegoo.bigdata.etl.model.SupplierModel;
import meegoo.bigdata.etl.model.SupplyModel;
import meegoo.bigdata.etl.parser.AccdbParser;
import meegoo.bigdata.etl.parser.ExcelParser;
import meegoo.bigdata.etl.database.ConnectionManager;
import meegoo.bigdata.etl.database.Exporter;
import meegoo.bigdata.etl.database.DatabaseUtil;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
	public static void main(String[] args) throws IOException, SQLException {
		runAccdb();
		runXlsx();
	}

	public static void runAccdb() throws SQLException {
		File file = new File("Branch_1.accdb");
		List<SupplierModel> s = AccdbParser.parse(file, SupplierModel.class);
		List<PartModel> p = AccdbParser.parse(file, PartModel.class);
		List<SupplyModel> sp = AccdbParser.parse(file, SupplyModel.class);

		process(s, p, sp);
	}
	public static void runXlsx() throws IOException {
		File file = new File("Branch_2.xlsx");
		List<SupplierModel> s = ExcelParser.parse(file, SupplierModel.class);
		List<PartModel> p = ExcelParser.parse(file, PartModel.class);
		List<SupplyModel> sp = ExcelParser.parse(file, SupplyModel.class);

		process(s, p, sp);
	}

	private static void process(List<SupplierModel> dirtyS, List<PartModel> dirtyP, List<SupplyModel> dirtySp) {
		//Читаем данные из БД
		Set<SupplierModel> suppliersPostgres =
				Importer.importFromPostgres(SupplierModel.class, List.of("SID", "SName", "Address", "SCity"));
		Set<PartModel> partsPostgres =
				Importer.importFromPostgres(PartModel.class, List.of("PID", "PName", "PCity", "Color", "Weight"));

		//очищаем данные
		List<SupplierModel> cleanedS = Cleaner.cleanSupplierModel(dirtyS, suppliersPostgres);
		List<PartModel> cleanedP = Cleaner.cleanPartModel(dirtyP, partsPostgres);
		List<SupplyModel> cleanedSp = Cleaner.cleanSupplyModel(dirtySp, cleanedP, cleanedS);

		PrimaryKeyUpdater.fixPrimaryKey(cleanedS, cleanedP, cleanedSp);

		//Убираем дубликаты
		cleanedS = cleanedS.stream().filter(it -> it.getDuplicate() == null).collect(Collectors.toList());
		cleanedP = cleanedP.stream().filter(it -> it.getDuplicate() == null).collect(Collectors.toList());

		try {
			//Выключаем автоматическую запись, что-бы все три таблицы записались как одна транзакция.
			ConnectionManager.getConnection().setAutoCommit(false);
			//Записываем данные в 3 таблицы
			Exporter.exportToPostgres(cleanedS, SupplierModel.class);
			Exporter.exportToPostgres(cleanedP, PartModel.class);
			Exporter.exportToPostgres(cleanedSp, SupplyModel.class);
			//Обновляем значения первичных ключей в БД.
			DatabaseUtil.updateNextKeyValue(SupplierModel.class);
			DatabaseUtil.updateNextKeyValue(PartModel.class);
			DatabaseUtil.updateNextKeyValue(SupplyModel.class);
			//Отправляем транзакцию
			ConnectionManager.getConnection().commit();
			ConnectionManager.getConnection().setAutoCommit(true);
			System.out.println("Commited changes");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
