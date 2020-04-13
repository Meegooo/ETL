package meegoo.bigdata.etl.model;

import meegoo.bigdata.etl.persistence.Column;
import meegoo.bigdata.etl.persistence.Key;
import meegoo.bigdata.etl.persistence.Table;

import java.util.Objects;

@Table(name = "P")
public class PartModel {

	@Column(name = "PID")
	@Key
	private Integer pid;

	@Column(name = "PName")
	private String name;

	@Column(name = "PCity")
	private String city;

	@Column(name = "Color")
	private String color;

	@Column(name = "Weight")
	private Double weight;

	private PartModel duplicate = null;

	public Integer getPid() {
		return pid;
	}

	public String getName() {
		return name;
	}

	public String getCity() {
		return city;
	}

	public String getColor() {
		return color;
	}

	public Double getWeight() {
		return weight;
	}

	public void setPid(Integer pid) {
		this.pid = pid;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public void setWeight(Double weight) {
		this.weight = weight;
	}

	public PartModel getDuplicate() {
		return duplicate;
	}

	public void setDuplicate(PartModel duplicate) {
		this.duplicate = duplicate;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PartModel partModel = (PartModel) o;
		return Objects.equals(name, partModel.name) &&
				Objects.equals(city, partModel.city) &&
				Objects.equals(color, partModel.color);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, city, color);
	}
}
