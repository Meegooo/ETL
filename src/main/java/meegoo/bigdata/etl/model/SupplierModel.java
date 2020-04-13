package meegoo.bigdata.etl.model;

import meegoo.bigdata.etl.persistence.Column;
import meegoo.bigdata.etl.persistence.Key;
import meegoo.bigdata.etl.persistence.Table;

import java.util.Objects;

@Table(name = "S")
public class SupplierModel {

	@Column(name="SID")
	@Key()
	private Integer sid;

	@Column(name="SName")
	private String name;

	@Column(name="SCity")
	private String city;

	@Column(name="Address")
	private String address;

	@Column(name="Risk")
	private Integer risk;

	private SupplierModel duplicate = null;

	public Integer getSid() {
		return sid;
	}

	public String getName() {
		return name;
	}

	public String getCity() {
		return city;
	}

	public String getAddress() {
		return address;
	}

	public Integer getRisk() {
		return risk;
	}

	public void setSid(Integer sid) {
		this.sid = sid;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public void setRisk(Integer risk) {
		this.risk = risk;
	}

	public SupplierModel getDuplicate() { return duplicate; }

	public void setDuplicate(SupplierModel duplicate) {
		this.duplicate = duplicate;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SupplierModel that = (SupplierModel) o;
		return Objects.equals(name, that.name) &&
				Objects.equals(city, that.city) &&
				Objects.equals(address, that.address);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, city, address);
	}
}
