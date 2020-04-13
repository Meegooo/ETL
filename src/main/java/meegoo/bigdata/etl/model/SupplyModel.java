package meegoo.bigdata.etl.model;

import meegoo.bigdata.etl.persistence.Column;
import meegoo.bigdata.etl.persistence.Key;
import meegoo.bigdata.etl.persistence.Table;

import java.time.LocalDateTime;

@Table(name = "SP")
public class SupplyModel {

	@Column(name = "SPID")
	@Key
	private Integer spid;

	@Column(name = "SID")
	private Integer sid;

	@Column(name = "PID")
	private Integer pid;

	@Column(name = "Quantity")
	private Integer quantity;

	@Column(name = "Price")
	private Double price;

	@Column(name = "ShipDate")
	private LocalDateTime shipDate;

	public Integer getSpid() {
		return spid;
	}

	public Integer getSid() {
		return sid;
	}

	public Integer getPid() {
		return pid;
	}

	public Integer getQuantity() {
		return quantity;
	}

	public Double getPrice() {
		return price;
	}

	public LocalDateTime getShipDate() {
		return shipDate;
	}

	public void setSpid(Integer spid) {
		this.spid = spid;
	}

	public void setSid(Integer sid) {
		this.sid = sid;
	}

	public void setPid(Integer pid) {
		this.pid = pid;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public void setShipDate(LocalDateTime shipDate) {
		this.shipDate = shipDate;
	}
}
