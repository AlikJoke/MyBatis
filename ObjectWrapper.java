package ru.bpc.cm.cashmanagement.orm.items;

public class ObjectWrapper<T> {

	private T object;

	public T getObject() {
		return object;
	}

	public void setObject(T object) {
		this.object = object;
	}

	public ObjectWrapper() {
	}

	public ObjectWrapper(T object) {
		super();
		this.object = object;
	}
}
