package ru.bpc.cm.cashmanagement.orm.items;

public class TripleObject<T, R, M> {

	private T first;
	private R second;
	private M third;

	public T getFirst() {
		return first;
	}

	public R getSecond() {
		return second;
	}

	public M getThird() {
		return third;
	}

	public TripleObject(T first, R second, M third) {
		super();
		this.first = first;
		this.second = second;
		this.third = third;
	}
}
