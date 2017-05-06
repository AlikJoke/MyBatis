package ru.bpc.cm.orm.items;

/**
 * Simple DTO for results of mapping ORM.
 * 
 * @author Alimurad A. Ramazanov
 * @since 06.05.2017
 * @version 1.0.0
 *
 * @param <T>
 * @param <R>
 * @param <M>
 * @param <N>
 * @param <V>
 * @param <W>
 * @param <A>
 * @param <B>
 * @param <C>
 * @param <D>
 */
public class MultiObject<T, R, M, N, V, W, A, B, C, D> {

	private T first;
	private R second;
	private M third;
	private N fourth;
	private V fifth;
	private W sixth;
	private A seventh;
	private B eighth;
	private C ninth;
	private D tenth;

	public A getSeventh() {
		return seventh;
	}

	public void setSeventh(A seventh) {
		this.seventh = seventh;
	}

	public B getEighth() {
		return eighth;
	}

	public void setEighth(B eighth) {
		this.eighth = eighth;
	}

	public C getNinth() {
		return ninth;
	}

	public void setNinth(C ninth) {
		this.ninth = ninth;
	}

	public D getTenth() {
		return tenth;
	}

	public void setTenth(D tenth) {
		this.tenth = tenth;
	}

	public void setFirst(T first) {
		this.first = first;
	}

	public void setSecond(R second) {
		this.second = second;
	}

	public void setThird(M third) {
		this.third = third;
	}

	public W getSixth() {
		return sixth;
	}

	public void setSixth(W sixth) {
		this.sixth = sixth;
	}

	public N getFourth() {
		return fourth;
	}

	public void setFourth(N fourth) {
		this.fourth = fourth;
	}

	public V getFifth() {
		return fifth;
	}

	public void setFifth(V fifth) {
		this.fifth = fifth;
	}

	public T getFirst() {
		return first;
	}

	public R getSecond() {
		return second;
	}

	public M getThird() {
		return third;
	}

	public MultiObject() {
		super();
	}
}
