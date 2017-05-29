package ru.bpc.cm.orm.common;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ibatis.session.SqlSession;

public class CloseableItem {

	private SqlSession session;
	private boolean isBatch;
	private AtomicInteger opened = new AtomicInteger(0);
	private AtomicInteger counter = new AtomicInteger(0);

	public CloseableItem(SqlSession session, boolean isBatch) {
		this.session = session;
		this.isBatch = isBatch;
	}
	
	public CloseableItem(SqlSession session) {
		this.session = session;
		this.isBatch = false;
	}

	public SqlSession getSession() {
		return this.session;
	}

	public boolean isBatch() {
		return this.isBatch;
	}
	
	public void increment() {
		counter.set(counter.addAndGet(1));
		opened.set(opened.addAndGet(1));
	}

	public void decrement() {
		opened.set(opened.addAndGet(-1));
	}

	public boolean isUseless() {
		return opened.get() == 0 && this.counter.get() > 10;
	}

	public int getCounter() {
		return counter.get();
	}

	@Override
	public int hashCode() {
		return session.hashCode();
	}

	public int getOpened() {
		return opened.get();
	}

	@Override
	public boolean equals(Object obj) {
		CloseableItem item = (CloseableItem) obj;
		if (item.getSession() == this.session)
			return true;

		if (item.getSession().hashCode() == this.session.hashCode())
			return true;

		return false;
	}
}
