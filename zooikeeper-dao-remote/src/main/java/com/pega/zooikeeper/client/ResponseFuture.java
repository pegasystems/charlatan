package com.pega.zooikeeper.client;

import java.util.concurrent.*;

/**
 * Simple Future implementation
 *
 * @param <T>
 */
public class ResponseFuture<T> implements Future<T> {
	private final CountDownLatch latch = new CountDownLatch(1);
	private T response;
	private Throwable failure;

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return latch.getCount() == 0;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		latch.await();
		if (failure != null)
			throw new ExecutionException(failure);
		return response;
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
		if (latch.await(timeout, unit)) {
			if (failure != null)
				throw new ExecutionException(failure);
			return response;
		} else {
			throw new TimeoutException();
		}
	}

	void setResponse(T result) {
		response = result;
		latch.countDown();
	}


	void setFailure(Throwable e) {
		failure = e;
		latch.countDown();
	}
}