#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"

#include <condition_variable>
#include <functional>
#include <thread>

namespace duckdb {

// Used to signal completion across multiple FutureWrappers.
struct Token {
	bool completed = false;
	mutex mu;
	std::condition_variable cv;
};

// A wrapper around a future, which could be used share tokens among multiple future wrappers.
template <typename T>
class FutureWrapper {
public:
	FutureWrapper() = default;

	FutureWrapper(const FutureWrapper &other) noexcept = disable;
	FutureWrapper &operator=(const FutureWrapper &other) noexcept = disable;

	FutureWrapper(FutureWrapper other) noexcept
	    : token(std::move(other.token)), state(std::move(other.state)), worker(std::move(other.worker)) {
	}
	FutureWrapper &operator=(FutureWrapper &&other) noexcept {
		if (this != &other) {
			token = std::move(other.token);
			state = std::move(other.state);
			worker = std::move(other.worker);
		}
		return *this;
	}

	FutureWrapper(const FutureWrapper &) = delete;
	FutureWrapper &operator=(const FutureWrapper &) = delete;

	~FutureWrapper() {
		if (worker.joinable()) {
			worker.join();
		}
	}

	bool IsReady() const {
		if (!state) {
			return false;
		}
		lock_guard<mutex> lock(state->mu);
		return state->ready;
	}

	// Block until result is available, then return it. Rethrows if the task threw.
	T Get() {
		if (worker.joinable()) {
			worker.join();
		}
		lock_guard<mutex> lock(state->mu);
		if (state->exception) {
			std::rethrow_exception(state->exception);
		}
		return std::move(state->result);
	}

	// Wait (blocking) for completion without consuming the result.
	void Wait() {
		if (worker.joinable()) {
			worker.join();
		}
	}

private:
	struct State {
		bool ready = false;
		T result;
		std::exception_ptr exception;
		mutex mu;
	};

	shared_ptr<Token> token;
	shared_ptr<State> state;
	std::thread worker;

	template <typename U>
	friend FutureWrapper<U> CreateFuture(std::function<U()> functor, shared_ptr<Token> token);
};

// void specialization
template <>
class FutureWrapper<void> {
public:
	FutureWrapper() = default;

	FutureWrapper(const FutureWrapper &other) noexcept = disable;
	FutureWrapper &operator=(const FutureWrapper &other) noexcept = disable;

	FutureWrapper(FutureWrapper &&other) noexcept
	    : token(std::move(other.token)), state(std::move(other.state)), worker(std::move(other.worker)) {
	}

	FutureWrapper &operator=(FutureWrapper &&other) noexcept {
		if (this != &other) {
			token = std::move(other.token);
			state = std::move(other.state);
			worker = std::move(other.worker);
		}
		return *this;
	}

	FutureWrapper(const FutureWrapper &) = delete;
	FutureWrapper &operator=(const FutureWrapper &) = delete;

	~FutureWrapper() {
		if (worker.joinable()) {
			worker.join();
		}
	}

	bool IsReady() const {
		if (!state) {
			return false;
		}
		lock_guard<mutex> lock(state->mu);
		return state->ready;
	}

	void Get() {
		if (worker.joinable()) {
			worker.join();
		}
		lock_guard<mutex> lock(state->mu);
		if (state->exception) {
			std::rethrow_exception(state->exception);
		}
	}

	void Wait() {
		if (worker.joinable()) {
			worker.join();
		}
	}

private:
	struct State {
		bool ready = false;
		std::exception_ptr exception;
		mutex mu;
	};

	shared_ptr<Token> token;
	shared_ptr<State> state;
	std::thread worker;

	friend FutureWrapper<void> CreateFuture(std::function<void()> functor, shared_ptr<Token> token);
};

// Create a FutureWrapper that runs functor in a new thread.
// On completion (success or exception), signals the Token via cv.
template <typename T>
FutureWrapper<T> CreateFuture(std::function<T()> functor, shared_ptr<Token> token) {
	FutureWrapper<T> fw;
	fw.token = token;
	fw.state = make_shared_ptr<typename FutureWrapper<T>::State>();

	auto state = fw.state;
	auto tok = fw.token;

	fw.worker = std::thread([functor = std::move(functor), state = std::move(state), tok = std::move(tok)]() {
		try {
			auto result = functor();
			{
				lock_guard<mutex> lock(state->mu);
				state->result = std::move(result);
				state->ready = true;
			}
		} catch (...) {
			{
				lock_guard<mutex> lock(state->mu);
				state->exception = std::current_exception();
				state->ready = true;
			}
		}
		// Signal the token
		{
			lock_guard<mutex> lock(tok->mu);
			tok->completed = true;
			tok->cv.notify_all();
		}
	});

	return fw;
}

template <>
inline FutureWrapper<void> CreateFuture(std::function<void()> functor, shared_ptr<Token> token) {
	FutureWrapper<void> fw;
	fw.token = token;
	fw.state = make_shared_ptr<FutureWrapper<void>::State>();

	auto state = fw.state;
	auto tok = fw.token;

	fw.worker = std::thread([functor = std::move(functor), state = std::move(state), tok = std::move(tok)]() {
		try {
			functor();
			{
				lock_guard<mutex> lock(state->mu);
				state->ready = true;
			}
		} catch (...) {
			{
				lock_guard<mutex> lock(state->mu);
				state->exception = std::current_exception();
				state->ready = true;
			}
		}
		// Signal the token
		{
			lock_guard<mutex> lock(tok->mu);
			tok->completed = true;
		}
		tok->cv.notify_all();
	});

	return fw;
}

// Wait until any of the futures completes.
// Returns the futures that are NOT yet ready.
template <typename T>
vector<FutureWrapper<T>> WaitForAny(vector<FutureWrapper<T>> &futs, shared_ptr<Token> token) {
	// Wait for any completion via the token's cv
	{
		unique_lock<mutex> lock(token->mu);
		token->cv.wait(lock, [&] { return token->completed; });
	}

	// Partition into ready and unready
	vector<FutureWrapper<T>> unready;
	vector<FutureWrapper<T>> remaining;
	for (auto &fut : futs) {
		if (!fut.IsReady()) {
			unready.push_back(std::move(fut));
		} else {
			remaining.push_back(std::move(fut));
		}
	}

	// Put back only the ready ones into futs
	futs = std::move(remaining);
	return unready;
}

} // namespace duckdb

