#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "thread_annotation.hpp"

#include <condition_variable>
#include <functional>
#include <future>

namespace duckdb {

// Token used to signal completion across multiple FutureWrappers.
struct Token {
	bool completed DUCKDB_GUARDED_BY(mu) = false;
	mutex mu;
	std::condition_variable cv DUCKDB_GUARDED_BY(mu);
};

// A wrapper around std::future, which shares a Token among multiple future wrappers.
// On completion, signals the Token via cv so WaitForAny can wake up.
template <typename T>
class FutureWrapper {
public:
	FutureWrapper(std::function<T()> functor, shared_ptr<Token> token_p) : token(std::move(token_p)) {
		auto tok = token;
		future = std::async(std::launch::async, [functor = std::move(functor), tok = std::move(tok)]() {
			try {
				auto result = functor();
				{
					const lock_guard<mutex> lock(tok->mu);
					tok->completed = true;
				}
				tok->cv.notify_all();
				return result;
			} catch (...) {
				{
					const lock_guard<mutex> lock(tok->mu);
					tok->completed = true;
					tok->cv.notify_all();
				}
				throw;
			}
		});
	}

	FutureWrapper(const FutureWrapper &) = delete;
	FutureWrapper &operator=(const FutureWrapper &) = delete;

	FutureWrapper(FutureWrapper &&other) noexcept : token(std::move(other.token)), future(std::move(other.future)) {
	}

	FutureWrapper &operator=(FutureWrapper &&other) noexcept {
		if (this != &other) {
			token = std::move(other.token);
			future = std::move(other.future);
		}
		return *this;
	}

	~FutureWrapper() {
		Wait();
	}

	bool IsReady() const {
		if (!future.valid()) {
			return false;
		}
		return future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
	}

	// Block until result is available, then return it. Rethrows if the task threw.
	T Get() {
		return future.get();
	}

	void Wait() {
		if (future.valid()) {
			future.wait();
		}
	}

private:
	shared_ptr<Token> token;
	std::future<T> future;
};

template <>
class FutureWrapper<void> {
public:
	FutureWrapper(std::function<void()> functor, shared_ptr<Token> token_p) : token(std::move(token_p)) {
		auto tok = token;
		future = std::async(std::launch::async, [functor = std::move(functor), tok = std::move(tok)]() {
			try {
				functor();
				{
					const lock_guard<mutex> lock(tok->mu);
					tok->completed = true;
				}
				tok->cv.notify_all();
			} catch (...) {
				{
					const lock_guard<mutex> lock(tok->mu);
					tok->completed = true;
					tok->cv.notify_all();
				}
				throw;
			}
		});
	}

	FutureWrapper(const FutureWrapper &) = delete;
	FutureWrapper &operator=(const FutureWrapper &) = delete;

	FutureWrapper(FutureWrapper &&other) noexcept : token(std::move(other.token)), future(std::move(other.future)) {
	}

	FutureWrapper &operator=(FutureWrapper &&other) noexcept {
		if (this != &other) {
			token = std::move(other.token);
			future = std::move(other.future);
		}
		return *this;
	}

	~FutureWrapper() {
		Wait();
	}

	bool IsReady() const {
		if (!future.valid()) {
			return false;
		}
		return future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready;
	}

	void Get() {
		future.get();
	}

	void Wait() {
		if (future.valid()) {
			future.wait();
		}
	}

private:
	shared_ptr<Token> token;
	std::future<void> future;
};

template <typename T>
struct WaitResult {
	vector<FutureWrapper<T>> pending_futures;
	T result;
};

// Wait until any of the futures completes.
// Returns a WaitResult containing the result of the first completed future and pending futures.
template <typename T>
WaitResult<T> WaitForAny(vector<FutureWrapper<T>> futs, shared_ptr<Token> token) {
	{
		unique_lock<mutex> lock(token->mu);
		token->cv.wait(lock, [&] { return token->completed; });
	}

	WaitResult<T> result;
	bool got_result = false;
	for (auto &fut : futs) {
		if (!got_result && fut.IsReady()) {
			result.result = fut.Get();
			got_result = true;
		} else {
			result.pending_futures.emplace_back(std::move(fut));
		}
	}
	return result;
}

} // namespace duckdb
