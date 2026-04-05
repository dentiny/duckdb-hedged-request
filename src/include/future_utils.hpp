#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

#include <condition_variable>
#include <exception>
#include <functional>

namespace duckdb {

template <typename T>
struct HedgedOutcomeToken {
	concurrency::mutex mu;
	std::condition_variable cv DUCKDB_GUARDED_BY(mu);
	bool completed DUCKDB_GUARDED_BY(mu) = false;
	std::exception_ptr eptr DUCKDB_GUARDED_BY(mu);
	unique_ptr<T> value DUCKDB_GUARDED_BY(mu);
};

template <>
struct HedgedOutcomeToken<void> {
	concurrency::mutex mu;
	std::condition_variable cv DUCKDB_GUARDED_BY(mu);
	bool completed DUCKDB_GUARDED_BY(mu) = false;
	std::exception_ptr eptr DUCKDB_GUARDED_BY(mu);
};

template <typename T>
T WaitForHedgedOutcome(shared_ptr<HedgedOutcomeToken<T>> token) {
	concurrency::unique_lock<concurrency::mutex> lock(token->mu);
	token->cv.wait(lock, [token]() DUCKDB_REQUIRES(token->mu) { return token->completed; });
	if (token->eptr != nullptr) {
		std::rethrow_exception(token->eptr);
	}
	return std::move(*token->value);
}

inline void WaitForHedgedOutcome(shared_ptr<HedgedOutcomeToken<void>> token) {
	concurrency::unique_lock<concurrency::mutex> lock(token->mu);
	token->cv.wait(lock, [token]() DUCKDB_REQUIRES(token->mu) { return token->completed; });
	if (token->eptr != nullptr) {
		std::rethrow_exception(token->eptr);
	}
}

template <typename T>
void RunHedgedJob(std::function<T()> fn, shared_ptr<HedgedOutcomeToken<T>> token) {
	try {
		T r = fn();
		{
			concurrency::unique_lock<concurrency::mutex> lock(token->mu);
			ALWAYS_ASSERT(!token->completed);
			token->value = make_uniq<T>(std::move(r));
			token->completed = true;
			token->cv.notify_all();
		}
	} catch (...) {
		{
			concurrency::unique_lock<concurrency::mutex> lock(token->mu);
			ALWAYS_ASSERT(!token->completed);
			token->eptr = std::current_exception();
			token->completed = true;
			token->cv.notify_all();
		}
	}
}

inline void RunHedgedVoidJob(std::function<void()> fn, shared_ptr<HedgedOutcomeToken<void>> token) {
	try {
		fn();
		{
			concurrency::unique_lock<concurrency::mutex> lock(token->mu);
			if (!token->completed) {
				token->completed = true;
			}
			token->cv.notify_all();
		}
	} catch (...) {
		{
			concurrency::unique_lock<concurrency::mutex> lock(token->mu);
			ALWAYS_ASSERT(!token->completed);
			token->eptr = std::current_exception();
			token->completed = true;
			token->cv.notify_all();
		}
	}
}

} // namespace duckdb
