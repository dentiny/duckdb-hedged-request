#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "thread_annotation.hpp"

#include <condition_variable>
#include <exception>
#include <functional>

namespace duckdb {

// Outcome of a hedged operation: first completion wins (success value or exception).
template <typename T>
struct HedgedOutcomeToken {
	mutex mu;
	std::condition_variable cv DUCKDB_GUARDED_BY(mu);
	bool completed DUCKDB_GUARDED_BY(mu) = false;
	bool has_exception DUCKDB_GUARDED_BY(mu) = false;
	std::exception_ptr eptr DUCKDB_GUARDED_BY(mu);
	unique_ptr<T> success_value DUCKDB_GUARDED_BY(mu);
};

template <>
struct HedgedOutcomeToken<void> {
	mutex mu;
	std::condition_variable cv DUCKDB_GUARDED_BY(mu);
	bool completed DUCKDB_GUARDED_BY(mu) = false;
	bool has_exception DUCKDB_GUARDED_BY(mu) = false;
	std::exception_ptr eptr DUCKDB_GUARDED_BY(mu);
};

template <typename T>
T WaitForHedgedOutcome(shared_ptr<HedgedOutcomeToken<T>> token) {
	unique_lock<mutex> lock(token->mu);
	token->cv.wait(lock, [&token]() { return token->completed; });
	if (token->has_exception) {
		std::rethrow_exception(token->eptr);
	}
	return std::move(*token->success_value);
}

inline void WaitForHedgedOutcome(shared_ptr<HedgedOutcomeToken<void>> token) {
	unique_lock<mutex> lock(token->mu);
	token->cv.wait(lock, [&token]() { return token->completed; });
	if (token->has_exception) {
		std::rethrow_exception(token->eptr);
	}
}

template <typename T>
void RunHedgedJob(std::function<T()> fn, shared_ptr<HedgedOutcomeToken<T>> token) {
	try {
		T r = fn();
		unique_lock<mutex> lock(token->mu);
		if (!token->completed) {
			token->success_value = make_uniq<T>(std::move(r));
			token->has_exception = false;
			token->completed = true;
		}
	} catch (...) {
		unique_lock<mutex> lock(token->mu);
		if (!token->completed) {
			token->eptr = std::current_exception();
			token->has_exception = true;
			token->completed = true;
		}
	}
	token->cv.notify_all();
}

inline void RunHedgedVoidJob(std::function<void()> fn, shared_ptr<HedgedOutcomeToken<void>> token) {
	try {
		fn();
		unique_lock<mutex> lock(token->mu);
		if (!token->completed) {
			token->has_exception = false;
			token->completed = true;
		}
	} catch (...) {
		unique_lock<mutex> lock(token->mu);
		if (!token->completed) {
			token->eptr = std::current_exception();
			token->has_exception = true;
			token->completed = true;
		}
	}
	token->cv.notify_all();
}

} // namespace duckdb
