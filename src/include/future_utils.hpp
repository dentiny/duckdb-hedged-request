#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/mutex.hpp"

#include <condition_variable>
#include <exception>
#include <functional>
#include <type_traits>

namespace duckdb {

template <typename T>
struct HedgedOutcomeToken {
	annotated_mutex mu;
	std::condition_variable cv DUCKDB_GUARDED_BY(mu);
	bool completed DUCKDB_GUARDED_BY(mu) = false;
	std::exception_ptr eptr DUCKDB_GUARDED_BY(mu);
	unique_ptr<T> value DUCKDB_GUARDED_BY(mu);
};

template <>
struct HedgedOutcomeToken<void> {
	annotated_mutex mu;
	std::condition_variable cv DUCKDB_GUARDED_BY(mu);
	bool completed DUCKDB_GUARDED_BY(mu) = false;
	std::exception_ptr eptr DUCKDB_GUARDED_BY(mu);
};

template <typename T>
T WaitForHedgedOutcome(shared_ptr<HedgedOutcomeToken<T>> token) {
	annotated_unique_lock<annotated_mutex> lock(token->mu);
	token->cv.wait(lock, [token]() DUCKDB_REQUIRES(token->mu) { return token->completed; });
	if (token->eptr != nullptr) {
		std::rethrow_exception(token->eptr);
	}
	if constexpr (!std::is_void_v<T>) {
		return std::move(*token->value);
	}
}

template <typename T>
void RunHedgedJob(std::function<T()> fn, shared_ptr<HedgedOutcomeToken<T>> token) {
	try {
		if constexpr (std::is_void_v<T>) {
			fn();
			annotated_unique_lock<annotated_mutex> lock(token->mu);
			if (token->completed) {
				return;
			}
			token->completed = true;
			token->cv.notify_all();
		} else {
			auto result = fn();
			annotated_unique_lock<annotated_mutex> lock(token->mu);
			if (token->completed) {
				return;
			}
			token->value = make_uniq<T>(std::move(result));
			token->completed = true;
			token->cv.notify_all();
		}
	} catch (...) {
		annotated_unique_lock<annotated_mutex> lock(token->mu);
		if (token->completed) {
			return;
		}
		token->eptr = std::current_exception();
		token->completed = true;
		token->cv.notify_all();
	}
}

inline void RunHedgedVoidJob(std::function<void()> fn, shared_ptr<HedgedOutcomeToken<void>> token) {
	RunHedgedJob(std::move(fn), std::move(token));
}

} // namespace duckdb
