#pragma once

#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector.hpp"
#include "mutex.hpp"
#include "thread_annotation.hpp"

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

namespace duckdb {

class ThreadPool {
public:
	ThreadPool();
	explicit ThreadPool(size_t thread_num);

	ThreadPool(const ThreadPool &) = delete;
	ThreadPool &operator=(const ThreadPool &) = delete;

	~ThreadPool() noexcept;

	// @return future for synchronization (result channel is optional; hedged code uses Token for outcomes).
	template <typename Fn, typename... Args>
	auto Push(Fn &&fn, Args &&...args) -> std::future<typename std::result_of<Fn(Args...)>::type>;

	// Block until the threadpool is idle (all workers idle and job queue empty).
	void Wait();

private:
	using Job = std::function<void(void)>;

	size_t idle_num_ DUCKDB_GUARDED_BY(mutex_) = 0;
	bool stopped_ DUCKDB_GUARDED_BY(mutex_) = false;
	concurrency::mutex mutex_;
	std::condition_variable new_job_cv_ DUCKDB_GUARDED_BY(mutex_);
	std::condition_variable job_completion_cv_ DUCKDB_GUARDED_BY(mutex_);
	queue<Job> jobs_ DUCKDB_GUARDED_BY(mutex_);
	vector<std::thread> workers_ DUCKDB_GUARDED_BY(mutex_);
};

template <typename Fn, typename... Args>
auto ThreadPool::Push(Fn &&fn, Args &&...args) -> std::future<typename std::result_of<Fn(Args...)>::type> {
	using Ret = typename std::result_of<Fn(Args...)>::type;

	auto job =
	    std::make_shared<std::packaged_task<Ret()>>(std::bind(std::forward<Fn>(fn), std::forward<Args>(args)...));
	std::future<Ret> result = job->get_future();
	{
		const concurrency::lock_guard<concurrency::mutex> lck(mutex_);
		jobs_.emplace([job = std::move(job)]() mutable { (*job)(); });
		new_job_cv_.notify_one();
	}
	return result;
}

} // namespace duckdb
