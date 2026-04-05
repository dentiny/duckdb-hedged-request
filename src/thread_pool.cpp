#include "thread_pool.hpp"

#include "duckdb/common/assert.hpp"

namespace duckdb {

namespace {

size_t DefaultThreadCount() {
	auto n = std::thread::hardware_concurrency();
	return n == 0 ? 4 : static_cast<size_t>(n);
}

} // namespace

ThreadPool::ThreadPool() : ThreadPool(DefaultThreadCount()) {
}

ThreadPool::ThreadPool(size_t thread_num) : idle_num_(thread_num) {
	workers_.reserve(thread_num);
	for (size_t ii = 0; ii < thread_num; ii++) {
		workers_.emplace_back([this]() {
			for (;;) {
				Job cur_job;
				{
					unique_lock<mutex> lck(mutex_);
					new_job_cv_.wait(lck, [this]() DUCKDB_REQUIRES(mutex_) { return !jobs_.empty() || stopped_; });
					if (stopped_) {
						return;
					}
					cur_job = std::move(jobs_.front());
					jobs_.pop();
					D_ASSERT(idle_num_ > 0);
					idle_num_--;
				}

				cur_job();

				{
					unique_lock<mutex> lck(mutex_);
					idle_num_++;
					job_completion_cv_.notify_one();
				}
			}
		});
	}
}

void ThreadPool::Wait() {
	unique_lock<mutex> lck(mutex_);
	job_completion_cv_.wait(lck, [this]() DUCKDB_REQUIRES(mutex_) {
		if (stopped_) {
			return true;
		}
		if (idle_num_ == workers_.size() && jobs_.empty()) {
			return true;
		}
		return false;
	});
}

ThreadPool::~ThreadPool() noexcept {
	{
		const lock_guard<mutex> lck(mutex_);
		stopped_ = true;
		new_job_cv_.notify_all();
	}
	for (auto &cur_worker : workers_) {
		D_ASSERT(cur_worker.joinable());
		cur_worker.join();
	}
}

} // namespace duckdb
