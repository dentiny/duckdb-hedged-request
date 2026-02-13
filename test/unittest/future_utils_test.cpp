#include "catch/catch.hpp"

#include "future_utils.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb; // NOLINT


TEST_CASE("FutureWrapper move semantics", "[future_utils]") {
	auto token = make_shared_ptr<Token>();
	FutureWrapper<int> fw1(std::function<int()>([]() { return 99; }), token);
	FutureWrapper<int> fw2(std::move(fw1));
	REQUIRE(fw2.Get() == 99);
}

TEST_CASE("FutureWrapper<void> move semantics", "[future_utils]") {
	auto token = make_shared_ptr<Token>();
	std::atomic<bool> executed(false);
	FutureWrapper<void> fw1(std::function<void()>([&executed]() { executed.store(true); }), token);

	FutureWrapper<void> fw2(std::move(fw1));
	fw2.Get();
	REQUIRE(executed.load());
}

TEST_CASE("FutureWrapper<int> basic get", "[future_utils]") {
	auto token = make_shared_ptr<Token>();
	FutureWrapper<int> fw(std::function<int()>([]() { return 42; }), token);
	REQUIRE(fw.Get() == 42);
}

TEST_CASE("FutureWrapper<void> basic get", "[future_utils]") {
	auto token = make_shared_ptr<Token>();
	std::atomic<bool> executed(false);
	FutureWrapper<void> fw(std::function<void()>([&executed]() { executed.store(true); }), token);
	fw.Get();
	REQUIRE(executed.load());
}

TEST_CASE("FutureWrapper<int> exception propagation", "[future_utils]") {
	auto token = make_shared_ptr<Token>();
	FutureWrapper<int> fw(std::function<int()>([]() -> int { throw std::runtime_error("test error"); }), token);
	REQUIRE_THROWS_AS(fw.Get(), std::runtime_error);
}

TEST_CASE("FutureWrapper<void> exception propagation", "[future_utils]") {
	auto token = make_shared_ptr<Token>();
	FutureWrapper<void> fw(std::function<void()>([]() { throw std::runtime_error("test error"); }), token);
	REQUIRE_THROWS_AS(fw.Get(), std::runtime_error);
}

TEST_CASE("FutureWrapper<int> IsReady", "[future_utils]") {
	auto token = make_shared_ptr<Token>();
	FutureWrapper<int> fw(std::function<int()>([]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
		return 10;
	}), token);
	fw.Wait();
	REQUIRE(fw.IsReady());
}

TEST_CASE("WaitForAny", "[future_utils]") {
	auto token = make_shared_ptr<Token>();

	vector<FutureWrapper<int>> futs;
	futs.emplace_back(std::function<int()>([]() { return 1; }), token);
	futs.emplace_back(std::function<int()>([]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
		return 2;
	}), token);

	auto wait_result = WaitForAny(std::move(futs), token);
	REQUIRE(wait_result.pending_futures.size() == 1);
	REQUIRE((wait_result.result == 1 || wait_result.result == 2));
}
