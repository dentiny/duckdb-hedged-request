#include "catch/catch.hpp"

#include "future_utils.hpp"
#include "thread_pool.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb; // NOLINT

TEST_CASE("RunHedgedJob success", "[future_utils]") {
	auto token = make_shared_ptr<HedgedOutcomeToken<int>>();
	RunHedgedJob(std::function<int()>([]() { return 99; }), token);
	REQUIRE(WaitForHedgedOutcome(token) == 99);
}

TEST_CASE("RunHedgedVoidJob", "[future_utils]") {
	auto token = make_shared_ptr<HedgedOutcomeToken<void>>();
	std::atomic<bool> executed(false);
	RunHedgedVoidJob(std::function<void()>([&executed]() { executed.store(true); }), token);
	WaitForHedgedOutcome(token);
	REQUIRE(executed.load());
}

TEST_CASE("RunHedgedJob exception propagation", "[future_utils]") {
	auto token = make_shared_ptr<HedgedOutcomeToken<int>>();
	RunHedgedJob(std::function<int()>([]() -> int { throw std::runtime_error("test error"); }), token);
	REQUIRE_THROWS_AS(WaitForHedgedOutcome(token), std::runtime_error);
}

TEST_CASE("RunHedgedVoidJob exception propagation", "[future_utils]") {
	auto token = make_shared_ptr<HedgedOutcomeToken<void>>();
	RunHedgedVoidJob(std::function<void()>([]() { throw std::runtime_error("test error"); }), token);
	REQUIRE_THROWS_AS(WaitForHedgedOutcome(token), std::runtime_error);
}

TEST_CASE("HedgedOutcomeToken via thread pool", "[future_utils]") {
	auto token = make_shared_ptr<HedgedOutcomeToken<int>>();
	ThreadPool pool;
	pool.Push([token]() { RunHedgedJob(std::function<int()>([]() { return 42; }), token); });
	REQUIRE(WaitForHedgedOutcome(token) == 42);
}

TEST_CASE("HedgedOutcomeToken first completion wins", "[future_utils]") {
	auto token = make_shared_ptr<HedgedOutcomeToken<int>>();
	ThreadPool pool;
	pool.Push([token]() { RunHedgedJob(std::function<int()>([]() { return 1; }), token); });
	pool.Push([token]() {
		RunHedgedJob(std::function<int()>([]() {
			             std::this_thread::sleep_for(std::chrono::milliseconds(500));
			             return 2;
		             }),
		             token);
	});
	int v = WaitForHedgedOutcome(token);
	REQUIRE((v == 1 || v == 2));
}

TEST_CASE("Hedged value completion ignores later results and failures", "[future_utils]") {
	auto token = make_shared_ptr<HedgedOutcomeToken<int>>();
	RunHedgedJob(std::function<int()>([]() { return 42; }), token);
	RunHedgedJob(std::function<int()>([]() { return 99; }), token);
	RunHedgedJob(std::function<int()>([]() -> int { throw std::runtime_error("late failure"); }), token);
	REQUIRE(WaitForHedgedOutcome(token) == 42);
}

TEST_CASE("Hedged void completion ignores later failures", "[future_utils]") {
	auto token = make_shared_ptr<HedgedOutcomeToken<void>>();
	RunHedgedVoidJob(std::function<void()>([]() {}), token);
	RunHedgedVoidJob(std::function<void()>([]() { throw std::runtime_error("late failure"); }), token);
	REQUIRE_NOTHROW(WaitForHedgedOutcome(token));
}

TEST_CASE("Hedged first failure survives later success", "[future_utils]") {
	SECTION("value result") {
		auto token = make_shared_ptr<HedgedOutcomeToken<int>>();
		RunHedgedJob(std::function<int()>([]() -> int { throw std::runtime_error("first failure"); }), token);
		RunHedgedJob(std::function<int()>([]() { return 42; }), token);
		REQUIRE_THROWS_WITH(WaitForHedgedOutcome(token), "first failure");
	}
	SECTION("void result") {
		auto token = make_shared_ptr<HedgedOutcomeToken<void>>();
		RunHedgedVoidJob(std::function<void()>([]() { throw std::runtime_error("first failure"); }), token);
		RunHedgedVoidJob(std::function<void()>([]() {}), token);
		REQUIRE_THROWS_WITH(WaitForHedgedOutcome(token), "first failure");
	}
}
