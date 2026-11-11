#include "duckdb/execution/operator/aggregate/physical_demand_scout_topk.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

namespace duckdb {

namespace {

static bool ProfileScout() {
	const auto enabled = std::getenv("DUCKDB_DEMAND_SCOUT_PROFILE");
	return enabled && enabled[0] == '1';
}

static double ScoutElapsedMs(std::chrono::steady_clock::time_point start) {
	return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

struct Entry {
	uint64_t score;
	int64_t value;
};

// Fixed K<=10, contiguous group state: no per-group vector/heap allocation.
class PackedTopK {
public:
	PackedTopK() = default;
	PackedTopK(idx_t groups, idx_t k_p) : k(k_p), slots(groups * k_p), counts(groups), minima(groups) {
	}
	void Add(idx_t group, Entry entry) {
		const auto base = group * k;
		auto &count = counts[group];
		auto &minimum = minima[group];
		if (count < k) {
			slots[base + count] = entry;
			if (count == 0 || entry.score < slots[base + minimum].score) {
				minimum = count;
			}
			++count;
			return;
		}
		if (entry.score <= slots[base + minimum].score) {
			return;
		}
		slots[base + minimum] = entry;
		minimum = 0;
		for (uint8_t index = 1; index < count; ++index) {
			if (slots[base + index].score < slots[base + minimum].score) {
				minimum = index;
			}
		}
	}
	uint8_t Count(idx_t group) const {
		return counts[group];
	}
	const Entry &At(idx_t group, idx_t index) const {
		return slots[group * k + index];
	}
	std::vector<Entry> Entries(idx_t group) const {
		const auto begin = slots.begin() + group * k;
		return std::vector<Entry>(begin, begin + counts[group]);
	}

private:
	idx_t k = 0;
	std::vector<Entry> slots;
	std::vector<uint8_t> counts, minima;
};

struct KeyTuple {
	idx_t group;
	Entry entry;
	bool valid;
};

class KeyQueue {
public:
	void Push(std::vector<KeyTuple> batch) {
		std::unique_lock<std::mutex> guard(lock);
		can_push.wait(guard, [&] { return aborted || batches.size() < 4; });
		if (aborted) {
			throw InterruptException();
		}
		batches.push_back(std::move(batch));
		can_pop.notify_one();
	}
	bool Pop(std::vector<KeyTuple> &batch) {
		std::unique_lock<std::mutex> guard(lock);
		can_pop.wait(guard, [&] { return aborted || closed || !batches.empty(); });
		if (aborted || batches.empty()) {
			return false;
		}
		batch = std::move(batches.front());
		batches.pop_front();
		can_push.notify_one();
		return true;
	}
	void Close() {
		std::lock_guard<std::mutex> guard(lock);
		closed = true;
		can_pop.notify_all();
	}
	void Abort() {
		std::lock_guard<std::mutex> guard(lock);
		aborted = true;
		can_pop.notify_all();
		can_push.notify_all();
	}

private:
	std::mutex lock;
	std::condition_variable can_push, can_pop;
	std::deque<std::vector<KeyTuple>> batches;
	bool closed = false, aborted = false;
};

static idx_t SampleGroups(ClientContext &context, TableCatalogEntry &table, column_t column_index, int32_t group_min,
                          idx_t group_domain) {
	auto &transaction = DuckTransaction::Get(context, table.ParentCatalog());
	auto row_groups = table.GetStorage().GetRowGroupCollection();
	std::vector<uint32_t> frequencies(group_domain);
	const auto group_count = row_groups->GetRowGroupCount();
	if (!group_count) {
		return 0;
	}
	for (const auto row_group_index : {idx_t(0), group_count / 2}) {
		auto row_group = row_groups->GetRowGroup(NumericCast<int64_t>(row_group_index));
		if (!row_group) {
			continue;
		}
		auto &column = row_group->GetColumnForDemand(column_index);
		const auto limit = MinValue<idx_t>(row_group->count, 65536);
		for (idx_t offset = 0; offset < limit; offset += STANDARD_VECTOR_SIZE) {
			const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, limit - offset);
			ColumnScanState state(nullptr);
			state.Initialize(context, LogicalType::INTEGER, StorageIndex(column_index), nullptr);
			column.InitializeScanWithOffset(state, offset);
			Vector values(LogicalType::INTEGER);
			if (column.Scan(transaction, offset / STANDARD_VECTOR_SIZE, state, values, count) != count) {
				throw InternalException("Demand scout short group scan");
			}
			UnifiedVectorFormat format;
			values.ToUnifiedFormat(format);
			const auto data = format.GetData<int32_t>();
			for (idx_t row = 0; row < count; ++row) {
				const auto selected = format.sel->get_index(row);
				if (!format.validity.RowIsValid(selected)) {
					continue;
				}
				const auto group = static_cast<int64_t>(data[selected]) - static_cast<int64_t>(group_min);
				if (group >= 0 && static_cast<idx_t>(group) < group_domain) {
					++frequencies[NumericCast<idx_t>(group)];
				}
			}
		}
	}
	idx_t seen = 0, once = 0, twice = 0;
	for (const auto count : frequencies) {
		seen += count != 0;
		once += count == 1;
		twice += count == 2;
	}
	return MinValue<idx_t>(group_domain, seen + once * once / (2 * MaxValue<idx_t>(1, twice)));
}

class ScoutSinkState : public GlobalSinkState {
public:
	ScoutSinkState(const PhysicalDemandScoutTopK &op, ClientContext &context)
	    : heaps(op.group_domain + 1, op.k), seen(op.group_domain + 1),
	      vacuum_lock(DuckTransactionManager::Get(op.table.GetStorage().GetAttached()).SharedVacuumLock()) {
		const auto sample_start = std::chrono::steady_clock::now();
		const auto threads = MaxValue<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
		const auto estimated_groups = SampleGroups(context, op.table, op.group_column, op.group_min, op.group_domain);
		sample_ms = ScoutElapsedMs(sample_start);
		const auto entries = MinValue<idx_t>(op.input_cardinality, estimated_groups * op.k * threads);
		const auto local_cost_ns = entries * 8.0 + op.input_cardinality * 0.5;
		const auto route_cost_ns = op.input_cardinality * 2.5;
		exchange = threads >= 2 && local_cost_ns > route_cost_ns;
		if (auto forced = std::getenv("DUCKDB_DEMAND_SCOUT_FORCE")) {
			if (std::string(forced) == "LOCAL") {
				exchange = false;
			} else if (std::string(forced) == "EXCHANGE") {
				exchange = threads >= 2;
			}
		}
		owners = exchange ? MaxValue<idx_t>(1, threads / 2) : 0;
		if (exchange) {
			for (idx_t owner = 0; owner < owners; ++owner) {
				queues.push_back(make_uniq<KeyQueue>());
			}
			for (idx_t owner = 0; owner < owners; ++owner) {
				consumers.emplace_back([this, owner] {
					try {
						std::vector<KeyTuple> batch;
						while (queues[owner]->Pop(batch)) {
							for (const auto &tuple : batch) {
								seen[tuple.group] = 1;
								if (tuple.valid) {
									heaps.Add(tuple.group, tuple.entry);
								}
							}
							batch.clear();
						}
					} catch (...) {
						{
							std::lock_guard<std::mutex> guard(error_lock);
							if (!consumer_error) {
								consumer_error = std::current_exception();
							}
						}
						for (auto &queue : queues) {
							queue->Abort();
						}
					}
				});
			}
		}
		if (auto trace = std::getenv("DUCKDB_DEMAND_SCOUT_TRACE"); trace && trace[0] == '1') {
			std::cerr << "DEMAND_SCOUT estimated_groups=" << estimated_groups
			          << " chosen=" << (exchange ? "EXCHANGE" : "LOCAL") << " workers=" << threads << '\n';
		}
		sink_start = std::chrono::steady_clock::now();
	}
	~ScoutSinkState() override {
		for (auto &queue : queues) {
			queue->Abort();
		}
		for (auto &consumer : consumers) {
			if (consumer.joinable()) {
				consumer.join();
			}
		}
	}
	idx_t MaxThreads(idx_t source_max_threads) override {
		return exchange ? MinValue(source_max_threads, owners) : source_max_threads;
	}
	PackedTopK heaps;
	std::vector<uint8_t> seen;
	bool exchange = false, finished = false;
	idx_t owners = 0;
	std::mutex combine_lock;
	std::mutex error_lock;
	std::vector<unique_ptr<KeyQueue>> queues;
	std::vector<std::thread> consumers;
	std::exception_ptr consumer_error;
	unique_ptr<StorageLockKey> vacuum_lock;
	std::chrono::steady_clock::time_point sink_start;
	double sample_ms = 0;
	std::atomic<idx_t> input_rows {0}, valid_rows {0};
};

class ScoutLocalState : public LocalSinkState {
public:
	ScoutLocalState(const PhysicalDemandScoutTopK &op, ScoutSinkState &global) : exchange(global.exchange) {
		if (exchange) {
			buffers.resize(global.owners);
			for (auto &buffer : buffers) {
				buffer.reserve(1024);
			}
		} else {
			heaps = PackedTopK(op.group_domain + 1, op.k);
			seen.resize(op.group_domain + 1);
		}
	}
	bool exchange;
	PackedTopK heaps;
	std::vector<uint8_t> seen;
	std::vector<std::vector<KeyTuple>> buffers;
	idx_t input_rows = 0, valid_rows = 0;
};

class ScoutSourceState : public GlobalSourceState {
public:
	idx_t next_group = 0;
	double output_ms = 0;
};

} // namespace

PhysicalDemandScoutTopK::PhysicalDemandScoutTopK(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                 idx_t estimated_cardinality, TableCatalogEntry &table,
                                                 column_t group_column, int32_t group_min, idx_t group_domain, idx_t k,
                                                 idx_t input_cardinality, bool score_unsigned, bool fused_handoff)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table(table), group_column(group_column), group_min(group_min), group_domain(group_domain), k(k),
      input_cardinality(input_cardinality), score_unsigned(score_unsigned), fused_handoff(fused_handoff) {
}

string PhysicalDemandScoutTopK::GetName() const {
	return "DEMAND_SCOUT_TOPK";
}

InsertionOrderPreservingMap<string> PhysicalDemandScoutTopK::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Group Domain"] = to_string(group_domain);
	result["Group Minimum"] = to_string(group_min);
	result["Winner Handoff"] = fused_handoff ? "DIRECT" : "LIST";
	result["K"] = to_string(k);
	return result;
}

unique_ptr<GlobalSinkState> PhysicalDemandScoutTopK::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<ScoutSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalDemandScoutTopK::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<ScoutLocalState>(*this, sink_state->Cast<ScoutSinkState>());
}

SinkResultType PhysicalDemandScoutTopK::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {
	auto &global = input.global_state.Cast<ScoutSinkState>();
	auto &local = input.local_state.Cast<ScoutLocalState>();
	const auto profile = ProfileScout();
	if (profile) {
		local.input_rows += chunk.size();
	}
	UnifiedVectorFormat groups, ids, scores;
	chunk.data[0].ToUnifiedFormat(groups);
	chunk.data[1].ToUnifiedFormat(ids);
	chunk.data[2].ToUnifiedFormat(scores);
	const auto group_data = groups.GetData<int32_t>();
	const auto id_data = ids.GetData<int64_t>();
	const auto signed_score_data = score_unsigned ? nullptr : scores.GetData<int64_t>();
	const auto unsigned_score_data = score_unsigned ? scores.GetData<uint64_t>() : nullptr;
	for (idx_t row = 0; row < chunk.size(); ++row) {
		const auto group_index = groups.sel->get_index(row);
		const auto id_index = ids.sel->get_index(row);
		const auto score_index = scores.sel->get_index(row);
		const auto group = groups.validity.RowIsValid(group_index)
		                       ? static_cast<int64_t>(group_data[group_index]) - static_cast<int64_t>(group_min)
		                       : static_cast<int64_t>(group_domain);
		if (group < 0 || static_cast<idx_t>(group) > group_domain) {
			throw InvalidInputException("Demand research Top-K group outside statistics-derived domain");
		}
		const auto target = static_cast<idx_t>(group);
		const auto valid = ids.validity.RowIsValid(id_index) && scores.validity.RowIsValid(score_index);
		if (profile) {
			local.valid_rows += valid;
		}
		const auto normalized_score =
		    !valid           ? uint64_t(0)
		    : score_unsigned ? unsigned_score_data[score_index]
		                     : (static_cast<uint64_t>(signed_score_data[score_index]) ^ (uint64_t(1) << 63));
		Entry entry {normalized_score, valid ? id_data[id_index] : 0};
		if (!global.exchange) {
			local.seen[target] = 1;
			if (valid) {
				local.heaps.Add(target, entry);
			}
			continue;
		}
		auto &buffer = local.buffers[target % global.owners];
		buffer.push_back({target, entry, valid});
		if (buffer.size() == 1024) {
			global.queues[target % global.owners]->Push(std::move(buffer));
			buffer.reserve(1024);
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalDemandScoutTopK::Combine(ExecutionContext &context,
                                                       OperatorSinkCombineInput &input) const {
	auto &global = input.global_state.Cast<ScoutSinkState>();
	auto &local = input.local_state.Cast<ScoutLocalState>();
	if (ProfileScout()) {
		global.input_rows.fetch_add(local.input_rows, std::memory_order_relaxed);
		global.valid_rows.fetch_add(local.valid_rows, std::memory_order_relaxed);
	}
	if (global.exchange) {
		for (idx_t owner = 0; owner < global.owners; ++owner) {
			if (!local.buffers[owner].empty()) {
				global.queues[owner]->Push(std::move(local.buffers[owner]));
			}
		}
	} else {
		std::lock_guard<std::mutex> guard(global.combine_lock);
		for (idx_t group = 0; group <= group_domain; ++group) {
			global.seen[group] |= local.seen[group];
			for (idx_t index = 0; index < local.heaps.Count(group); ++index) {
				global.heaps.Add(group, local.heaps.At(group, index));
			}
		}
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalDemandScoutTopK::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	auto &global = input.global_state.Cast<ScoutSinkState>();
	for (auto &queue : global.queues) {
		queue->Close();
	}
	for (auto &consumer : global.consumers) {
		if (consumer.joinable()) {
			consumer.join();
		}
	}
	if (global.consumer_error) {
		std::rethrow_exception(global.consumer_error);
	}
	if (ProfileScout()) {
		idx_t groups = 0, winners = 0;
		for (idx_t group = 0; group <= group_domain; ++group) {
			groups += global.seen[group] != 0;
			winners += global.heaps.Count(group);
		}
		std::cerr << "DEMAND_SCOUT_PROFILE input_rows=" << global.input_rows.load()
		          << " valid_rows=" << global.valid_rows.load() << " groups=" << groups << " winners=" << winners
		          << " sample_ms=" << global.sample_ms << " sink_wall_ms=" << ScoutElapsedMs(global.sink_start)
		          << " route=" << (global.exchange ? "EXCHANGE" : "LOCAL") << '\n';
	}
	global.finished = true;
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalDemandScoutTopK::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<ScoutSourceState>();
}

vector<int64_t> PhysicalDemandScoutTopK::WinnerRowIds(const Value &group) const {
	auto &global = sink_state->Cast<ScoutSinkState>();
	D_ASSERT(global.finished && fused_handoff);
	const auto normalized = group.IsNull()
	                            ? static_cast<int64_t>(group_domain)
	                            : static_cast<int64_t>(group.GetValue<int32_t>()) - static_cast<int64_t>(group_min);
	if (normalized < 0 || static_cast<idx_t>(normalized) > group_domain) {
		throw InternalException("Demand fused handoff received group outside domain");
	}
	auto entries = global.heaps.Entries(static_cast<idx_t>(normalized));
	std::sort(entries.begin(), entries.end(), [](const Entry &a, const Entry &b) { return a.score > b.score; });
	vector<int64_t> result;
	result.reserve(entries.size());
	for (const auto &entry : entries) {
		result.push_back(entry.value);
	}
	return result;
}

SourceResultType PhysicalDemandScoutTopK::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                          OperatorSourceInput &input) const {
	const auto output_start = std::chrono::steady_clock::now();
	auto &global = sink_state->Cast<ScoutSinkState>();
	auto &source = input.global_state.Cast<ScoutSourceState>();
	D_ASSERT(global.finished);
	idx_t output = 0;
	while (source.next_group <= group_domain && output < STANDARD_VECTOR_SIZE) {
		const auto group = source.next_group++;
		if (!global.seen[group]) {
			continue;
		}
		chunk.data[0].SetValue(output, group == group_domain
		                                   ? Value()
		                                   : Value::INTEGER(NumericCast<int32_t>(static_cast<int64_t>(group_min) +
		                                                                         static_cast<int64_t>(group))));
		if (fused_handoff) {
			++output;
			continue;
		}
		if (!global.heaps.Count(group)) {
			chunk.data[1].SetValue(output++, Value());
			continue;
		}
		auto entries = global.heaps.Entries(group);
		std::sort(entries.begin(), entries.end(), [](const Entry &a, const Entry &b) { return a.score > b.score; });
		vector<Value> values;
		values.reserve(entries.size());
		for (const auto &entry : entries) {
			values.push_back(Value::BIGINT(entry.value));
		}
		chunk.data[1].SetValue(output, Value::LIST(LogicalType::BIGINT, std::move(values)));
		++output;
	}
	chunk.SetChildCardinality(output);
	if (ProfileScout()) {
		source.output_ms += ScoutElapsedMs(output_start);
		if (output && source.next_group > group_domain) {
			std::cerr << "DEMAND_SCOUT_OUTPUT output_ms=" << source.output_ms << '\n';
		}
	}
	return output ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
}

} // namespace duckdb
