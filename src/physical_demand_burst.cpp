#include "duckdb/execution/operator/aggregate/physical_demand_burst.hpp"
#include "duckdb/execution/operator/aggregate/physical_demand_scout_topk.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace duckdb {

namespace {

struct GroupRow {
	Value group;
	bool null_list = false;
	std::vector<int64_t> ids;
	struct PayloadRow {
		int64_t id = 0;
		bool id_valid = false;
		std::array<double, 6> values {};
		uint8_t valid = 0;
	};
	std::vector<PayloadRow> payloads;
};
struct FetchRef {
	idx_t rid;
	idx_t group;
	idx_t item;
	idx_t block;
};
struct RowSpan {
	idx_t index;
	idx_t start;
	idx_t count;
	idx_t first_block;
};
struct BlockTask {
	idx_t first;
	idx_t last;
	idx_t block_start;
	idx_t span_index;
};

class BurstSinkState : public GlobalSinkState {
public:
	std::mutex lock;
	std::vector<GroupRow> rows;
	bool finished = false;
	double sink_ms = 0;
	double prepare_ms = 0;
	double fetch_ms = 0;
	idx_t winner_count = 0;
	idx_t block_count = 0;
};
class BurstSourceState : public GlobalSourceState {
public:
	idx_t next = 0;
	double output_ms = 0;
};

static double ElapsedMs(std::chrono::steady_clock::time_point start) {
	return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

static bool ProfileBurst() {
	const auto enabled = std::getenv("DUCKDB_DEMAND_BURST_PROFILE");
	return enabled && enabled[0] == '1';
}

static Vector ScanBlock(ClientContext &context, DuckTransaction &transaction, RowGroup &row_group,
                        column_t column_index, const LogicalType &type, idx_t local_start, idx_t count) {
	auto &column = row_group.GetColumnForDemand(column_index);
	ColumnScanState state(nullptr);
	state.Initialize(context, type, StorageIndex(column_index), nullptr);
	column.InitializeScanWithOffset(state, local_start);
	Vector values(type);
	if (column.Scan(transaction, local_start / STANDARD_VECTOR_SIZE, state, values, count) != count) {
		throw InternalException("Demand Burst short Payload scan");
	}
	return values;
}

} // namespace

PhysicalDemandBurst::PhysicalDemandBurst(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                         idx_t estimated_cardinality, TableCatalogEntry &table,
                                         LogicalType payload_type, vector<column_t> payload_columns, bool fused_handoff)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table(table), payload_type(std::move(payload_type)), payload_columns(std::move(payload_columns)),
      fused_handoff(fused_handoff) {
}

string PhysicalDemandBurst::GetName() const {
	return "DEMAND_BURST";
}

InsertionOrderPreservingMap<string> PhysicalDemandBurst::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Payload Fields"] = to_string(payload_columns.size());
	result["Winner Handoff"] = fused_handoff ? "DIRECT" : "LIST";
	return result;
}

unique_ptr<GlobalSinkState> PhysicalDemandBurst::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<BurstSinkState>();
}

SinkResultType PhysicalDemandBurst::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	const auto start = std::chrono::steady_clock::now();
	auto &state = input.global_state.Cast<BurstSinkState>();
	std::lock_guard<std::mutex> guard(state.lock);
	if (fused_handoff) {
		auto &scout = children[0].get().Cast<PhysicalDemandScoutTopK>();
		for (idx_t row = 0; row < chunk.size(); ++row) {
			GroupRow result;
			result.group = chunk.data[0].GetValue(row);
			result.ids = scout.WinnerRowIds(result.group);
			result.null_list = result.ids.empty();
			result.payloads.resize(result.ids.size());
			state.rows.push_back(std::move(result));
		}
		if (ProfileBurst()) {
			state.sink_ms += ElapsedMs(start);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}
	UnifiedVectorFormat lists, ids;
	chunk.data[1].ToUnifiedFormat(lists);
	ListVector::GetChild(chunk.data[1]).ToUnifiedFormat(ids);
	const auto *list_data = lists.GetData<list_entry_t>();
	const auto *id_data = ids.GetData<int64_t>();
	for (idx_t row = 0; row < chunk.size(); ++row) {
		GroupRow result;
		result.group = chunk.data[0].GetValue(row);
		const auto list_index = lists.sel->get_index(row);
		result.null_list = !lists.validity.RowIsValid(list_index);
		if (!result.null_list) {
			const auto entry = list_data[list_index];
			result.ids.reserve(entry.length);
			for (idx_t item = 0; item < entry.length; ++item) {
				const auto id_index = ids.sel->get_index(entry.offset + item);
				if (!ids.validity.RowIsValid(id_index)) {
					throw InvalidInputException("Demand Burst requires non-NULL winner ids");
				}
				result.ids.push_back(id_data[id_index]);
			}
			result.payloads.resize(result.ids.size());
		}
		state.rows.push_back(std::move(result));
	}
	if (ProfileBurst()) {
		state.sink_ms += ElapsedMs(start);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalDemandBurst::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalDemandBurst::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	const auto prepare_start = std::chrono::steady_clock::now();
	auto &state = input.global_state.Cast<BurstSinkState>();
	auto &transaction = DuckTransaction::Get(context, table.ParentCatalog());
	auto row_groups = table.GetStorage().GetRowGroupCollection();
	std::vector<RowSpan> spans;
	idx_t physical_blocks = 0;
	auto segments = row_groups->GetRowGroups();
	for (idx_t index = 0; index < row_groups->GetRowGroupCount(); ++index) {
		auto segment = segments->GetSegmentByIndex(NumericCast<int64_t>(index));
		if (!segment) {
			throw InternalException("Demand Burst missing row group");
		}
		spans.push_back({index, segment->GetRowStart(), segment->GetCount(), physical_blocks});
		physical_blocks += (segment->GetCount() + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	}
	std::vector<FetchRef> refs;
	std::vector<FetchRef> local_refs;
	for (idx_t group = 0; group < state.rows.size(); ++group) {
		for (idx_t item = 0; item < state.rows[group].ids.size(); ++item) {
			const auto id = state.rows[group].ids[item];
			if (id < 0) {
				throw InternalException("Demand Burst received negative rowid");
			}
			const auto rid = static_cast<idx_t>(id);
			if (rid >= MAX_ROW_ID) {
				local_refs.push_back({rid, group, item, 0});
				continue;
			}
			auto it = std::upper_bound(spans.begin(), spans.end(), rid,
			                           [](idx_t rowid, const RowSpan &span) { return rowid < span.start; });
			if (it == spans.begin() || rid >= (it - 1)->start + (it - 1)->count) {
				throw InternalException("Demand Burst rowid outside row groups");
			}
			--it;
			refs.push_back({rid, group, item, it->first_block + (rid - it->start) / STANDARD_VECTOR_SIZE});
		}
	}
	std::vector<idx_t> offsets(physical_blocks + 1);
	for (const auto &ref : refs) {
		++offsets[ref.block + 1];
	}
	for (idx_t block = 1; block < offsets.size(); ++block) {
		offsets[block] += offsets[block - 1];
	}
	const auto order = std::getenv("DUCKDB_DEMAND_BURST_ORDER");
	if (order && std::string(order) == "SORT") {
		std::sort(refs.begin(), refs.end(), [](const FetchRef &a, const FetchRef &b) { return a.rid < b.rid; });
	} else {
		auto next = offsets;
		std::vector<FetchRef> ordered(refs.size());
		for (const auto &ref : refs) {
			ordered[next[ref.block]++] = ref;
		}
		refs.swap(ordered);
	}
	idx_t span_index = 0;
	std::vector<BlockTask> tasks;
	for (idx_t block = 0; block < physical_blocks; ++block) {
		if (offsets[block] == offsets[block + 1]) {
			continue;
		}
		while (span_index + 1 < spans.size() && block >= spans[span_index + 1].first_block) {
			++span_index;
		}
		const auto &span = spans[span_index];
		const auto block_start = span.start + (block - span.first_block) * STANDARD_VECTOR_SIZE;
		tasks.push_back({offsets[block], offsets[block + 1], block_start, span_index});
	}
	if (ProfileBurst()) {
		state.prepare_ms = ElapsedMs(prepare_start);
		state.winner_count = refs.size() + local_refs.size();
		state.block_count = tasks.size();
	}
	const auto fetch_start = std::chrono::steady_clock::now();
	std::atomic<idx_t> next_task {0};
	std::atomic<bool> failed {false};
	std::mutex error_lock;
	std::exception_ptr error;
	auto fetch = [&] {
		try {
			while (!failed) {
				const auto task_index = next_task.fetch_add(1);
				if (task_index >= tasks.size()) {
					break;
				}
				const auto &task = tasks[task_index];
				const auto &span = spans[task.span_index];
				auto row_group = row_groups->GetRowGroup(NumericCast<int64_t>(span.index));
				const auto local_start = task.block_start - span.start;
				const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, span.count - local_start);
				auto ids = ScanBlock(context, transaction, *row_group, payload_columns[0], LogicalType::BIGINT,
				                     local_start, count);
				UnifiedVectorFormat id_format;
				ids.ToUnifiedFormat(id_format);
				const auto id_data = id_format.GetData<int64_t>();
				for (idx_t ref = task.first; ref < task.last; ++ref) {
					const auto offset = id_format.sel->get_index(refs[ref].rid - task.block_start);
					auto &payload = state.rows[refs[ref].group].payloads[refs[ref].item];
					payload.id_valid = id_format.validity.RowIsValid(offset);
					if (payload.id_valid) {
						payload.id = id_data[offset];
					}
				}
				for (idx_t field = 1; field < payload_columns.size(); ++field) {
					auto values = ScanBlock(context, transaction, *row_group, payload_columns[field],
					                        LogicalType::DOUBLE, local_start, count);
					UnifiedVectorFormat format;
					values.ToUnifiedFormat(format);
					const auto data = format.GetData<double>();
					for (idx_t ref = task.first; ref < task.last; ++ref) {
						const auto offset = format.sel->get_index(refs[ref].rid - task.block_start);
						auto &payload = state.rows[refs[ref].group].payloads[refs[ref].item];
						if (format.validity.RowIsValid(offset)) {
							payload.values[field - 1] = data[offset];
							payload.valid |= uint8_t(1U << (field - 1));
						}
					}
				}
			}
		} catch (...) {
			std::lock_guard<std::mutex> guard(error_lock);
			if (!error) {
				error = std::current_exception();
			}
			failed = true;
		}
	};
	const auto configured_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	const auto serial = std::getenv("DUCKDB_DEMAND_BURST_SERIAL");
	const auto thread_count =
	    serial && serial[0] == '1' ? idx_t(1) : MinValue<idx_t>(tasks.size(), MaxValue<idx_t>(1, configured_threads));
	std::vector<std::thread> workers;
	for (idx_t thread = 1; thread < thread_count; ++thread) {
		workers.emplace_back(fetch);
	}
	fetch();
	for (auto &worker : workers) {
		worker.join();
	}
	if (error) {
		std::rethrow_exception(error);
	}
	// Transaction-local inserts have rowids outside the persistent segment tree.
	// Fetch them through DuckDB's transactional API rather than assuming a physical block.
	if (!local_refs.empty()) {
		vector<StorageIndex> columns;
		vector<LogicalType> types;
		for (idx_t field = 0; field < payload_columns.size(); ++field) {
			columns.emplace_back(payload_columns[field]);
			types.push_back(field == 0 ? LogicalType::BIGINT : LogicalType::DOUBLE);
		}
		for (idx_t first = 0; first < local_refs.size(); first += STANDARD_VECTOR_SIZE) {
			const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, local_refs.size() - first);
			Vector row_ids(LogicalType::ROW_TYPE, count);
			auto *row_id_data = FlatVector::GetDataMutable<row_t>(row_ids);
			for (idx_t i = 0; i < count; ++i) {
				row_id_data[i] = NumericCast<row_t>(local_refs[first + i].rid);
			}
			DataChunk fetched;
			fetched.Initialize(context, types);
			ColumnFetchState fetch_state;
			table.GetStorage().Fetch(transaction, fetched, columns, row_ids, count, fetch_state);
			if (fetched.size() != count) {
				throw InternalException("Demand Burst local rowid visibility changed");
			}
			for (idx_t field = 0; field < columns.size(); ++field) {
				UnifiedVectorFormat format;
				fetched.data[field].ToUnifiedFormat(format);
				for (idx_t i = 0; i < count; ++i) {
					const auto selected = format.sel->get_index(i);
					auto &payload = state.rows[local_refs[first + i].group].payloads[local_refs[first + i].item];
					if (!format.validity.RowIsValid(selected)) {
						continue;
					}
					if (field == 0) {
						payload.id = format.GetData<int64_t>()[selected];
						payload.id_valid = true;
					} else {
						payload.values[field - 1] = format.GetData<double>()[selected];
						payload.valid |= uint8_t(1U << (field - 1));
					}
				}
			}
		}
	}
	if (ProfileBurst()) {
		state.fetch_ms = ElapsedMs(fetch_start);
	}
	state.finished = true;
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSourceState> PhysicalDemandBurst::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<BurstSourceState>();
}

SourceResultType PhysicalDemandBurst::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	const auto output_start = std::chrono::steady_clock::now();
	auto &state = sink_state->Cast<BurstSinkState>();
	auto &source = input.global_state.Cast<BurstSourceState>();
	D_ASSERT(state.finished);
	const auto end = MinValue<idx_t>(state.rows.size(), source.next + STANDARD_VECTOR_SIZE);
	const auto output = end - source.next;
	if (!output) {
		chunk.SetChildCardinality(0);
		return SourceResultType::FINISHED;
	}
	idx_t total_items = 0;
	for (idx_t index = source.next; index < end; ++index) {
		total_items += state.rows[index].ids.size();
	}
	auto &list = chunk.data[1];
	ListVector::Reserve(list, total_items);
	ListVector::SetListSize(list, total_items);
	auto *list_entries = FlatVector::GetDataMutable<list_entry_t>(list);
	auto &list_validity = FlatVector::ValidityMutable(list);
	list_validity.SetAllValid(output);
	auto &struct_vector = ListVector::GetChildMutable(list);
	auto &children = StructVector::GetEntries(struct_vector);
	FlatVector::ValidityMutable(struct_vector).SetAllValid(total_items);
	auto *id_data = FlatVector::GetDataMutable<int64_t>(children[0]);
	auto &id_validity = FlatVector::ValidityMutable(children[0]);
	id_validity.SetAllValid(total_items);
	std::array<double *, 6> field_data {};
	std::array<ValidityMask *, 6> field_validity {};
	for (idx_t field = 1; field < payload_columns.size(); ++field) {
		field_data[field - 1] = FlatVector::GetDataMutable<double>(children[field]);
		field_validity[field - 1] = &FlatVector::ValidityMutable(children[field]);
		field_validity[field - 1]->SetAllValid(total_items);
	}
	idx_t item_offset = 0;
	for (idx_t index = source.next; index < end; ++index) {
		const auto output_index = index - source.next;
		const auto &row = state.rows[index];
		chunk.data[0].SetValue(output_index, row.group);
		if (row.null_list) {
			list_validity.SetInvalid(output_index);
			continue;
		}
		list_entries[output_index] = list_entry_t(item_offset, row.ids.size());
		for (idx_t item = 0; item < row.ids.size(); ++item) {
			const auto position = item_offset + item;
			const auto &payload = row.payloads[item];
			id_data[position] = payload.id;
			if (!payload.id_valid) {
				id_validity.SetInvalid(position);
			}
			for (idx_t field = 1; field < payload_columns.size(); ++field) {
				field_data[field - 1][position] = payload.values[field - 1];
				if (!(payload.valid & uint8_t(1U << (field - 1)))) {
					field_validity[field - 1]->SetInvalid(position);
				}
			}
		}
		item_offset += row.ids.size();
	}
	source.next = end;
	chunk.SetChildCardinality(output);
	if (ProfileBurst()) {
		source.output_ms += ElapsedMs(output_start);
		if (source.next == state.rows.size()) {
			std::cerr << "DEMAND_BURST_PROFILE groups=" << state.rows.size() << " winners=" << state.winner_count
			          << " blocks=" << state.block_count << " sink_ms=" << state.sink_ms
			          << " prepare_ms=" << state.prepare_ms << " fetch_ms=" << state.fetch_ms
			          << " output_ms=" << source.output_ms << '\n';
		}
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
