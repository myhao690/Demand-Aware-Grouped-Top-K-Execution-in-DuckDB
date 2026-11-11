#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class TableCatalogEntry;

// Materializes grouped winner rowids into LIST<STRUCT> using block-local payload scans.
// Transaction-local rows use DuckDB's transactional rowid fetch instead.
class PhysicalDemandBurst : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;
	PhysicalDemandBurst(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                    TableCatalogEntry &table, LogicalType payload_type, vector<column_t> payload_columns,
	                    bool fused_handoff);

	TableCatalogEntry &table;
	LogicalType payload_type;
	vector<column_t> payload_columns;
	bool fused_handoff;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	bool SinkOrderDependent() const override {
		return false;
	}
	PipelineExternalInputSupport GetExternalInputSupport() const override {
		return PipelineExternalInputSupport::SUPPORTED;
	}
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return false;
	}
	OrderPreservationType SourceOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}
};

} // namespace duckdb
