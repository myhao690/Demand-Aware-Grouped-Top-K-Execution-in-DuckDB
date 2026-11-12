#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class TableCatalogEntry;
class ClientContext;

// Research-only grouped arg_max(STRUCT, BIGINT/UBIGINT, constant K) specialization.
// LOCAL/EXCHANGE use the normal scan; forced OWNED repeats a snapshot-visible key scan per owner.
class PhysicalDemandScoutTopK : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalDemandScoutTopK(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
	                        TableCatalogEntry &table, column_t group_column, column_t score_column, int32_t group_min,
	                        idx_t group_domain, idx_t k, idx_t input_cardinality, bool score_unsigned,
	                        bool fused_handoff, bool owned);

	TableCatalogEntry &table;
	column_t group_column;
	column_t score_column;
	int32_t group_min;
	idx_t group_domain;
	idx_t k;
	idx_t input_cardinality;
	bool score_unsigned;
	bool fused_handoff;
	bool owned;

	vector<int64_t> WinnerRowIds(const Value &group) const;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

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

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	bool SinkOrderDependent() const override {
		return false;
	}
	PipelineExternalInputSupport GetExternalInputSupport() const override {
		return PipelineExternalInputSupport::SUPPORTED;
	}
};

} // namespace duckdb
