#pragma once

#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

namespace duckdb {

class PhysicalTrampoline : public PhysicalRecursiveCTE {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RECURSIVE_CTE;

	bool ParallelSink() const override {
		return false;
	}

public:
	PhysicalTrampoline(string ctename, idx_t table_index, vector<LogicalType> types, bool union_all,
	                   unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom,
	                   vector<unique_ptr<PhysicalOperator>> children, idx_t estimated_cardinality);

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	void ExecuteRecursivePipelines(ExecutionContext &context) const;
};

} // namespace duckdb
