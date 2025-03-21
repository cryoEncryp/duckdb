//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_hashtable_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/optionally_owned_ptr.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

namespace duckdb {
class PhysicalHashTableScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;
public:
	PhysicalHashTableScan(vector<LogicalType> types, PhysicalOperatorType op_type, idx_t estimated_cardinality,
	                      shared_ptr<GroupedAggregateHashTable> collection_p);

	shared_ptr<GroupedAggregateHashTable> ht;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	void PopulateChunk(DataChunk &group_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set, bool reference) const;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	void BuildPipelines(Pipeline &current, MetaPipeline &metaPipeline) override;

	bool IsSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
	// BTODO: not parallel yet, have to change scan of aggregate hashtable
	bool ParallelSource() const override {
		return false;
	}
};

}