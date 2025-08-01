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
#include "iostream"

namespace duckdb {

class PhysicalHashTableGlobalScanState : public GlobalSourceState {
public:
	PhysicalHashTableGlobalScanState(const shared_ptr<GroupedAggregateHashTable> &collection)
	    : payload_types(collection->payload_types), distinct_types(collection->distinct_types),
	      max_threads(MaxValue<idx_t>(collection->GetPartitionedData().GetChunkCount(), 1)) {
		collection->InitializeScan(scan_state);
	}

	idx_t MaxThreads() override {
		return max_threads;
	}
	const vector<LogicalType> &payload_types, &distinct_types;
	const idx_t max_threads = 1;
	mutex lock;
	AggregateHTScanState scan_state;
};


class PhysicalHashTableLocalScanState : public LocalSourceState {
public:
	PhysicalHashTableLocalScanState(PhysicalHashTableGlobalScanState &gstate) {
		// Initialize chunks to scan partition
		payload.Initialize(Allocator::DefaultAllocator(), gstate.payload_types);
		keys.Initialize(Allocator::DefaultAllocator(), gstate.distinct_types);
	}
	DataChunk payload;
	DataChunk keys;
};


class PhysicalHashTableScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;
public:
	PhysicalHashTableScan(vector<LogicalType> types, PhysicalOperatorType op_type, idx_t estimated_cardinality,
	                      shared_ptr<GroupedAggregateHashTable> collection_p);

	shared_ptr<GroupedAggregateHashTable> ht;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;

	void PopulateChunk(DataChunk &group_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set, bool reference) const;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	void BuildPipelines(Pipeline &current, MetaPipeline &metaPipeline) override;

	bool IsSource() const override {
		return true;
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override;
	bool ParallelSource() const override {
		return true;
	}
};

}