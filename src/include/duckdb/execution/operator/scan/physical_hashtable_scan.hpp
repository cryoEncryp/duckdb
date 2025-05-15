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
	    : max_threads(MaxValue<idx_t>(1, collection->GetPartitionedData().PartitionCount())), collection(collection),
	payload_types(collection->payload_types), distinct_types(collection->distinct_types) {
	}

	idx_t MaxThreads() override {
		return max_threads;
	}
	const idx_t max_threads;
	const shared_ptr<GroupedAggregateHashTable> &collection;
	const vector<LogicalType>& payload_types, &distinct_types;
	idx_t partition_idx = 0;
	bool init = false;
	mutex lock;
};


class PhysicalHashTableLocalScanState : public LocalSourceState {
public:
	PhysicalHashTableLocalScanState(PhysicalHashTableGlobalScanState &gstate) {
		partition_idx = gstate.partition_idx++;
		// Intialize which scan for specific partition
		gstate.collection->InitializeScan(scan_state, partition_idx);
		// Initialize chunks to scan partition
		payload.Initialize(Allocator::DefaultAllocator(), gstate.payload_types);
		keys.Initialize(Allocator::DefaultAllocator(), gstate.distinct_types);
	}
	// index of the partition that is scanned
	idx_t partition_idx;
	DataChunk payload;
	DataChunk keys;

	AggregateHTScanState scan_state;
	bool init = false;
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