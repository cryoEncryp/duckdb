#include "duckdb/execution/operator/scan/physical_hashtable_scan.hpp"

#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

namespace duckdb {

	PhysicalHashTableScan::PhysicalHashTableScan(vector<LogicalType> types, PhysicalOperatorType op_type,
                                             	 idx_t estimated_cardinality,
                                             	 shared_ptr<GroupedAggregateHashTable> collection_p)
	    : PhysicalOperator(op_type, std::move(types), estimated_cardinality), ht(collection_p) {
    }

    void PhysicalHashTableScan::PopulateChunk(DataChunk &group_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set, bool reference) const {
	    idx_t chunk_index = 0;
	    // Populate the group_chunk
	    for (auto &group_idx : idx_set) {
		    if (reference) {
			    // Reference from input_chunk[chunk_index] -> group_chunk[group_idx]
			    group_chunk.data[chunk_index++].Reference(input_chunk.data[group_idx]);
		    } else {
			    // Reference from input_chunk[group.index] -> group_chunk[chunk_index]
			    group_chunk.data[group_idx].Reference(input_chunk.data[chunk_index++]);
		    }
	    }
	    group_chunk.SetCardinality(input_chunk.size());
    }



    unique_ptr<GlobalSourceState> PhysicalHashTableScan::GetGlobalSourceState(ClientContext &context) const {
	    return make_uniq<PhysicalHashTableGlobalScanState>(ht);
    }

    unique_ptr<LocalSourceState> PhysicalHashTableScan::GetLocalSourceState(ExecutionContext& context, GlobalSourceState& state) const {
	    auto& gstate = state.Cast<PhysicalHashTableGlobalScanState>();
	    return make_uniq<PhysicalHashTableLocalScanState>(gstate);
    }


    SourceResultType PhysicalHashTableScan::GetData(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	    auto& lstate = input.local_state.Cast<PhysicalHashTableLocalScanState>();

	    ht->Scan(lstate.scan_state, lstate.keys, lstate.payload);

	    PopulateChunk(chunk, lstate.keys, ht->distinct_idx, false);
	    PopulateChunk(chunk, lstate.payload, ht->payload_idx, false);

	    return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
    }

    void PhysicalHashTableScan::BuildPipelines(Pipeline &current, MetaPipeline &metaPipeline) {
	    auto &state = metaPipeline.GetState();
	    D_ASSERT(children.empty());
	    state.SetPipelineSource(current, *this);
    }

    InsertionOrderPreservingMap<string> PhysicalHashTableScan::ParamsToString() const {
	    InsertionOrderPreservingMap<string> result;
	    SetEstimatedCardinality(result, estimated_cardinality);
	    return result;
    }
}