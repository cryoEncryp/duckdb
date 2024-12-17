
#include "duckdb/execution/operator/set/physical_trampoline.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

class TrampolineCTEState : public GlobalSinkState {
public:
	explicit TrampolineCTEState(ClientContext &context, const PhysicalTrampoline &op) {
		// Initialize the intermediate tables and scan states, each branch gets
		// a separate intermediate table and scan state
		// intermediate table on index 0 should be scanned for results.
		for (auto &child : op.children) {
			intermediate_tables.emplace_back(make_shared_ptr<ColumnDataCollection>(context, child->GetTypes()));
			scan_states.emplace_back(ColumnDataScanState());
			new_groups.push_back(SelectionVector(STANDARD_VECTOR_SIZE));
		}
	}
	bool intermediate_empty = true;
	mutex intermediate_table_lock;
	bool initialized = false;
	bool finished_scan = false;

	// Each branch, including the return branch (Q0), has its own intermediate table and state.
	vector<shared_ptr<ColumnDataCollection>> intermediate_tables;
	// BTODO: Is it necessary to create a scan state for each branch? We only scan branch 0 other intermediate tables
	// are only combined(?)
	vector<ColumnDataScanState> scan_states;
	// SelectionVectors used to match incoming rows to the correct intermediate table…
	vector<SelectionVector> new_groups;
};

PhysicalTrampoline::PhysicalTrampoline(string ctename, idx_t table_index, vector<LogicalType> types, bool union_all,
                                       unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom,
                                       vector<unique_ptr<PhysicalOperator>> children, idx_t estimated_cardinality)
    : PhysicalRecursiveCTE(ctename, table_index, std::move(types), union_all, std::move(top), std::move(bottom),
                           estimated_cardinality) {
	for (idx_t child_index = 0; child_index < children.size(); child_index++) {
		if (child_index >= 2) {
			this->children.emplace_back(std::move(children[child_index]));
		}
	}
}

unique_ptr<GlobalSinkState> PhysicalTrampoline::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TrampolineCTEState>(context, *this);
}

SinkResultType PhysicalTrampoline::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<TrampolineCTEState>();
	lock_guard<mutex> guard(gstate.intermediate_table_lock);

	// We need to count the number of new rows for each branch.
	vector<idx_t> new_row_counts(children.size(), 0);

	// We iterate over all incoming rows, checking the value in the first column.
	// The value in the first column is stored in the SelectionVector,
	// which also increases the number of rows in the SelectionVector.
	for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		idx_t branch_idx = chunk.GetValue(0, row_idx).GetValue<idx_t>();
		gstate.new_groups[branch_idx].set_index(new_row_counts[branch_idx]++, row_idx);
	}

	// After we collected all SelectionVector we slice the chunk and add it to the correct intermediate table
	for (idx_t branch_idx = 0; branch_idx < children.size(); branch_idx++) {
		// If we do not need to insert a line into the branch, we can skip the copy.
		if (new_row_counts[branch_idx] > 0) {
			DataChunk copy_chunk;
			copy_chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
			chunk.Copy(copy_chunk, gstate.new_groups[branch_idx], new_row_counts[branch_idx], 0);
			gstate.intermediate_tables[branch_idx]->Append(copy_chunk);
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//

void PhysicalTrampoline::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();
	recursive_meta_pipeline.reset();

	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);

	auto &executor = meta_pipeline.GetExecutor();
	executor.AddRecursiveCTE(*this);

	// the LHS of the recursive CTE is our initial state (q0),
	// pipeline current can only start, when Pipeline for q0 is rdy
	auto &initial_state_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	initial_state_pipeline.Build(*children[0]);

	// Create a new MetaPipeline that should contain all the Pipeline for the Branches
	recursive_meta_pipeline = make_shared_ptr<MetaPipeline>(executor, state, this);
	recursive_meta_pipeline->SetRecursiveCTE();

	vector<const_reference<PhysicalOperator>> ops;
	// The idea is that children contains all branches of the trampoline
	auto first = true;
	for (idx_t child_index = 1; child_index < children.size(); child_index++) {
		if (first) {
			recursive_meta_pipeline->Build(*children[child_index]);
			first = false;
		} else {
			// BTODO: Why is new Pipeline needed?
			recursive_meta_pipeline->CreatePipeline();
			recursive_meta_pipeline->Build(*children[child_index]);
		}
		GatherColumnDataScans(*children[child_index], ops);
	}

	// Create a dependency to all found column data scan, so that we do not scan until all scans are ready
	for (auto op : ops) {
		auto entry = state.cte_dependencies.find(op);
		if (entry == state.cte_dependencies.end()) {
			continue;
		}
		// this chunk scan introduces a dependency to the current pipeline
		// namely a dependency on the CTE pipeline to finish
		auto cte_dependency = entry->second.get().shared_from_this();
		current.AddDependency(cte_dependency);
	}
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalTrampoline::GetData(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<TrampolineCTEState>();

	if (!gstate.initialized) {
		// The first call to GetData initialises the scan of the intermediate table.
		gstate.intermediate_tables[0]->InitializeScan(gstate.scan_states[0]);
		gstate.finished_scan = false;
		gstate.initialized = true;
	}
	while (chunk.size() == 0) {
		if (!gstate.finished_scan) {
			// After each iteration, we scan all the rows collected so far from the Q0 table.
			gstate.intermediate_tables[0]->Scan(gstate.scan_states[0], chunk);
			if (chunk.size() == 0) {
				gstate.finished_scan = true;
			} else {
				break;
			}
		} else {
			// We have scanned all the rows produced for table Q0 and can start
			// with another recursion. If we do not produce any rows, we are done.

			// We set up the working table with the data we gathered in this iteration.
			for (idx_t branch_idx = 0; branch_idx < working_tables.size(); branch_idx++) {
				working_tables[branch_idx]->Reset();
				working_tables[branch_idx]->Combine(*gstate.intermediate_tables[branch_idx + 1]);
			}

			// We clear the intermediate table
			gstate.finished_scan = false;
			for (auto intermediate_table : gstate.intermediate_tables) {
				intermediate_table->Reset();
			}
			// Now we need to re-execute all the pipelines that depend on the recursion.
			ExecuteRecursivePipelines(context);

			// Check if we obtained any rows, if not, we are done.
			bool no_rows = true;
			for (auto intermediate_table : gstate.intermediate_tables) {
				if (intermediate_table->Count() > 0) {
					no_rows = false;
					break;
				}
			}
			if (no_rows) {
				gstate.finished_scan = true;
				break;
			}
			// We have produced new rows.
			// We may therefore have new rows in table Q0 that need to be scanned.
			gstate.intermediate_tables[0]->InitializeScan(gstate.scan_states[0]);
		}
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

// BTODO: need to be changed completely
void PhysicalTrampoline::ExecuteRecursivePipelines(ExecutionContext &context) const {
	if (!recursive_meta_pipeline) {
		throw InternalException("Missing meta pipeline for recursive CTE");
	}
	D_ASSERT(recursive_meta_pipeline->HasRecursiveCTE());

	// get and reset pipelines
	vector<shared_ptr<Pipeline>> pipelines;
	recursive_meta_pipeline->GetPipelines(pipelines, true);
	for (auto &pipeline : pipelines) {
		auto sink = pipeline->GetSink();
		if (sink.get() != this) {
			sink->sink_state.reset();
		}
		for (auto &op_ref : pipeline->GetOperators()) {
			auto &op = op_ref.get();
			op.op_state.reset();
		}
		pipeline->ClearSource();
	}

	// get the MetaPipelines in the recursive_meta_pipeline and reschedule them
	vector<shared_ptr<MetaPipeline>> meta_pipelines;
	recursive_meta_pipeline->GetMetaPipelines(meta_pipelines, true, false);
	auto &executor = recursive_meta_pipeline->GetExecutor();
	vector<shared_ptr<Event>> events;
	executor.ReschedulePipelines(meta_pipelines, events);

	while (true) {
		executor.WorkOnTasks();
		if (executor.HasError()) {
			executor.ThrowException();
		}
		bool finished = true;
		for (auto &event : events) {
			if (!event->IsFinished()) {
				finished = false;
				break;
			}
		}
		if (finished) {
			// all pipelines finished: done!
			break;
		}
	}
}

} // namespace duckdb
