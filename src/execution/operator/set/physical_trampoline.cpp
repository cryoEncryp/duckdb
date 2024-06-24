
#include "duckdb/execution/operator/set/physical_trampoline.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

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

SinkResultType PhysicalTrampoline::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	// For the first step I like to see that the right input is generated.
	auto &gstate = input.global_state.Cast<RecursiveCTEState>();
	lock_guard<mutex> guard(gstate.intermediate_table_lock);
	if (!union_all) {
		idx_t match_count = ProbeHT(chunk, gstate);
		if (match_count > 0) {
			gstate.intermediate_table.Append(chunk);
		}
	} else {
		gstate.intermediate_table.Append(chunk);
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
			Pipeline &new_pipe = recursive_meta_pipeline->CreatePipeline();
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
	auto &gstate = sink_state->Cast<RecursiveCTEState>();
	if (!gstate.initialized) {
		gstate.intermediate_table.InitializeScan(gstate.scan_state);
		gstate.finished_scan = false;
		gstate.initialized = true;
	}
	while (chunk.size() == 0) {
		if (!gstate.finished_scan) {
			// scan any chunks we have collected so far
			gstate.intermediate_table.Scan(gstate.scan_state, chunk);
			if (chunk.size() == 0) {
				gstate.finished_scan = true;
			} else {
				break;
			}
		} else {
			// we have run out of chunks
			// now we need to recurse
			// we set up the working table as the data we gathered in this iteration of the recursion
			working_table->Reset();
			working_table->Combine(gstate.intermediate_table);
			// and we clear the intermediate table
			gstate.finished_scan = false;
			gstate.intermediate_table.Reset();
			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (gstate.intermediate_table.Count() == 0) {
				gstate.finished_scan = true;
				break;
			}
			// set up the scan again
			gstate.intermediate_table.InitializeScan(gstate.scan_state);
		}
	}
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

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
