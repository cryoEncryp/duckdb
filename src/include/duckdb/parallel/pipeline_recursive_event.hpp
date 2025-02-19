//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_recursive_event.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {
class PipelineRecursiveTask;
class PipelineRecursiveEvent : public BasePipelineEvent {
public:
	explicit PipelineRecursiveEvent(shared_ptr<Pipeline> pipeline, shared_ptr<Event> child_event);
	explicit PipelineRecursiveEvent(Pipeline& pipeline, shared_ptr<Event> child_event);

	void PrintPipeline() override {
		pipeline->Print();
	}

	//! The pipeline that this event belongs to
	shared_ptr<Pipeline> pipeline;

public:
	//! Schedule a new task that add new dependency to this event and schedule this event again
	void Schedule() override;
	void FinishEvent() override;
	shared_ptr<Event> child_event;

	// BTODO: Remove only for testing
	int count = 0;
};

class PipelineRecursiveChildEvent : public BasePipelineEvent {
public:
	explicit PipelineRecursiveChildEvent(shared_ptr<Pipeline> pipeline);
	explicit PipelineRecursiveChildEvent(Pipeline& pipeline);


	void PrintPipeline() override {
		pipeline->Print();
	}

	//! The pipeline that this event belongs to
	shared_ptr<Pipeline> pipeline;
public:
	void Schedule() override;
	void FinishEvent() override;
};
}