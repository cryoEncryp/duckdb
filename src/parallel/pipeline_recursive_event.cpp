#include "duckdb/parallel/pipeline_recursive_event.hpp"
#include "iostream"

namespace duckdb {

class PipelineRecursiveTask : public ExecutorTask {
public:
	PipelineRecursiveTask(Pipeline &pipeline, shared_ptr<Event> event_p)
	    : ExecutorTask(pipeline.executor, std::move(event_p)) {
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

};

TaskExecutionResult PipelineRecursiveTask::ExecuteTask(TaskExecutionMode mode) {
	// parent event which should reschedule the child event
	auto& current_event = event->Cast<PipelineRecursiveEvent>();

	// BTODO: change to real condition
	// only reschedule if the breaking condition is yet not met (intermediate table are empty).
	if (current_event.count++ < 2) {
		current_event.child_event->Reset();
		// BTODO: change to normal reset, for now we have an ugly insertion
		current_event.ResetParent();

		current_event.child_event->Schedule();
	} else {
		// BTODO: change to normal reset, for now we have an ugly insertion and dependency are set not right
		current_event.ResetParent();
		// finish task only if our breaking condition is met
		event->FinishTask();
	}
	return TaskExecutionResult::TASK_FINISHED;
}

PipelineRecursiveEvent::PipelineRecursiveEvent(shared_ptr<Pipeline> pipeline_p, shared_ptr<Event> child_event)
    : BasePipelineEvent(pipeline_p), pipeline(pipeline_p), child_event(child_event) {
}

PipelineRecursiveEvent::PipelineRecursiveEvent(Pipeline &pipeline_p, shared_ptr<Event> child_event)
    : BasePipelineEvent(pipeline_p), pipeline(pipeline_p.shared_from_this()), child_event(child_event) {
}

void PipelineRecursiveEvent::Schedule() {
	std::cout << "Schedule recursive Event\n";
	// BTODO: create one task which should be executed every iteration
	auto task = make_shared_ptr<PipelineRecursiveTask>(*pipeline, shared_from_this());
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(task);
	SetTasks(tasks);
}

void PipelineRecursiveEvent::FinishEvent() {
	std::cout << "Recursive event ist finished\n";
}

/* BTODO: Remove
 * Recursive pipeline event that should be rescheduled until the condition in the parent is met.
 * only temporary, should be replaced with real PipelineEvent
 */

class PipelineRecursiveChildTask : public ExecutorTask {
public:
	    PipelineRecursiveChildTask(Pipeline &pipeline, shared_ptr<Event> event_p)
	    : ExecutorTask(pipeline.executor, std::move(event_p)) {}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;
};

TaskExecutionResult PipelineRecursiveChildTask::ExecuteTask(TaskExecutionMode mode) {
	std::cout << "\t Child Task was executed.\n";
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

PipelineRecursiveChildEvent::PipelineRecursiveChildEvent(shared_ptr<Pipeline> pipeline) :
      BasePipelineEvent(pipeline), pipeline(pipeline){

}

PipelineRecursiveChildEvent::PipelineRecursiveChildEvent(Pipeline& pipeline) :
      BasePipelineEvent(pipeline), pipeline(pipeline.shared_from_this()){}

void PipelineRecursiveChildEvent::Schedule() {
	std::cout << "\t Schedule recursive child event\n";
	shared_ptr<PipelineRecursiveChildTask> recursive_task = make_shared_ptr<PipelineRecursiveChildTask>(*pipeline, shared_from_this());
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(recursive_task);
	SetTasks(tasks);
}

void PipelineRecursiveChildEvent::FinishEvent() {
	std::cout << "\t Recursive child event is finished\n";
}

} // namespace duckdb
