#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

unique_ptr<BoundQueryNode> Binder::BindNode(RecursiveCTENode &statement) {
	auto result = make_uniq<BoundRecursiveCTENode>();

	// first recursively visit the recursive CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.left);
	D_ASSERT(statement.trampolines.size() >= 1);

	result->ctename = statement.ctename;
	result->union_all = statement.union_all;
	result->setop_index = GenerateTableIndex();

	result->left_binder = Binder::CreateBinder(context, this);
	result->left = result->left_binder->BindNode(*statement.left);

	// the result types of the CTE are the types of the LHS
	result->types = result->left->types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result->names = result->left->names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result->names.size(); i++) {
		result->names[i] = statement.aliases[i];
	}

	// This allows the right side to reference the CTE recursively
	bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->names, result->types);

	// Binding all branches
	for (size_t child_index = 0; child_index < statement.trampolines.size(); child_index++) {
		auto binder = Binder::CreateBinder(context, this);
		// Add bindings of left side to temporary CTE bindings context
		binder->bind_context.AddCTEBinding(result->setop_index, statement.ctename, result->names, result->types);
		result->trampolines.emplace_back(binder->BindNode(*statement.trampolines[child_index]));
		result->trampoline_binder.emplace_back(binder);
	}

	// OPTIMIZE: How are we processing correlation
	for (auto &c : result->left_binder->correlated_columns) {
		for (auto &branch_binder : result->trampoline_binder) {
			branch_binder->AddCorrelatedColumn(c);
		}
	}

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*result->left_binder);
	for (auto &branch_binder : result->trampoline_binder) {
		MoveCorrelatedExpressions(*branch_binder);
	}

	// now both sides have been bound we can resolve types
	for (auto &branch : result->trampolines) {
		if (result->left->types.size() != branch->types.size()) {
			throw BinderException("Set operations can only apply to expressions with the "
			                      "same number of result columns");
		}
	}

	if (!statement.modifiers.empty()) {
		throw NotImplementedException("FIXME: bind modifiers in recursive CTE");
	}

	return std::move(result);
}

} // namespace duckdb
