#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include <iostream>

namespace duckdb {

unique_ptr<BoundQueryNode> Binder::BindNode(RecursiveCTENode &statement) {
	auto result = make_uniq<BoundRecursiveCTENode>();

	// first recursively visit the recursive CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.left);
	D_ASSERT(statement.right);
	if (statement.union_all && !statement.key_targets.empty()) {
		throw BinderException("UNION ALL cannot be used with USING KEY in recursive CTE.");
	}

	result->ctename = statement.ctename;
	result->union_all = statement.union_all;
	result->setop_index = GenerateTableIndex();

	result->left_binder = Binder::CreateBinder(context, this);
	result->left = result->left_binder->BindNode(*statement.left);

	// the result types of the CTE are the types of the LHS
	result->types = result->left->types;
	result->internal_types = result->left->types;
	// BTODO: only here until i know how to fix ResolveTypes()
	result->return_types = result->types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result->names = result->left->names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result->names.size(); i++) {
		result->names[i] = statement.aliases[i];
	}

	// This allows the right side to reference the CTE recursively
	bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->names, result->types);



	// Create temporary binder to bind expressions
	auto bin = Binder::CreateBinder(context, nullptr);
	ErrorData error;
	FunctionBinder function_binder(*bin);
	bin->bind_context.AddGenericBinding(result->setop_index, statement.ctename, result->names, result->types);
	ExpressionBinder expression_binder(*bin, context);

	// Set contains column indices that are already bound
	unordered_set<idx_t> column_references;

	// Bind specified keys to the referenced column
	for (unique_ptr<ParsedExpression> &expr : statement.key_targets) {
		auto bound_expr = expression_binder.Bind(expr);
		D_ASSERT(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
		auto &bound_ref = bound_expr->Cast<BoundColumnRefExpression>();

		idx_t column_index = bound_ref.binding.column_index;
		if (column_references.find(column_index) != column_references.end()) {
			throw BinderException(bound_ref.GetQueryLocation(),
			                      "Column '%s' referenced multiple times in recursive CTE aggregates",
			                      result->names[column_index]);
		}

		column_references.insert(column_index);
		result->key_targets.push_back(std::move(bound_expr));
	}

	// Bind user-defined aggregates
	for (auto &expr : statement.payload_aggregates) {
		D_ASSERT(expr->type == ExpressionType::FUNCTION);
		auto &func_expr = expr->Cast<FunctionExpression>();

		// Look up the aggregate function in the catalog
		auto &func = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA,
																								func_expr.function_name);
		vector<LogicalType> aggregation_input_types;
		vector<unique_ptr<Expression>> bound_children;
		// Bind the children of the aggregate function and check if they are valid column references
		for (auto& child : func_expr.children) {
			auto bound_child = expression_binder.Bind(child);
			if (bound_child->type != ExpressionType::BOUND_COLUMN_REF) {
				throw BinderException(*bound_child, "Aggregate function argument must be a column reference");
			}
			aggregation_input_types.push_back(bound_child->return_type);
			bound_children.push_back(std::move(bound_child));
		}

		// Find the best matching aggregate function
		auto best_function_idx = function_binder.BindFunction(func.name, func.functions,
		                                                      std::move(aggregation_input_types),
		                                                      error);
		if (!best_function_idx.IsValid()) {
			throw BinderException("No matching aggregate function\n%s", error.Message());
		}
		// Found a matching function, bind it as an aggregate
		auto best_function = func.functions.GetFunctionByOffset(best_function_idx.GetIndex());
		auto aggregate = function_binder.BindAggregateFunction(std::move(best_function), std::move(bound_children),
		                                                       nullptr, AggregateType::NON_DISTINCT);

		idx_t aggregate_idx = aggregate->children[0]->Cast<BoundColumnRefExpression>().binding.column_index;

		if (column_references.find(aggregate_idx) != column_references.end()) {
			throw BinderException(aggregate->children[0]->GetQueryLocation(),
			                      "Column '%s' referenced multiple times in recursive CTE aggregates",
			                      result->names[aggregate_idx]);
		}

		// BTODO: only here until i know how to fix ResolveTypes()
		result->return_types[aggregate_idx] = aggregate->return_type;
		result->payload_aggregates.push_back(std::move(aggregate));
		column_references.insert(aggregate_idx);
	}

	// If we have key targets, then all the other columns must be aggregated
	if (!result->key_targets.empty()) {
		// Bind every column that is neither referenced as a key nor by an aggregate to a LAST aggregate
		for (idx_t i = 0; i < result->left->types.size(); i++) {
			if (column_references.find(i) == column_references.end()) {
				// Create a new bound column reference for the missing columns
				vector<unique_ptr<Expression>> first_children;
				auto bound =
				    make_uniq<BoundColumnRefExpression>(result->types[i], ColumnBinding(result->setop_index, i));
				first_children.push_back(std::move(bound));

				// Create a last aggregate for the newly bound column reference
				auto first_aggregate = function_binder.BindAggregateFunction(
				    LastFunctionGetter::GetFunction(result->types[i]), std::move(first_children), nullptr,
				    AggregateType::NON_DISTINCT);

				result->payload_aggregates.push_back(std::move(first_aggregate));
			}
		}
	}

	// BTODO: only here until i know how to fix ResolveTypes()
	// Now that we have finished binding all aggregates, we can update the operator types
	result->types = result->return_types;

	result->right_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	// If there is already a binding for the CTE, we need to remove it first
	// as we are binding a CTE currently, we take precedence over the existing binding.
	// This implements the CTE shadowing behavior.
	result->right_binder->bind_context.RemoveCTEBinding(statement.ctename);
	result->right_binder->bind_context.AddCTEBinding(result->setop_index, statement.ctename, result->names,
	                                                 result->internal_types, result->types, !statement.key_targets.empty());

	result->right = result->right_binder->BindNode(*statement.right);
	for (auto &c : result->left_binder->correlated_columns) {
		result->right_binder->AddCorrelatedColumn(c);
	}

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*result->left_binder);
	MoveCorrelatedExpressions(*result->right_binder);

	// now both sides have been bound we can resolve types
	if (result->left->types.size() != result->right->types.size()) {
		throw BinderException("Set operations can only apply to expressions with the "
		                      "same number of result columns");
	}

	if (!statement.modifiers.empty()) {
		throw NotImplementedException("FIXME: bind modifiers in recursive CTE");
	}

	return std::move(result);
}

} // namespace duckdb
