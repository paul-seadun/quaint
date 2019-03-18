var N = null;var searchIndex = {};
searchIndex["prisma_query"]={"doc":"prisma-query","items":[[0,"ast","prisma_query","An abstract syntax tree for SQL queries.",N,N],[3,"Column","prisma_query::ast","A column definition.",N,N],[12,"name","","",0,N],[12,"table","","",0,N],[12,"alias","","",0,N],[12,"type_identifier","","",0,N],[3,"Over","","",N,N],[12,"ordering","","",1,N],[12,"partitioning","","",1,N],[3,"RowNumber","","",N,N],[12,"over","","",2,N],[3,"Count","","",N,N],[12,"exprs","","",3,N],[3,"Distinct","","",N,N],[12,"exprs","","",4,N],[3,"Function","","A database function definition",N,N],[12,"typ_","","",5,N],[12,"alias","","",5,N],[3,"Insert","","A builder for an `INSERT` statement.",N,N],[12,"table","","",6,N],[12,"values","","",6,N],[12,"returning","","",6,N],[3,"JoinData","","The `JOIN` table and conditions.",N,N],[12,"table","","",7,N],[12,"conditions","","",7,N],[3,"Ordering","","A list of definitions for the `ORDER BY` statement",N,N],[12,"0","","",8,N],[3,"Row","","A collection of values surrounded by parentheses.",N,N],[12,"values","","",9,N],[3,"Select","","A builder for a `SELECT` statement.",N,N],[12,"table","","",10,N],[12,"columns","","",10,N],[12,"conditions","","",10,N],[12,"ordering","","",10,N],[12,"limit","","",10,N],[12,"offset","","",10,N],[12,"joins","","",10,N],[3,"Table","","A table definition",N,N],[12,"typ","","",11,N],[12,"alias","","",11,N],[12,"database","","",11,N],[4,"Compare","","For modeling comparison expression",N,N],[13,"Equals","","`left = right`",12,N],[13,"NotEquals","","`left <> right`",12,N],[13,"LessThan","","`left < right`",12,N],[13,"LessThanOrEquals","","`left <= right`",12,N],[13,"GreaterThan","","`left > right`",12,N],[13,"GreaterThanOrEquals","","`left >= right`",12,N],[13,"In","","`left IN (..)`",12,N],[13,"NotIn","","`left NOT IN (..)`",12,N],[13,"Like","","`left LIKE %..%`",12,N],[13,"NotLike","","`left NOT LIKE %..%`",12,N],[13,"BeginsWith","","`left LIKE ..%`",12,N],[13,"NotBeginsWith","","`left NOT LIKE ..%`",12,N],[13,"EndsInto","","`left LIKE %..`",12,N],[13,"NotEndsInto","","`left NOT LIKE %..`",12,N],[13,"Null","","`value IS NULL`",12,N],[13,"NotNull","","`value IS NOT NULL`",12,N],[13,"Between","","`value` BETWEEN `left` AND `right`",12,N],[13,"NotBetween","","`value` NOT BETWEEN `left` AND `right`",12,N],[4,"ConditionTree","","Tree structures and leaves for condition building.",N,N],[13,"And","","`(left_expression AND right_expression)`",13,N],[13,"Or","","`(left_expression OR right_expression)`",13,N],[13,"Not","","`(NOT expression)`",13,N],[13,"Single","","A single expression leaf",13,N],[13,"NoCondition","","A leaf that does nothing to the condition, `1=1`",13,N],[13,"NegativeCondition","","A leaf that cancels the condition, `1=0`",13,N],[4,"Expression","","A database expression.",N,N],[13,"ConditionTree","","A tree of expressions to evaluate from the deepest value to up",14,N],[13,"Compare","","A comparison expression",14,N],[13,"Value","","A single value, column, row or a nested select",14,N],[4,"FunctionType","","A database function type",N,N],[13,"RowNumber","","",15,N],[13,"Count","","",15,N],[13,"Distinct","","",15,N],[4,"Join","","A representation of a `JOIN` statement.",N,N],[13,"Inner","","Implements an `INNER JOIN` with given `JoinData`.",16,N],[13,"LeftOuter","","Implements an `LEFT OUTER JOIN` with given `JoinData`.",16,N],[4,"Order","","The ordering direction",N,N],[13,"Asc","","Ascending",17,N],[13,"Desc","","Descending",17,N],[4,"Query","","A database query",N,N],[13,"Select","","Query for fetching data. E.g. the `SELECT` query.",18,N],[13,"Insert","","",18,N],[4,"TableType","","",N,N],[13,"Table","","",19,N],[13,"Query","","",19,N],[4,"DatabaseValue","","A value we can compare and use in database queries.",N,N],[13,"Parameterized","","Anything that we must parameterize before querying",20,N],[13,"Column","","A database column",20,N],[13,"Row","","Data in a row form, e.g. (1, 2, 3)",20,N],[13,"Select","","A nested `SELECT` statement",20,N],[13,"Function","","A database function call",20,N],[13,"Asterisk","","A qualified asterisk to a table",20,N],[4,"ParameterizedValue","","A value we must parameterize for the prepared statement.",N,N],[13,"Null","","A database null",21,N],[13,"Integer","","An integer value",21,N],[13,"Real","","A floating point value",21,N],[13,"Text","","A string value",21,N],[13,"Boolean","","a boolean value",21,N],[5,"row_number","","A number from 1 to n in specified order",N,[[],["rownumber"]]],[5,"count","","Count of the underlying table where the given expression is not null.",N,[[["t"]],["count"]]],[5,"distinct","","Select distinct rows by given expressions.",N,[[["t"]],["distinct"]]],[5,"asterisk","","A quick alias to create an asterisk to a table.",N,[[],["databasevalue"]]],[11,"new","","Create a column definition.",0,[[["s"]],["self"]]],[11,"table","","Include the table name in the column expression.",0,[[["self"],["table"]],["self"]]],[11,"alias","","Give the column an alias in the query.",0,[[["self"],["s"]],["self"]]],[11,"and","","An `AND` statement, is true when both sides are true.",13,[[["e"],["j"]],["conditiontree"]]],[11,"or","","An `OR` statement, is true when one side is true.",13,[[["e"],["j"]],["conditiontree"]]],[11,"not","","A `NOT` statement, is true when the expression is false.",13,[[["e"]],["conditiontree"]]],[11,"single","","A single leaf, is true when the expression is true.",13,[[["e"]],["conditiontree"]]],[11,"invert_if","","Inverts the entire condition tree if condition is met.",13,[[["self"],["bool"]],["conditiontree"]]],[11,"is_empty","","",1,[[["self"]],["bool"]]],[11,"order_by","","Define the order of the row number. Is the row order if not set.",2,[[["self"],["t"]],["self"]]],[11,"partition_by","","Define the partitioning of the row number",2,[[["self"],["t"]],["self"]]],[11,"distinct","","Add another expression to a distinct statement",4,[[["self"],["t"]],["distinct"]]],[11,"alias","","Give the function an alias in the query.",5,[[["self"],["s"]],["self"]]],[11,"into","","Creates a new `INSERT` statement for the given table.",6,[[["t"]],["self"]]],[11,"value","","Adds a new value to the `INSERT` statement",6,[[["self"],["k"],["v"]],["self"]]],[11,"returning","","Define the column to be returned from the newly inserted row. ```rust # use prisma_query::{ast::*, visitor::{Visitor, Sqlite}}; let query = Insert::into(\"users\").returning(\"id\"); let (sql, _) = Sqlite::build(query);",6,[[["self"],["t"]],["self"]]],[11,"new","","",8,[[["vec",["orderdefinition"]]],["self"]]],[11,"is_empty","","",8,[[["self"]],["bool"]]],[11,"new","","",9,[[],["self"]]],[11,"push","","",9,[[["self"],["t"]],["self"]]],[11,"from","","Creates a new `SELECT` statement for the given table.",10,[[["t"]],["self"]]],[11,"value","","Selects a static value as the column.",10,[[["self"],["t"]],["self"]]],[11,"column","","Adds a column to be selected.",10,[[["self"],["t"]],["self"]]],[11,"columns","","A bulk method to select multiple values.",10,[[["self"],["vec"]],["self"]]],[11,"so_that","","Adds `WHERE` conditions to the query. See Comparable for more examples.",10,[[["self"],["t"]],["self"]]],[11,"inner_join","","Adds `INNER JOIN` clause to the query.",10,[[["self"],["j"]],["self"]]],[11,"left_outer_join","","Adds `LEFT OUTER JOIN` clause to the query.",10,[[["self"],["j"]],["self"]]],[11,"order_by","","Adds an ordering to the `ORDER BY` section.",10,[[["self"],["t"]],["self"]]],[11,"limit","","Sets the `LIMIT` value.",10,[[["self"],["usize"]],["self"]]],[11,"offset","","Sets the `OFFSET` value.",10,[[["self"],["usize"]],["self"]]],[11,"database","","Define in which database the table is located",11,[[["self"],["t"]],["self"]]],[11,"asterisk","","A qualified asterisk to this table",11,[[["self"]],["databasevalue"]]],[6,"OrderDefinition","","",N,N],[8,"Comparable","","An item that can be compared against other values in the database.",N,N],[10,"equals","","Tests if both sides are the same value.",22,[[["self"],["t"]],["compare"]]],[10,"not_equals","","Tests if both sides are not the same value.",22,[[["self"],["t"]],["compare"]]],[10,"less_than","","Tests if the left side is smaller than the right side.",22,[[["self"],["t"]],["compare"]]],[10,"less_than_or_equals","","Tests if the left side is smaller than the right side or the same.",22,[[["self"],["t"]],["compare"]]],[10,"greater_than","","Tests if the left side is bigger than the right side.",22,[[["self"],["t"]],["compare"]]],[10,"greater_than_or_equals","","Tests if the left side is bigger than the right side or the same.",22,[[["self"],["t"]],["compare"]]],[10,"in_selection","","Tests if the left side is included in the right side collection.",22,[[["self"],["t"]],["compare"]]],[10,"not_in_selection","","Tests if the left side is not included in the right side collection.",22,[[["self"],["t"]],["compare"]]],[10,"like","","Tests if the left side includes the right side string.",22,[[["self"],["t"]],["compare"]]],[10,"not_like","","Tests if the left side does not include the right side string.",22,[[["self"],["t"]],["compare"]]],[10,"begins_with","","Tests if the left side starts with the right side string.",22,[[["self"],["t"]],["compare"]]],[10,"not_begins_with","","Tests if the left side doesn't start with the right side string.",22,[[["self"],["t"]],["compare"]]],[10,"ends_into","","Tests if the left side ends into the right side string.",22,[[["self"],["t"]],["compare"]]],[10,"not_ends_into","","Tests if the left side does not end into the right side string.",22,[[["self"],["t"]],["compare"]]],[10,"is_null","","Tests if the left side is `NULL`.",22,[[["self"]],["compare"]]],[10,"is_not_null","","Tests if the left side is not `NULL`.",22,[[["self"]],["compare"]]],[10,"between","","Tests if the value is between two given values.",22,[[["self"],["t"],["v"]],["compare"]]],[10,"not_between","","Tests if the value is not between two given values.",22,[[["self"],["t"],["v"]],["compare"]]],[8,"Conjuctive","","`AND`, `OR` and `NOT` conjuctive implementations.",N,N],[10,"and","","Builds an `AND` condition having `self` as the left leaf and `other` as the right.",23,[[["self"],["e"]],["conditiontree"]]],[10,"or","","Builds an `OR` condition having `self` as the left leaf and `other` as the right.",23,[[["self"],["e"]],["conditiontree"]]],[10,"not","","Builds a `NOT` condition having `self` as the condition.",23,[[["self"]],["conditiontree"]]],[8,"Joinable","","An item that can be joined.",N,N],[10,"on","","Add the `JOIN` conditions.",24,[[["self"],["t"]],["joindata"]]],[8,"IntoOrderDefinition","","Convert the value into an order definition with order item and direction",N,N],[10,"into_order_definition","","",25,[[["self"]],["orderdefinition"]]],[8,"Orderable","","An item that can be used in the `ORDER BY` statement",N,N],[10,"order","","Order by `self` in the given order",26,[[["self"],["option",["order"]]],["orderdefinition"]]],[11,"ascend","","Change the order to `ASC`",26,[[["self"]],["orderdefinition"]]],[11,"descend","","Change the order to `DESC`",26,[[["self"]],["orderdefinition"]]],[8,"Aliasable","","An object that can be aliased.",N,N],[10,"alias","","Alias table for usage elsewhere in the query.",27,[[["self"],["t"]],["table"]]],[0,"visitor","prisma_query","Visitors for reading an abstract SQL syntax tree, generating the query and gathering parameters in the right order.",N,N],[3,"Sqlite","prisma_query::visitor","A visitor for generating queries for an SQLite database. Requires that `rusqlite` feature flag is selected.",N,N],[8,"Visitor","","A function travelling through the query AST, building the final query string and gathering parameters sent to the database together with the query.",N,N],[18,"C_PARAM","","Parameter character when parameterizing values in the query.",28,N],[18,"C_BACKTICK","","Backtick character to surround identifiers, such as column and table names.",28,N],[18,"C_WILDCARD","","Wildcard character to be used in `LIKE` queries.",28,N],[10,"build","","Convert the given `Query` to an SQL string and a vector of parameters. When certain parameters are replaced with the `C_PARAM` character in the query, the vector should contain the parameter value in the right position.",28,N],[10,"add_parameter","","When called, the visitor decided to not render the parameter into the query, replacing it with the `C_PARAM`, calling `add_parameter` with the replaced value.",28,[[["self"],["parameterizedvalue"]]]],[10,"visit_limit","","The `LIMIT` statement in the query",28,[[["self"],["option",["usize"]]],["string"]]],[10,"visit_offset","","The `OFFSET` statement in the query",28,[[["self"],["usize"]],["string"]]],[10,"visit_function","","A database function.",28,[[["self"],["function"]],["string"]]],[10,"visit_partitioning","","A partitioning statement.",28,[[["self"],["over"]],["string"]]],[11,"visit_joins","","The join statements in the query",28,[[["self"],["vec",["join"]]],["string"]]],[11,"visit_join_data","","",28,[[["self"],["joindata"]],["string"]]],[11,"visit_select","","A walk through a `SELECT` statement",28,[[["self"],["select"]],["string"]]],[11,"visit_insert","","",28,[[["self"],["insert"]],["string"]]],[11,"delimited_identifiers","","A helper for delimiting an identifier, surrounding every part with `C_BACKTICK` and delimiting the values with a `.`",28,[[["vec",["string"]]],["string"]]],[11,"visit_query","","A walk through a complete `Query` statement",28,[[["self"],["query"]],["string"]]],[11,"visit_columns","","The selected columns",28,[[["self"],["vec",["databasevalue"]]],["string"]]],[11,"visit_parameterized","","A visit to a value we parameterize and replace with a ?",28,[[["self"],["parameterizedvalue"]],["string"]]],[11,"visit_database_value","","A visit to a value used in an expression",28,[[["self"],["databasevalue"]],["string"]]],[11,"visit_table","","A database table identifier",28,[[["self"],["table"],["bool"]],["string"]]],[11,"visit_column","","A database column identifier",28,[[["self"],["column"]],["string"]]],[11,"visit_row","","A row of data used as an expression",28,[[["self"],["row"]],["string"]]],[11,"visit_conditions","","A walk through the query conditions",28,[[["self"],["conditiontree"]],["string"]]],[11,"visit_expression","","An expression that can either be a single value, a set of conditions or a comparison call",28,[[["self"],["expression"]],["string"]]],[11,"visit_compare","","A comparison expression",28,[[["self"],["compare"]],["string"]]],[11,"visit_ordering","","A visit in the `ORDER BY` section of the query",28,[[["self"],["ordering"]],["string"]]],[11,"into","prisma_query::ast","",0,[[["self"]],["u"]]],[11,"to_owned","","",0,[[["self"]],["t"]]],[11,"clone_into","","",0,N],[11,"from","","",0,[[["t"]],["t"]]],[11,"try_from","","",0,[[["u"]],["result"]]],[11,"borrow","","",0,[[["self"]],["t"]]],[11,"get_type_id","","",0,[[["self"]],["typeid"]]],[11,"try_into","","",0,[[["self"]],["result"]]],[11,"borrow_mut","","",0,[[["self"]],["t"]]],[11,"into","","",1,[[["self"]],["u"]]],[11,"to_owned","","",1,[[["self"]],["t"]]],[11,"clone_into","","",1,N],[11,"from","","",1,[[["t"]],["t"]]],[11,"try_from","","",1,[[["u"]],["result"]]],[11,"borrow","","",1,[[["self"]],["t"]]],[11,"get_type_id","","",1,[[["self"]],["typeid"]]],[11,"try_into","","",1,[[["self"]],["result"]]],[11,"borrow_mut","","",1,[[["self"]],["t"]]],[11,"into","","",2,[[["self"]],["u"]]],[11,"to_owned","","",2,[[["self"]],["t"]]],[11,"clone_into","","",2,N],[11,"from","","",2,[[["t"]],["t"]]],[11,"try_from","","",2,[[["u"]],["result"]]],[11,"borrow","","",2,[[["self"]],["t"]]],[11,"get_type_id","","",2,[[["self"]],["typeid"]]],[11,"try_into","","",2,[[["self"]],["result"]]],[11,"borrow_mut","","",2,[[["self"]],["t"]]],[11,"into","","",3,[[["self"]],["u"]]],[11,"to_owned","","",3,[[["self"]],["t"]]],[11,"clone_into","","",3,N],[11,"from","","",3,[[["t"]],["t"]]],[11,"try_from","","",3,[[["u"]],["result"]]],[11,"borrow","","",3,[[["self"]],["t"]]],[11,"get_type_id","","",3,[[["self"]],["typeid"]]],[11,"try_into","","",3,[[["self"]],["result"]]],[11,"borrow_mut","","",3,[[["self"]],["t"]]],[11,"into","","",4,[[["self"]],["u"]]],[11,"to_owned","","",4,[[["self"]],["t"]]],[11,"clone_into","","",4,N],[11,"from","","",4,[[["t"]],["t"]]],[11,"try_from","","",4,[[["u"]],["result"]]],[11,"borrow","","",4,[[["self"]],["t"]]],[11,"get_type_id","","",4,[[["self"]],["typeid"]]],[11,"try_into","","",4,[[["self"]],["result"]]],[11,"borrow_mut","","",4,[[["self"]],["t"]]],[11,"into","","",5,[[["self"]],["u"]]],[11,"to_owned","","",5,[[["self"]],["t"]]],[11,"clone_into","","",5,N],[11,"from","","",5,[[["t"]],["t"]]],[11,"try_from","","",5,[[["u"]],["result"]]],[11,"borrow","","",5,[[["self"]],["t"]]],[11,"get_type_id","","",5,[[["self"]],["typeid"]]],[11,"try_into","","",5,[[["self"]],["result"]]],[11,"borrow_mut","","",5,[[["self"]],["t"]]],[11,"into","","",6,[[["self"]],["u"]]],[11,"to_owned","","",6,[[["self"]],["t"]]],[11,"clone_into","","",6,N],[11,"from","","",6,[[["t"]],["t"]]],[11,"try_from","","",6,[[["u"]],["result"]]],[11,"borrow","","",6,[[["self"]],["t"]]],[11,"get_type_id","","",6,[[["self"]],["typeid"]]],[11,"try_into","","",6,[[["self"]],["result"]]],[11,"borrow_mut","","",6,[[["self"]],["t"]]],[11,"into","","",7,[[["self"]],["u"]]],[11,"to_owned","","",7,[[["self"]],["t"]]],[11,"clone_into","","",7,N],[11,"from","","",7,[[["t"]],["t"]]],[11,"try_from","","",7,[[["u"]],["result"]]],[11,"borrow","","",7,[[["self"]],["t"]]],[11,"get_type_id","","",7,[[["self"]],["typeid"]]],[11,"try_into","","",7,[[["self"]],["result"]]],[11,"borrow_mut","","",7,[[["self"]],["t"]]],[11,"into","","",8,[[["self"]],["u"]]],[11,"to_owned","","",8,[[["self"]],["t"]]],[11,"clone_into","","",8,N],[11,"from","","",8,[[["t"]],["t"]]],[11,"try_from","","",8,[[["u"]],["result"]]],[11,"borrow","","",8,[[["self"]],["t"]]],[11,"get_type_id","","",8,[[["self"]],["typeid"]]],[11,"try_into","","",8,[[["self"]],["result"]]],[11,"borrow_mut","","",8,[[["self"]],["t"]]],[11,"into","","",9,[[["self"]],["u"]]],[11,"to_owned","","",9,[[["self"]],["t"]]],[11,"clone_into","","",9,N],[11,"from","","",9,[[["t"]],["t"]]],[11,"try_from","","",9,[[["u"]],["result"]]],[11,"borrow","","",9,[[["self"]],["t"]]],[11,"get_type_id","","",9,[[["self"]],["typeid"]]],[11,"try_into","","",9,[[["self"]],["result"]]],[11,"borrow_mut","","",9,[[["self"]],["t"]]],[11,"into","","",10,[[["self"]],["u"]]],[11,"to_owned","","",10,[[["self"]],["t"]]],[11,"clone_into","","",10,N],[11,"from","","",10,[[["t"]],["t"]]],[11,"try_from","","",10,[[["u"]],["result"]]],[11,"borrow","","",10,[[["self"]],["t"]]],[11,"get_type_id","","",10,[[["self"]],["typeid"]]],[11,"try_into","","",10,[[["self"]],["result"]]],[11,"borrow_mut","","",10,[[["self"]],["t"]]],[11,"into","","",11,[[["self"]],["u"]]],[11,"to_owned","","",11,[[["self"]],["t"]]],[11,"clone_into","","",11,N],[11,"from","","",11,[[["t"]],["t"]]],[11,"try_from","","",11,[[["u"]],["result"]]],[11,"borrow","","",11,[[["self"]],["t"]]],[11,"get_type_id","","",11,[[["self"]],["typeid"]]],[11,"try_into","","",11,[[["self"]],["result"]]],[11,"borrow_mut","","",11,[[["self"]],["t"]]],[11,"into","","",12,[[["self"]],["u"]]],[11,"to_owned","","",12,[[["self"]],["t"]]],[11,"clone_into","","",12,N],[11,"from","","",12,[[["t"]],["t"]]],[11,"try_from","","",12,[[["u"]],["result"]]],[11,"borrow","","",12,[[["self"]],["t"]]],[11,"get_type_id","","",12,[[["self"]],["typeid"]]],[11,"try_into","","",12,[[["self"]],["result"]]],[11,"borrow_mut","","",12,[[["self"]],["t"]]],[11,"into","","",13,[[["self"]],["u"]]],[11,"to_owned","","",13,[[["self"]],["t"]]],[11,"clone_into","","",13,N],[11,"from","","",13,[[["t"]],["t"]]],[11,"try_from","","",13,[[["u"]],["result"]]],[11,"borrow","","",13,[[["self"]],["t"]]],[11,"get_type_id","","",13,[[["self"]],["typeid"]]],[11,"try_into","","",13,[[["self"]],["result"]]],[11,"borrow_mut","","",13,[[["self"]],["t"]]],[11,"into","","",14,[[["self"]],["u"]]],[11,"to_owned","","",14,[[["self"]],["t"]]],[11,"clone_into","","",14,N],[11,"from","","",14,[[["t"]],["t"]]],[11,"try_from","","",14,[[["u"]],["result"]]],[11,"borrow","","",14,[[["self"]],["t"]]],[11,"get_type_id","","",14,[[["self"]],["typeid"]]],[11,"try_into","","",14,[[["self"]],["result"]]],[11,"borrow_mut","","",14,[[["self"]],["t"]]],[11,"into","","",15,[[["self"]],["u"]]],[11,"to_owned","","",15,[[["self"]],["t"]]],[11,"clone_into","","",15,N],[11,"from","","",15,[[["t"]],["t"]]],[11,"try_from","","",15,[[["u"]],["result"]]],[11,"borrow","","",15,[[["self"]],["t"]]],[11,"get_type_id","","",15,[[["self"]],["typeid"]]],[11,"try_into","","",15,[[["self"]],["result"]]],[11,"borrow_mut","","",15,[[["self"]],["t"]]],[11,"into","","",16,[[["self"]],["u"]]],[11,"to_owned","","",16,[[["self"]],["t"]]],[11,"clone_into","","",16,N],[11,"from","","",16,[[["t"]],["t"]]],[11,"try_from","","",16,[[["u"]],["result"]]],[11,"borrow","","",16,[[["self"]],["t"]]],[11,"get_type_id","","",16,[[["self"]],["typeid"]]],[11,"try_into","","",16,[[["self"]],["result"]]],[11,"borrow_mut","","",16,[[["self"]],["t"]]],[11,"into","","",17,[[["self"]],["u"]]],[11,"to_owned","","",17,[[["self"]],["t"]]],[11,"clone_into","","",17,N],[11,"from","","",17,[[["t"]],["t"]]],[11,"try_from","","",17,[[["u"]],["result"]]],[11,"borrow","","",17,[[["self"]],["t"]]],[11,"get_type_id","","",17,[[["self"]],["typeid"]]],[11,"try_into","","",17,[[["self"]],["result"]]],[11,"borrow_mut","","",17,[[["self"]],["t"]]],[11,"into","","",18,[[["self"]],["u"]]],[11,"to_owned","","",18,[[["self"]],["t"]]],[11,"clone_into","","",18,N],[11,"from","","",18,[[["t"]],["t"]]],[11,"try_from","","",18,[[["u"]],["result"]]],[11,"borrow","","",18,[[["self"]],["t"]]],[11,"get_type_id","","",18,[[["self"]],["typeid"]]],[11,"try_into","","",18,[[["self"]],["result"]]],[11,"borrow_mut","","",18,[[["self"]],["t"]]],[11,"into","","",19,[[["self"]],["u"]]],[11,"to_owned","","",19,[[["self"]],["t"]]],[11,"clone_into","","",19,N],[11,"from","","",19,[[["t"]],["t"]]],[11,"try_from","","",19,[[["u"]],["result"]]],[11,"borrow","","",19,[[["self"]],["t"]]],[11,"get_type_id","","",19,[[["self"]],["typeid"]]],[11,"try_into","","",19,[[["self"]],["result"]]],[11,"borrow_mut","","",19,[[["self"]],["t"]]],[11,"into","","",20,[[["self"]],["u"]]],[11,"to_owned","","",20,[[["self"]],["t"]]],[11,"clone_into","","",20,N],[11,"from","","",20,[[["t"]],["t"]]],[11,"try_from","","",20,[[["u"]],["result"]]],[11,"borrow","","",20,[[["self"]],["t"]]],[11,"get_type_id","","",20,[[["self"]],["typeid"]]],[11,"try_into","","",20,[[["self"]],["result"]]],[11,"borrow_mut","","",20,[[["self"]],["t"]]],[11,"into","","",21,[[["self"]],["u"]]],[11,"to_owned","","",21,[[["self"]],["t"]]],[11,"clone_into","","",21,N],[11,"from","","",21,[[["t"]],["t"]]],[11,"try_from","","",21,[[["u"]],["result"]]],[11,"borrow","","",21,[[["self"]],["t"]]],[11,"get_type_id","","",21,[[["self"]],["typeid"]]],[11,"try_into","","",21,[[["self"]],["result"]]],[11,"borrow_mut","","",21,[[["self"]],["t"]]],[11,"into","prisma_query::visitor","",29,[[["self"]],["u"]]],[11,"from","","",29,[[["t"]],["t"]]],[11,"try_from","","",29,[[["u"]],["result"]]],[11,"borrow","","",29,[[["self"]],["t"]]],[11,"get_type_id","","",29,[[["self"]],["typeid"]]],[11,"try_into","","",29,[[["self"]],["result"]]],[11,"borrow_mut","","",29,[[["self"]],["t"]]],[11,"equals","prisma_query::ast","",9,[[["self"],["t"]],["compare"]]],[11,"not_equals","","",9,[[["self"],["t"]],["compare"]]],[11,"less_than","","",9,[[["self"],["t"]],["compare"]]],[11,"less_than_or_equals","","",9,[[["self"],["t"]],["compare"]]],[11,"greater_than","","",9,[[["self"],["t"]],["compare"]]],[11,"greater_than_or_equals","","",9,[[["self"],["t"]],["compare"]]],[11,"in_selection","","",9,[[["self"],["t"]],["compare"]]],[11,"not_in_selection","","",9,[[["self"],["t"]],["compare"]]],[11,"like","","",9,[[["self"],["t"]],["compare"]]],[11,"not_like","","",9,[[["self"],["t"]],["compare"]]],[11,"begins_with","","",9,[[["self"],["t"]],["compare"]]],[11,"not_begins_with","","",9,[[["self"],["t"]],["compare"]]],[11,"ends_into","","",9,[[["self"],["t"]],["compare"]]],[11,"not_ends_into","","",9,[[["self"],["t"]],["compare"]]],[11,"is_null","","",9,[[["self"]],["compare"]]],[11,"is_not_null","","",9,[[["self"]],["compare"]]],[11,"between","","",9,[[["self"],["t"],["v"]],["compare"]]],[11,"not_between","","",9,[[["self"],["t"],["v"]],["compare"]]],[11,"equals","","",20,[[["self"],["t"]],["compare"]]],[11,"not_equals","","",20,[[["self"],["t"]],["compare"]]],[11,"less_than","","",20,[[["self"],["t"]],["compare"]]],[11,"less_than_or_equals","","",20,[[["self"],["t"]],["compare"]]],[11,"greater_than","","",20,[[["self"],["t"]],["compare"]]],[11,"greater_than_or_equals","","",20,[[["self"],["t"]],["compare"]]],[11,"in_selection","","",20,[[["self"],["t"]],["compare"]]],[11,"not_in_selection","","",20,[[["self"],["t"]],["compare"]]],[11,"like","","",20,[[["self"],["t"]],["compare"]]],[11,"not_like","","",20,[[["self"],["t"]],["compare"]]],[11,"begins_with","","",20,[[["self"],["t"]],["compare"]]],[11,"not_begins_with","","",20,[[["self"],["t"]],["compare"]]],[11,"ends_into","","",20,[[["self"],["t"]],["compare"]]],[11,"not_ends_into","","",20,[[["self"],["t"]],["compare"]]],[11,"is_null","","",20,[[["self"]],["compare"]]],[11,"is_not_null","","",20,[[["self"]],["compare"]]],[11,"between","","",20,[[["self"],["t"],["v"]],["compare"]]],[11,"not_between","","",20,[[["self"],["t"],["v"]],["compare"]]],[11,"order","","",0,[[["self"],["option",["order"]]],["orderdefinition"]]],[11,"into_order_definition","","",0,[[["self"]],["orderdefinition"]]],[11,"into_order_definition","prisma_query","",30,[[["self"]],["orderdefinition"]]],[11,"alias","prisma_query::ast","",11,[[["self"],["t"]],["self"]]],[11,"build","prisma_query::visitor","",29,N],[11,"add_parameter","","",29,[[["self"],["parameterizedvalue"]]]],[11,"visit_limit","","",29,[[["self"],["option",["usize"]]],["string"]]],[11,"visit_function","","",29,[[["self"],["function"]],["string"]]],[11,"visit_partitioning","","",29,[[["self"],["over"]],["string"]]],[11,"visit_offset","","",29,[[["self"],["usize"]],["string"]]],[11,"into","prisma_query::ast","",0,[[["self"]],["databasevalue"]]],[11,"into","","",12,[[["self"]],["conditiontree"]]],[11,"into","","",12,[[["self"]],["expression"]]],[11,"into","","",13,[[["self"]],["expression"]]],[11,"into","","",10,[[["self"]],["databasevalue"]]],[11,"into","","",21,[[["self"]],["databasevalue"]]],[11,"into","","",9,[[["self"]],["databasevalue"]]],[11,"into","","",5,[[["self"]],["databasevalue"]]],[11,"default","","",0,[[],["column"]]],[11,"default","","",13,[[],["self"]]],[11,"default","","",1,[[],["over"]]],[11,"default","","",2,[[],["rownumber"]]],[11,"default","","",8,[[],["ordering"]]],[11,"default","","",9,[[],["row"]]],[11,"default","","",10,[[],["select"]]],[11,"eq","","",0,[[["self"],["column"]],["bool"]]],[11,"ne","","",0,[[["self"],["column"]],["bool"]]],[11,"eq","","",12,[[["self"],["compare"]],["bool"]]],[11,"ne","","",12,[[["self"],["compare"]],["bool"]]],[11,"eq","","",13,[[["self"],["conditiontree"]],["bool"]]],[11,"ne","","",13,[[["self"],["conditiontree"]],["bool"]]],[11,"eq","","",14,[[["self"],["expression"]],["bool"]]],[11,"ne","","",14,[[["self"],["expression"]],["bool"]]],[11,"eq","","",1,[[["self"],["over"]],["bool"]]],[11,"ne","","",1,[[["self"],["over"]],["bool"]]],[11,"eq","","",2,[[["self"],["rownumber"]],["bool"]]],[11,"ne","","",2,[[["self"],["rownumber"]],["bool"]]],[11,"eq","","",3,[[["self"],["count"]],["bool"]]],[11,"ne","","",3,[[["self"],["count"]],["bool"]]],[11,"eq","","",4,[[["self"],["distinct"]],["bool"]]],[11,"ne","","",4,[[["self"],["distinct"]],["bool"]]],[11,"eq","","",5,[[["self"],["function"]],["bool"]]],[11,"ne","","",5,[[["self"],["function"]],["bool"]]],[11,"eq","","",15,[[["self"],["functiontype"]],["bool"]]],[11,"ne","","",15,[[["self"],["functiontype"]],["bool"]]],[11,"eq","","",6,[[["self"],["insert"]],["bool"]]],[11,"ne","","",6,[[["self"],["insert"]],["bool"]]],[11,"eq","","",7,[[["self"],["joindata"]],["bool"]]],[11,"ne","","",7,[[["self"],["joindata"]],["bool"]]],[11,"eq","","",16,[[["self"],["join"]],["bool"]]],[11,"ne","","",16,[[["self"],["join"]],["bool"]]],[11,"eq","","",8,[[["self"],["ordering"]],["bool"]]],[11,"ne","","",8,[[["self"],["ordering"]],["bool"]]],[11,"eq","","",17,[[["self"],["order"]],["bool"]]],[11,"eq","","",18,[[["self"],["query"]],["bool"]]],[11,"ne","","",18,[[["self"],["query"]],["bool"]]],[11,"eq","","",9,[[["self"],["row"]],["bool"]]],[11,"ne","","",9,[[["self"],["row"]],["bool"]]],[11,"eq","","",10,[[["self"],["select"]],["bool"]]],[11,"ne","","",10,[[["self"],["select"]],["bool"]]],[11,"eq","","",19,[[["self"],["tabletype"]],["bool"]]],[11,"ne","","",19,[[["self"],["tabletype"]],["bool"]]],[11,"eq","","",11,[[["self"],["table"]],["bool"]]],[11,"ne","","",11,[[["self"],["table"]],["bool"]]],[11,"eq","","",21,[[["self"],["parameterizedvalue"]],["bool"]]],[11,"ne","","",21,[[["self"],["parameterizedvalue"]],["bool"]]],[11,"eq","","",20,[[["self"],["databasevalue"]],["bool"]]],[11,"ne","","",20,[[["self"],["databasevalue"]],["bool"]]],[11,"from","","",0,[[["str"]],["column"]]],[11,"from","","",0,N],[11,"from","","",0,N],[11,"from","","",0,[[["string"]],["column"]]],[11,"from","","",0,N],[11,"from","","",0,N],[11,"from","","",13,[[["select"]],["conditiontree"]]],[11,"from","","",14,[[["select"]],["expression"]]],[11,"from","","",5,[[["rownumber"]],["function"]]],[11,"from","","",20,[[["rownumber"]],["databasevalue"]]],[11,"from","","",5,[[["distinct"]],["function"]]],[11,"from","","",20,[[["distinct"]],["databasevalue"]]],[11,"from","","",5,[[["count"]],["function"]]],[11,"from","","",20,[[["count"]],["databasevalue"]]],[11,"from","","",18,[[["insert"]],["query"]]],[11,"from","","",9,[[["vec"]],["row"]]],[11,"from","","",9,N],[11,"from","","",9,N],[11,"from","","",9,N],[11,"from","","",9,N],[11,"from","","",18,[[["select"]],["query"]]],[11,"from","","",11,[[["str"]],["table"]]],[11,"from","","",11,N],[11,"from","","",11,[[["string"]],["table"]]],[11,"from","","",11,N],[11,"from","","",11,[[["select"]],["table"]]],[11,"from","","",20,[[["vec"]],["databasevalue"]]],[11,"clone","","",0,[[["self"]],["column"]]],[11,"clone","","",12,[[["self"]],["compare"]]],[11,"clone","","",13,[[["self"]],["conditiontree"]]],[11,"clone","","",14,[[["self"]],["expression"]]],[11,"clone","","",1,[[["self"]],["over"]]],[11,"clone","","",2,[[["self"]],["rownumber"]]],[11,"clone","","",3,[[["self"]],["count"]]],[11,"clone","","",4,[[["self"]],["distinct"]]],[11,"clone","","",5,[[["self"]],["function"]]],[11,"clone","","",15,[[["self"]],["functiontype"]]],[11,"clone","","",6,[[["self"]],["insert"]]],[11,"clone","","",7,[[["self"]],["joindata"]]],[11,"clone","","",16,[[["self"]],["join"]]],[11,"clone","","",8,[[["self"]],["ordering"]]],[11,"clone","","",17,[[["self"]],["order"]]],[11,"clone","","",18,[[["self"]],["query"]]],[11,"clone","","",9,[[["self"]],["row"]]],[11,"clone","","",10,[[["self"]],["select"]]],[11,"clone","","",19,[[["self"]],["tabletype"]]],[11,"clone","","",11,[[["self"]],["table"]]],[11,"clone","","",21,[[["self"]],["parameterizedvalue"]]],[11,"clone","","",20,[[["self"]],["databasevalue"]]],[11,"fmt","","",0,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",12,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",13,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",14,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",1,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",2,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",3,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",4,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",5,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",15,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",6,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",7,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",16,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",8,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",17,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",18,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",9,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",10,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",19,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",11,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",21,[[["self"],["formatter"]],["result"]]],[11,"fmt","","",20,[[["self"],["formatter"]],["result"]]],[11,"to_sql","","",21,[[["self"]],["result",["tosqloutput","rusqlerror"]]]],[11,"ascend","","Change the order to `ASC`",26,[[["self"]],["orderdefinition"]]],[11,"descend","","Change the order to `DESC`",26,[[["self"]],["orderdefinition"]]]],"paths":[[3,"Column"],[3,"Over"],[3,"RowNumber"],[3,"Count"],[3,"Distinct"],[3,"Function"],[3,"Insert"],[3,"JoinData"],[3,"Ordering"],[3,"Row"],[3,"Select"],[3,"Table"],[4,"Compare"],[4,"ConditionTree"],[4,"Expression"],[4,"FunctionType"],[4,"Join"],[4,"Order"],[4,"Query"],[4,"TableType"],[4,"DatabaseValue"],[4,"ParameterizedValue"],[8,"Comparable"],[8,"Conjuctive"],[8,"Joinable"],[8,"IntoOrderDefinition"],[8,"Orderable"],[8,"Aliasable"],[8,"Visitor"],[3,"Sqlite"],[6,"OrderDefinition"]]};
initSearch(searchIndex);
