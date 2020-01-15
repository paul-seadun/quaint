initSidebarItems({"enum":[["Compare","For modeling comparison expression"],["ConditionTree","Tree structures and leaves for condition building."],["DatabaseValue","A value we can compare and use in database queries."],["Expression","A database expression."],["Join","A representation of a `JOIN` statement."],["OnConflict","`INSERT` conflict resolution strategies."],["Order","The ordering direction"],["ParameterizedValue","A value we must parameterize for the prepared statement."],["Query","A database query"],["SqlOp","Calculation operations in SQL queries."],["TableType","Either an identifier or a nested query."]],"fn":[["aggregate_to_string","Aggregates the given field into a string."],["asterisk","A quick alias to create an asterisk to a table."],["count","Count of the underlying table where the given expression is not null."],["row_number","A number from 1 to n in specified order"]],"struct":[["AggregateToString","An aggregate function that concatenates strings from a group into a single string with various options."],["Column","A column definition."],["Count","Returns the number of rows that matches a specified criteria."],["Delete","A builder for a `DELETE` statement."],["Function","A database function definition"],["Grouping","A list of definitions for the `GROUP BY` statement"],["Insert","A builder for an `INSERT` statement."],["JoinData","The `JOIN` table and conditions."],["MultiRowInsert","A builder for an `INSERT` statement for multiple rows."],["Ordering","A list of definitions for the `ORDER BY` statement"],["Over","Determines the partitioning and ordering of a rowset before the associated window function is applied."],["Row","A collection of values surrounded by parentheses."],["RowNumber","A window function that assigns a sequential integer number to each row in the query’s result set."],["Select","A builder for a `SELECT` statement."],["SingleRowInsert","A builder for an `INSERT` statement for a single row."],["Table","A table definition"],["Union","A builder for a `UNION`s over multiple `SELECT` statements."],["Update","A builder for an `UPDATE` statement."]],"trait":[["Aliasable","An object that can be aliased."],["Comparable","An item that can be compared against other values in the database."],["Conjuctive","`AND`, `OR` and `NOT` conjuctive implementations."],["Groupable","An item that can be used in the `GROUP BY` statement"],["IntoGroupByDefinition","Convert the value into a group by definition."],["IntoOrderDefinition","Convert the value into an order definition with order item and direction"],["Joinable","An item that can be joined."],["Orderable","An item that can be used in the `ORDER BY` statement"]],"type":[["GroupByDefinition",""],["OrderDefinition",""]]});