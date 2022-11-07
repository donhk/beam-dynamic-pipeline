## Sample project of dynamic pipeline

- Apache Beam

## Concepts
A column is composed of a name and a type there are 3 types of columns (this is defined by the schema)

- dimension
  - string 
- temporal
  - time 
- scalar
  - double

### Rules
Scalars can only be combined with other scalars `scalar * scala = scalar`

### Transformations

Aggregations
```
window: 5 records
trans:  sum(col1)
output: 1 record | 1 column | col1
```

Transform a column
```vim
window: 5 records
trans:  divide(col1, 1000)
output: 5 records | 1 column | (col1/1000)
```

Combine columns into one
```vim
window: 5 records
trans:  sumColumns([col1, col2, col3], 'col4')
output: 5 records | 1 column | col1
```

Derive columns from other columns
```vim
window: 5 records
trans:  sumColumnsKeep([col1, col2], 'col3')
output: 5 records | 3 columns | col1, col2 col3
```

Example:
```
Column
name: abc
value 1
```

The stream is broken into windows where the transformations are applied
```
Key         ElasticRow
            col_a(dimension), col_b(scalar), col_c(scalar)]
100         row{'col_a'=>s1, 'col_b'=>2, 'col_c'=3}
101         row{'col_a'=>s1, 'col_b'=>2, 'col_c'=3}
102         row{'col_a'=>s1, 'col_b'=>2, 'col_c'=3}
window 1
103         row{'col_a'=>s1, 'col_b'=>2, 'col_c'=3}
104         row{'col_a'=>s1, 'col_b'=>2, 'col_c'=3}
105         row{'col_a'=>s1, 'col_b'=>2, 'col_c'=3}
window 2
...
```
