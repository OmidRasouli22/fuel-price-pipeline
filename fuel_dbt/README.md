# fuel_dbt

dbt models for the fuel price pipeline. See the main [README](../README.md)
for what each table holds.

```
dbt build                                   all models and tests
dbt build --select price_changes+           only the history part
```
