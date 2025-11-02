# Generate data for the CI

The goal is to have at leat one alert per user-filter. For this just generate data on the cluster using:

```bash
fink start generate_test_data -s ${SURVEY} -c ${conf} -night ${NIGHT} ${RESOURCES}
```

The parameter `night` is actually not used, as it is set to 2023/10/18. In case you have a new filter, that requires new columns, just create them on-the-fly (like for TNS).
