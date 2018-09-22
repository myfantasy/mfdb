#mfdb

pool for db connect

## how to use

## example for setings

``` json
[
    {
        "name":"connection1",
        "recheck":3000,
        "reconnection":200,
        "params":
        {
            "name":"c1_inst1",
            "driver_name":"postgres",
            "data_source":"host=h1 port=5432 user=postgres password=password dbname=name sslmode=disable",
            "pool_size":3,
            "min_pool_size":3,
            "pool_life_time_connection":300,
            "priority":0,
            "context_prepare":"set time zone 'Europe/Moscow';set statement_timeout to 2000;"

        }
    },
    {
        "name":"connection2",
        "recheck":3000,
        "reconnection":200,
        "params":
        [
            {
                "name":"c1_inst2.1",
                "driver_name":"postgres",
                "data_source":"host=h21 port=5432 user=postgres password=password dbname=name sslmode=disable",
                "pool_size":3,
                "min_pool_size":3,
                "pool_life_time_connection":300,
                "priority":0

            },
            {
                "name":"c1_inst2.2",
                "driver_name":"postgres",
                "data_source":"host=h22 port=5432 user=postgres password=password dbname=name sslmode=disable",
                "pool_size":3,
                "min_pool_size":3,
                "pool_life_time_connection":300,
                "priority":2

            }
        ]
    }
]
```

Fields `recheck, reconnection, pool_size, min_pool_size, pool_life_time_connection, priority, context_prepare` are optional



