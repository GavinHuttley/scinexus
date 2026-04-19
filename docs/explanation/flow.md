# Execution flow of a composed app

!!! abstract ""

    How data flows through a composed pipeline, step by step.

Consider two apps composed into a pipeline:

```python { notest }
from scinexus import define_app


@define_app(app_type="loader")
def read_json(path: str) -> dict:
    import json

    with open(path) as f:
        return json.load(f)


@define_app
def validate(data: dict, required_field: str) -> dict:
    if required_field not in data:
        raise ValueError(f"missing {required_field!r} field")
    return data


app = read_json() + validate(required_field="name")
```

Composing with `+` creates a new app where `validate` is the outermost app and `read_json` is stored as its `.input` attribute. When you call `app(filepath)`, execution begins at the outermost app and works inward.

## The execution flow when you call `app(filepath)`

```mermaid
flowchart TD
    entry["Executes scinexus __call__(val)"] --> none{val is None?}
    none -- yes --> nc_none[create and return NotCompleted ERROR, recording current app as origin]
    none -- no --> nc{val is NotCompleted?}
    nc -- yes --> nc_return[returns same NotCompleted]
    nc -- no --> has_input{has an input app?}
    has_input -- yes --> call_input["call input(val), which enters the top of this chart"]
    call_input --> input_nc{result is NotCompleted?}
    input_nc -- yes --> nc_input[return same NotCompleted]
    input_nc -- no --> type_check
    has_input -- no --> type_check{type check val}
    type_check -- fail --> nc_type[create and return NotCompleted ERROR]
    type_check -- pass --> main["main(val)"]
    main -- exception --> nc_main[NotCompleted ERROR]
    main -- success --> result["return result (which may be NotCompleted FAIL)"]

    classDef errorNode fill:#fde0c8,stroke:#333
    classDef successNode fill:#c8e0fd,stroke:#333
    class nc_none,nc_return,nc_input,nc_type,nc_main errorNode
    class result successNode
```

This is the same sequence for every composed app, regardless of pipeline length. Each app in the chain runs the same `__call__` checks, so `NotCompleted` propagation and exception handling are consistent throughout. See [Runtime type checking](type-system.md#runtime-type-checking) for details on how type validation works and how to disable it.
