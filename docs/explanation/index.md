# Explanation

Background and design rationale behind `scinexus`.

- [Why composable apps?](why-composable-apps.md) -- the problem `scinexus` solves and how it compares to alternatives
- [The app lifecycle](app-lifecycle.md) -- what `define_app` does and how `__call__` works
- [Type system](type-system.md) -- how composition-time type checking works
- [NotCompleted design](not-completed-design.md) -- why a sentinel pattern instead of exceptions
- [Source tracking](source-tracking.md) -- how `source_proxy` tracks data provenance through pipelines
- [Customisation hooks](customisation-hooks.md) -- `set_summary_display` and `set_id_from_source` registry functions
- [Data store model](data-store-model.md) -- unique IDs, checkpointing, and backend choices
