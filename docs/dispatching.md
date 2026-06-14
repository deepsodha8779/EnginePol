# Dispatcher and Rule Handlers

The dispatcher routes each rule in the Tadpole tail to a registered handler.
Handlers do evaluation and return a structured result (PASS / FAIL / INCONCLUSIVE + reason code + message).

## Contract

All rule handlers implement the dispatcher interface:

- `RuleEvaluator::evaluate(tadpole, rule) -> RuleEvaluation`
- The dispatcher depends on this trait, not concrete handler types.

## Registering Handlers

Register handlers by `RuleKind` when constructing the dispatcher.
Example (Boolean + Enrichment Stub):

```rust
use std::collections::HashMap;
use std::sync::Arc;

use engine_core::actors::dispatcher::{
    BooleanRuleEvaluator, DispatcherActor, EnrichmentStubEvaluator, RuleEvaluator,
};
use engine_core::messages::rules::RuleKind;

let mut evaluators: HashMap<RuleKind, Arc<dyn RuleEvaluator>> = HashMap::new();
evaluators.insert(RuleKind::Boolean, Arc::new(BooleanRuleEvaluator::new(boolean_handler)));
evaluators.insert(RuleKind::EnrichmentStub, Arc::new(EnrichmentStubEvaluator));

let dispatcher = DispatcherActor { evaluators }.start();
```

## Behavior

- Dispatcher is responsible for routing only.
- Evaluators are responsible for rule evaluation and returning `RuleEvaluation`.
- If no evaluator is registered for a `RuleKind`, the dispatcher returns `INCONCLUSIVE`.
