# Tadpole Engine Flow

Mermaid (current runtime path):

```mermaid
flowchart TD
  OS[OutSystems Domain Model<br>External Dependency + Artefacts] -->|Domain Change Event<br>sub-object + minimal snapshot| INTAKE[Intake API]
  SCH[Engine Runtime Scheduler<br>time-based triggers] -->|Time-Driven Domain Change Event| INTAKE

  INTAKE --> ENG[Tadpole Engine Runtime]

  ENG --> VAL[1. Validate Envelope<br>ULIDs, enums, tenant boundary]
  VAL --> ASSIGN[2. Assign Playbooks<br>header + scope descriptor]
  ASSIGN --> EXEC[3. Execute Rules<br>Dispatcher + Handlers]
  EXEC --> ORCH[4. Orchestrate Results<br>derive conclusions]
  ORCH --> OUT[5. Emit GovernanceConclusion Event]

  OUT --> OS2[OutSystems Consumers]
  OS2 --> PROJ[Update Governance Projections<br>dashboard tables]
  OS2 --> WORK[Update Work Queue<br>actions + assignments]
  PROJ --> UI[User Dashboards<br>OutSystems UI]
  WORK --> UI
```

Notes:
- RabbitMQ is omitted in the current runtime; the intake API is the integration point.
- Intake accepts only domain change events (`event_category` Transaction/TimeDriven).
- The engine emits `GovernanceConclusion` events; trace data stays internal.
