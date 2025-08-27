# SAPPHIRE Forecast Pipeline Documentation

## Overview

The SAPPHIRE Forecast Tools use a Luigi-based pipeline to orchestrate complex hydrological forecasting workflows. The pipeline consists of multiple workflows that run at different times based on data availability:

1. **Preprocessing Gateway Workflow** - Runs at 04:00 UTC for meteorological data
2. **Preprocessing Runoff Workflow** - Runs at 10:00-11:00 local time when discharge data is available
3. **Pentadal Forecast Workflow** - Runs after both preprocessing workflows complete
4. **Decadal Forecast Workflow** - Runs after both preprocessing workflows complete

## Workflow Architecture

### Complete Pipeline Overview

```mermaid
graph TB
    %% Styling
    classDef preprocessing fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff
    classDef model fill:#2196F3,stroke:#333,stroke-width:2px,color:#fff
    classDef postprocessing fill:#ff9800,stroke:#333,stroke-width:2px,color:#fff
    classDef utility fill:#9c27b0,stroke:#333,stroke-width:2px,color:#fff
    classDef notification fill:#f44336,stroke:#333,stroke-width:2px,color:#fff
    classDef workflow fill:#607d8b,stroke:#333,stroke-width:2px,color:#fff
    classDef trigger fill:#795548,stroke:#333,stroke-width:2px,color:#fff

    %% Time triggers
    T1[04:00 UTC<br/>Cron Trigger]:::trigger
    T2[10:00-11:00 Local<br/>Data Available Trigger]:::trigger
    T3[After Preprocessing<br/>Complete Trigger]:::trigger

    %% Workflows
    W1[run_preprocessing_gateway_workflow]:::workflow
    W2[run_preprocessing_runoff_workflow]:::workflow
    W3[run_pentadal_forecasts]:::workflow
    W4[run_decadal_forecasts]:::workflow

    %% Connections
    T1 --> W1
    T2 --> W2
    W1 & W2 --> T3
    T3 --> W3
    T3 --> W4

    %% Workflow contents
    W1 --> PG[PreprocessingGatewayQuantileMapping<br/>Meteorological Data]:::preprocessing
    W2 --> PR[PreprocessingRunoff<br/>Discharge Data]:::preprocessing
    
    subgraph " "
        W3 --> P3[Pentadal Models & Processing]
        W4 --> P4[Decadal Models & Processing]
    end
```

### Workflow 1: Preprocessing Gateway (04:00 UTC)

```mermaid
graph TD
    %% Styling
    classDef preprocessing fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff
    classDef utility fill:#9c27b0,stroke:#333,stroke-width:2px,color:#fff
    classDef notification fill:#f44336,stroke:#333,stroke-width:2px,color:#fff
    classDef workflow fill:#607d8b,stroke:#333,stroke-width:2px,color:#fff

    %% Nodes
    W1[run_preprocessing_gateway_workflow<br/>04:00 UTC]:::workflow
    PG[PreprocessingGatewayQuantileMapping<br/>Download & Process Meteo Data]:::preprocessing
    DO[DeleteOldGatewayFiles<br/>Cleanup Old Data]:::utility
    SN1[SendNotification<br/>Gateway Complete]:::notification

    %% Dependencies
    W1 --> PG
    W1 --> DO
    PG --> SN1
    DO --> SN1
```

### Workflow 2: Preprocessing Runoff (10:00-11:00 Local)

```mermaid
graph TD
    %% Styling
    classDef preprocessing fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff
    classDef utility fill:#9c27b0,stroke:#333,stroke-width:2px,color:#fff
    classDef notification fill:#f44336,stroke:#333,stroke-width:2px,color:#fff
    classDef workflow fill:#607d8b,stroke:#333,stroke-width:2px,color:#fff

    %% Nodes
    W2[run_preprocessing_runoff_workflow<br/>When Data Available]:::workflow
    PR[PreprocessingRunoff<br/>Process Discharge Data]:::preprocessing
    LC[LogFileCleanup<br/>Cleanup Logs]:::utility
    SN2[SendNotification<br/>Runoff Complete]:::notification

    %% Dependencies
    W2 --> PR
    W2 --> LC
    PR --> SN2
    LC --> SN2
```

### Workflow 3: Pentadal Forecasts

```mermaid
graph TD
    %% Styling
    classDef preprocessing fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff
    classDef model fill:#2196F3,stroke:#333,stroke-width:2px,color:#fff
    classDef postprocessing fill:#ff9800,stroke:#333,stroke-width:2px,color:#fff
    classDef notification fill:#f44336,stroke:#333,stroke-width:2px,color:#fff
    classDef workflow fill:#607d8b,stroke:#333,stroke-width:2px,color:#fff
    classDef config fill:#9c27b0,stroke:#333,stroke-width:2px,color:#fff

    %% Workflow node
    W3[run_pentadal_forecasts]:::workflow

    %% Check dependencies
    W3 --> CD{Check Dependencies:<br/>Gateway & Runoff Complete?}
    CD -->|No| Wait[Wait & Retry]
    CD -->|Yes| EC[Environment Config<br/>Check .env]:::config
    
    %% Linear Regression - Always runs
    EC --> LR[LinearRegression<br/>Statistical Model]:::model
    
    %% Conceptual Model - Configuration based
    EC --> CMC{Conceptual Model<br/>Configured?}
    CMC -->|Yes| CM[ConceptualModel<br/>GR4J/RRAM]:::model
    CMC -->|No| SkipCM[Skip CM]
    
    %% ML Models - Configuration based
    EC --> MLC{ML Models<br/>Configured?}
    MLC -->|No| SkipML[Skip ML]
    MLC -->|Yes| Models
    
    %% ML Models
    subgraph Models [" "]
        ML1[RunMLModel<br/>TFT-PENTAD]:::model
        ML2[RunMLModel<br/>TIDE-PENTAD]:::model
        ML3[RunMLModel<br/>TSMIXER-PENTAD]:::model
        ML4[RunMLModel<br/>ARIMA-PENTAD]:::model
    end

    %% Postprocessing
    LR --> PP[PostProcessingForecasts<br/>PENTAD]:::postprocessing
    CM --> PP
    Models --> PP
    SkipCM --> PP
    SkipML --> PP
    PP --> SN[SendNotification<br/>Pentadal Complete]:::notification
```

### Workflow 4: Decadal Forecasts

```mermaid
graph TD
    %% Styling
    classDef preprocessing fill:#4CAF50,stroke:#333,stroke-width:2px,color:#fff
    classDef model fill:#2196F3,stroke:#333,stroke-width:2px,color:#fff
    classDef postprocessing fill:#ff9800,stroke:#333,stroke-width:2px,color:#fff
    classDef notification fill:#f44336,stroke:#333,stroke-width:2px,color:#fff
    classDef workflow fill:#607d8b,stroke:#333,stroke-width:2px,color:#fff
    classDef config fill:#9c27b0,stroke:#333,stroke-width:2px,color:#fff

    %% Workflow node
    W4[run_decadal_forecasts]:::workflow

    %% Check dependencies
    W4 --> CD{Check Dependencies:<br/>Gateway & Runoff Complete?}
    CD -->|No| Wait[Wait & Retry]
    CD -->|Yes| EC[Environment Config<br/>Check .env]:::config
    
    %% Linear Regression - Always runs
    EC --> LR[LinearRegression<br/>Statistical Model]:::model
    
    %% Conceptual Model - Conditional
    EC --> CMC{Conceptual Model<br/>Configured?}
    CMC -->|Yes| CMP{Pentadal CM<br/>Already Run?}
    CMC -->|No| SkipCM[Skip CM]
    CMP -->|No| CM[ConceptualModel<br/>GR4J/RRAM]:::model
    CMP -->|Yes| SkipCM
    
    %% ML Models - Configuration based
    EC --> MLC{ML Models<br/>Configured?}
    MLC -->|No| SkipML[Skip ML]
    MLC -->|Yes| Models
    
    %% ML Models
    subgraph Models [" "]
        ML1[RunMLModel<br/>TFT-DECAD]:::model
        ML2[RunMLModel<br/>TIDE-DECAD]:::model
        ML3[RunMLModel<br/>TSMIXER-DECAD]:::model
        ML4[RunMLModel<br/>ARIMA-DECAD]:::model
    end

    %% Postprocessing
    LR --> PP[PostProcessingForecasts<br/>DECAD]:::postprocessing
    CM --> PP
    Models --> PP
    SkipCM --> PP
    SkipML --> PP
    PP --> SN[SendNotification<br/>Decadal Complete]:::notification
```

## Execution Timeline

```mermaid
%%{init: {'ganttConfig': {'width': 800, 'lineColor': '#e8e8e8'}}}%%
gantt
    title Daily Pipeline Execution Schedule (Local Time)
    dateFormat HH:mm
    axisFormat %H:%M
    
    section Gateway P   
    Meteo Download & Process     :gw, 04:00, 30m
    Gateway Cleanup             :gc, 04:00, 5m
    
    section Runoff P
    Wait for Data Entry         :done, 04:30, 5h
    Runoff Processing          :pr, 10:00, 15m
    Log Cleanup                :lc, 10:00, 5m
    
    section Pentadal F
    Check Dependencies         :cd1, 10:15, 5m
    Linear Regression         :lr, 10:20, 10m
    Conceptual Model         :cm, 10:20, 30m
    ML Models (4x parallel)   :ml, 10:20, 8m
    Postprocess Pentadal     :pp1, 10:50, 15m
    
    section Decadal F
    Check Dependencies       :cd2, 10:15, 5m
    Linear Regression       :lr2, 10:20, 10m
    ML Models (4x parallel)  :ml2, 10:20, 8m
    Postprocess Decadal     :pp2, 10:30, 15m
```

## Workflow Dependencies

```mermaid
stateDiagram-v2
    [*] --> GatewayWorkflow
    
    GatewayWorkflow --> GatewayComplete
    RunoffWorkflow --> RunoffComplete
    
    GatewayComplete --> CheckDependencies
    RunoffComplete --> CheckDependencies
    
    CheckDependencies --> PentadalWorkflow
    CheckDependencies --> DecadalWorkflow
    
    PentadalWorkflow --> [*]
    DecadalWorkflow --> [*]
    
    note right of GatewayWorkflow: Preprocessing Gateway
    note right of RunoffWorkflow: Preprocessing Runoff
    note right of PentadalWorkflow: Forecast Generation
    note right of DecadalWorkflow: Forecast Generation
    
    state GatewayComplete <<choice>>
    state RunoffComplete <<choice>>
    state CheckDependencies <<choice>>
```

## Workflow Configurations

### Organization-Specific Model Selection

| Organization | Pentadal Models | Decadal Models |
|--------------|-----------------|----------------|
| **demo** | Linear Regression | Linear Regression |
| **kghm** | Linear Regression<br>Conceptual Model<br>All ML Models | Linear Regression<br>Conceptual Model<br>All ML Models |
| **tjhm** | Linear Regression<br>All ML Models | Linear Regression<br>All ML Models |

### Workflow Triggers

```yaml
# Cron expressions for workflow scheduling
workflows:
  preprocessing_gateway:
    schedule: "0 4 * * *"  # 04:00 UTC daily
    timezone: "UTC"
    
  preprocessing_runoff:
    schedule: "0 10 * * *"  # 10:00 local time
    timezone: "Local"
    condition: "data_available"
    
  pentadal_forecasts:
    trigger: "on_dependencies_complete"
    dependencies: ["preprocessing_gateway", "preprocessing_runoff"]
    
  decadal_forecasts:
    trigger: "on_dependencies_complete"
    dependencies: ["preprocessing_gateway", "preprocessing_runoff"]
```

## Data Flow Between Workflows

```mermaid
flowchart LR
    subgraph External Sources
        S1[SAPPHIRE<br>Data Gateway]
        S2[iEasyHydro<br>Database]
        S3[Excel Files]
    end
    
    subgraph Workflow 1 - Gateway
        S1 --> PG[Process<br>Meteo Data]
        PG --> D1[(Gridded<br>Weather Data)]
    end
    
    subgraph Workflow 2 - Runoff
        S2 --> PR1[Process<br>Discharge]
        S3 --> PR2[Process<br>Excel]
        PR1 & PR2 --> D2[(Discharge<br>Time Series)]
    end
    
    subgraph Workflow 3 & 4 - Forecasts
        D1 & D2 --> M1[Models]
        M1 --> D3[(Forecast<br>Results)]
        D3 --> OUT[Output Products]
    end
```

## Implementation Examples

### Luigi Task for Checking Dependencies

```python
class CheckPreprocessingComplete(luigi.Task):
    """Check if both preprocessing workflows have completed"""
    
    def requires(self):
        return {
            'gateway': PreprocessingGatewayComplete(),
            'runoff': PreprocessingRunoffComplete()
        }
    
    def run(self):
        # Both preprocessing tasks are complete
        with self.output().open('w') as f:
            f.write('Prerequisites complete')
```

### Workflow Orchestration

```python
class RunPentadalForecasts(luigi.Task):
    """Orchestrate pentadal forecast workflow"""
    
    def requires(self):
        # First check if preprocessing is complete
        yield CheckPreprocessingComplete()
        
        # Then run models based on organization
        if ORGANIZATION == 'kghm':
            yield LinearRegression(prediction_mode='PENTAD')
            yield ConceptualModel(prediction_mode='PENTAD')
            yield RunAllMLModels(prediction_mode='PENTAD')
        elif ORGANIZATION == 'tjhm':
            yield LinearRegression(prediction_mode='PENTAD')
            yield RunAllMLModels(prediction_mode='PENTAD')
        else:  # demo
            yield LinearRegression(prediction_mode='PENTAD')
    
    def run(self):
        # Run postprocessing
        yield PostProcessingForecasts(prediction_mode='PENTAD')
        yield SendNotification(message='Pentadal forecasts complete')
```

## Monitoring and Status

### Workflow Status Dashboard

```mermaid
graph LR
    subgraph Status Indicators
        S1[🟢 Complete]
        S2[🟡 Running]
        S3[🔴 Failed]
        S4[⏸️ Waiting]
    end
    
    subgraph Today's Status
        GW[Gateway<br>🟢 04:30]
        RO[Runoff<br>🟡 10:15]
        PF[Pentadal<br>⏸️ Waiting]
        DF[Decadal<br>⏸️ Waiting]
    end
```

### Key Metrics to Monitor

1. **Preprocessing Completion Times**
   - Gateway workflow duration
   - Runoff workflow duration
   - Time between trigger and completion

2. **Model Execution Times**
   - Individual model runtimes
   - Parallel execution efficiency
   - Resource utilization

3. **Data Quality Indicators**
   - Missing data points
   - Data validation failures
   - Forecast quality metrics

## Error Handling Across Workflows

```mermaid
stateDiagram-v2
    [*] --> WorkflowStart
    
    WorkflowStart --> TaskExecution
    TaskExecution --> Success: All tasks complete
    TaskExecution --> PartialFailure: Some tasks fail
    TaskExecution --> CriticalFailure: Preprocessing fails
    
    Success --> NotifySuccess
    PartialFailure --> RetryFailed: Retry failed tasks
    CriticalFailure --> NotifyFailure: Alert operators
    
    RetryFailed --> Success: Retry succeeds
    RetryFailed --> NotifyPartial: Max retries reached
    
    NotifySuccess --> [*]
    NotifyPartial --> [*]
    NotifyFailure --> [*]
```

## Best Practices

1. **Dependency Management**
   - Always check preprocessing completion before running forecasts
   - Use file markers or database flags to track workflow status
   - Implement proper timeout handling for long-running tasks

2. **Scheduling Considerations**
   - Account for time zone differences (UTC vs local)
   - Build in buffer time for data availability delays
   - Consider holidays and weekends for operational data entry

3. **Resource Optimization**
   - Run independent workflows in parallel
   - Stagger resource-intensive tasks
   - Clean up intermediate data regularly

4. **Monitoring and Alerts**
   - Set up alerts for workflow failures
   - Monitor preprocessing completion times
   - Track forecast quality metrics