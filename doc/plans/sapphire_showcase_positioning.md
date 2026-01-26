# SAPPHIRE Forecast Tools — Showcase & Positioning Analysis

*Created: 2026-01-20*
*Status: Draft for discussion*

## Strategic Context

SAPPHIRE Forecast Tools is an operational hydrological forecasting system developed within Swiss development cooperation (SDC-funded SAPPHIRE project). The goal is to position this on hydrosolutions' website to attract new projects and clients.

**Core value proposition**: Help hydromets improve their services for their customers (hydropower operators, flood risk managers, environmental regulators).

## Target Audiences (Priority Order)

| Priority | Audience | Description | Current relevance |
|----------|----------|-------------|-------------------|
| **HIGH** | Commercial clients | Hydropower companies, utilities seeking forecasting services | Growth target |
| **HIGH** | Hydromets | Technical staff evaluating tools for their operations | Current projects |
| **MEDIUM** | Development cooperation | SDC, GIZ, World Bank, ADB project managers | Current projects, but not priority |
| **LOW** | Technical community | Researchers, engineers who might collaborate | Awareness/credibility |

## Current Capabilities

### What Exists Today
- **Multiple forecast models**: Linear regression, ARIMA, ML (TFT, TIDE, TSMIXER), conceptual rainfall-runoff (GR4J with data assimilation)
- **Ensemble forecasting**: Skill-weighted model averaging
- **Operational deployment**: Running in Kyrgyzstan with Kyrgyz Hydromet
- **Docker-based deployment**: Works on limited infrastructure
- **Dashboards**: Configuration and visualization interfaces
- **Multi-language**: Russian/English support

### In Development
- **Long-term forecasting**: Monthly to seasonal horizons (in progress)
- **Sub-daily peak forecasting**: For flood applications (planned project)

## Use Cases Being Developed

| Use case | End customer | Key forecast need | Status |
|----------|-------------|-------------------|--------|
| Flood risk | Civil protection, municipalities | Sub-daily peaks, timing, exceedance probabilities | Planned |
| Hydropower | Plant operators, energy traders | Inflow volumes, seasonal horizons, uncertainty | In development |
| Environmental flows | Regulators, river basin authorities | Low flow forecasts, threshold probabilities | Exploring |

## Gap Analysis

### Element: Working Models
- **Current state**: Multiple model types deployed and operational
- **Gap**: None — this is solid
- **Priority**: N/A

### Element: Operational Track Record
- **Current state**: Running in Kyrgyzstan
- **Gap**: Need to document runtime history, uptime statistics, operational lessons
- **Priority**: HIGH for all audiences
- **Action**: Compile operational statistics from Kyrgyz deployment

### Element: Quantified Skill Metrics
- **Current state**: Skill metrics exist internally
- **Gap**: Not published or easily accessible; need comparison vs. baseline/persistence
- **Priority**:
  - CRITICAL for commercial clients (they want to see numbers)
  - HIGH for hydromets (technical credibility)
  - MEDIUM for dev cooperation (care more about impact stories)
- **Action**: Create skill metric summaries by model type and forecast horizon

### Element: Visual Demo
- **Current state**: Dashboard exists but requires deployment
- **Gap**: No public-facing demo instance
- **Priority**:
  - HIGH for commercial clients (want to see product)
  - HIGH for hydromets (want to evaluate)
  - LOW for dev cooperation (care more about narrative)
- **Action**: Consider lightweight demo with Swiss public data

### Element: Case Study
- **Current state**: Experience exists from Kyrgyzstan
- **Gap**: Not written up with quantified outcomes
- **Priority**: HIGH for all audiences
- **Action**: Write 1-2 page case study with before/after, user quotes if possible

### Element: Comparison to Alternatives
- **Current state**: Not documented
- **Gap**: "Why SAPPHIRE vs. commercial solutions or building in-house?"
- **Priority**:
  - CRITICAL for commercial clients (competitive positioning)
  - MEDIUM for hydromets (often not aware of alternatives)
  - LOW for dev cooperation (less relevant)
- **Action**: Create positioning statement vs. alternatives

### Element: Long-term Forecasting
- **Current state**: In development
- **Gap**: Cannot yet demonstrate
- **Priority**: HIGH for hydropower clients
- **Action**: Complete development, then add to showcase

### Element: Sub-daily/Flood Forecasting
- **Current state**: Planned
- **Gap**: Cannot yet demonstrate
- **Priority**: HIGH for flood risk market
- **Action**: Complete project, then add to showcase

## Prioritized Actions by Audience

### For Commercial Clients (Hydropower, Utilities) — HIGH PRIORITY

1. **Quantified skill metrics** — "Our models achieve X% improvement over persistence forecasts"
2. **Comparison to alternatives** — Position vs. buying commercial solutions or building in-house
3. **Visual demo** — Let them see the product
4. **Long-term forecasting capability** — Complete development to address their horizon needs

### For Hydromets — HIGH PRIORITY

1. **Case study from Kyrgyzstan** — Peer credibility matters
2. **Quantified skill metrics** — Technical substance
3. **Operational track record** — Proof it works in real conditions
4. **Visual demo** — Evaluate fit for their context

### For Development Cooperation — MEDIUM PRIORITY

1. **Case study** — Impact narrative with human element
2. **Operational track record** — Sustainability, handover success
3. *(Skill metrics less critical — they trust technical partners)*

### For Technical Community — LOW PRIORITY

1. **Open source repository** — Already exists
2. **Technical documentation** — Improve as side effect of other work
3. *(Lower priority unless seeking research collaborations)*

## Recommended Sequence

Based on effort vs. impact:

1. **Write Kyrgyzstan case study** (1-2 days effort, high impact across all audiences)
2. **Compile and publish skill metrics** (need to assess effort, critical for commercial)
3. **Document operational track record** (low effort, supports credibility)
4. **Create comparison/positioning statement** (half day, important for commercial)
5. **Set up public demo** (more effort, but high impact for commercial + hydromets)
6. **Complete long-term forecasting** (ongoing, unlocks hydropower market)
7. **Complete sub-daily forecasting** (future, unlocks flood market)

## Open Questions

- [ ] Do we have permission to share Kyrgyz Hydromet outcomes publicly?
- [ ] What skill metrics are currently computed and stored?
- [ ] Is there budget/appetite for maintaining a public demo instance?
- [ ] How do we want to position pricing/engagement model on website?

---

*This document to be revisited after initial actions completed.*
