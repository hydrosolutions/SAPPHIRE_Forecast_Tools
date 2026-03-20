# Testing Anti-Patterns

**Load this reference when:** writing or changing tests, adding mocks, or tempted to add test-only methods to production code.

## Overview

Tests must verify real behavior, not mock behavior. Mocks are a means to isolate, not the thing being tested.

**Core principle:** Test what the code does, not what the mocks do.

**Following strict TDD prevents these anti-patterns.**

## The Iron Laws

```
1. NEVER test mock behavior
2. NEVER add test-only methods to production classes
3. NEVER mock without understanding dependencies
```

## Anti-Pattern 1: Testing Mock Behavior

**The violation:**
```python
# BAD: Testing that the mock exists
def test_calls_api_client(mocker):
    mock_client = mocker.patch("module.api_client")
    process_data(data)
    mock_client.assert_called_once()
    # Only proves the mock was called — not that the data was processed correctly
```

**Why this is wrong:**
- You're verifying the mock works, not that the function works
- Test passes when mock is present, fails when it's not
- Tells you nothing about real behavior

**your human partner's correction:** "Are we testing the behavior of a mock?"

**The fix:**
```python
# GOOD: Test real behavior
def test_processes_data_correctly(tmp_path):
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("date,value\n2024-01-01,100.0\n")

    result = process_data(input_csv)

    assert result["mean_value"] == 100.0
```

### Gate Function

```
BEFORE asserting on any mock element:
  Ask: "Am I testing real behavior or just mock existence?"

  IF testing mock existence:
    STOP - Delete the assertion or unmock the dependency

  Test real behavior instead
```

## Anti-Pattern 2: Test-Only Methods in Production

**The violation:**
```python
# BAD: _reset() only used in tests
class ForecastCache:
    def _reset(self):  # Looks like production API!
        self._cache.clear()
        self._initialized = False

# In tests
@pytest.fixture(autouse=True)
def clean_cache(cache):
    cache._reset()
    yield
    cache._reset()
```

**Why this is wrong:**
- Production class polluted with test-only code
- Dangerous if accidentally called in production
- Violates YAGNI and separation of concerns

**The fix:**
```python
# GOOD: Test utilities handle test cleanup
# ForecastCache has no _reset() - not needed in production

# In conftest.py
@pytest.fixture(autouse=True)
def clean_cache():
    """Create fresh cache for each test."""
    cache = ForecastCache()  # Fresh instance per test
    yield cache
```

### Gate Function

```
BEFORE adding any method to production class:
  Ask: "Is this only used by tests?"

  IF yes:
    STOP - Don't add it
    Put it in test utilities instead

  Ask: "Does this class own this resource's lifecycle?"

  IF no:
    STOP - Wrong class for this method
```

## Anti-Pattern 3: Mocking Without Understanding

**The violation:**
```python
# BAD: Mock breaks test logic
def test_detects_duplicate_station(mocker):
    # Mock prevents config write that test depends on!
    mocker.patch("module.write_station_config", return_value=None)

    add_station(config)
    add_station(config)  # Should raise — but won't!
```

**Why this is wrong:**
- Mocked method had side effect test depended on (writing config)
- Over-mocking to "be safe" breaks actual behavior
- Test passes for wrong reason or fails mysteriously

**The fix:**
```python
# GOOD: Mock at correct level
def test_detects_duplicate_station(tmp_path):
    # Use real config file in temp dir, only mock external API
    config_path = tmp_path / "stations.json"

    add_station(config, config_path=config_path)  # Config written
    with pytest.raises(ValueError, match="already exists"):
        add_station(config, config_path=config_path)  # Duplicate detected
```

### Gate Function

```
BEFORE mocking any method:
  STOP - Don't mock yet

  1. Ask: "What side effects does the real method have?"
  2. Ask: "Does this test depend on any of those side effects?"
  3. Ask: "Do I fully understand what this test needs?"

  IF depends on side effects:
    Mock at lower level (the actual slow/external operation)
    OR use test doubles that preserve necessary behavior
    NOT the high-level method the test depends on

  IF unsure what test depends on:
    Run test with real implementation FIRST
    Observe what actually needs to happen
    THEN add minimal mocking at the right level

  Red flags:
    - "I'll mock this to be safe"
    - "This might be slow, better mock it"
    - Mocking without understanding the dependency chain
```

## Anti-Pattern 4: Incomplete Mocks

**The violation:**
```python
# BAD: Partial mock — only fields you think you need
mock_response = {
    "station_code": "15013",
    "data": [{"variable_code": "WDDA", "values": [{"value": 156.0}]}]
    # Missing: unit, timestamp_local, timestamp_utc, value_type
    # that downstream code uses
}

# Later: breaks when code accesses response["data"][0]["unit"]
```

**Why this is wrong:**
- **Partial mocks hide structural assumptions** — You only mocked fields you know about
- **Downstream code may depend on fields you didn't include** — Silent failures
- **Tests pass but integration fails** — Mock incomplete, real API complete
- **False confidence** — Test proves nothing about real behavior

**The Iron Rule:** Mock the COMPLETE data structure as it exists in reality, not just fields your immediate test uses.

**The fix:**
```python
# GOOD: Mirror real API response completeness
mock_response = {
    "station_code": "15013",
    "station_name": "Test Station",
    "station_type": "hydro",
    "data": [{
        "variable_code": "WDDA",
        "unit": "m3/s",
        "values": [{
            "value": 156.0,
            "value_type": "M",
            "timestamp_local": "2024-03-01T08:00:00",
            "timestamp_utc": "2024-03-01T02:00:00Z",
            "value_code": None,
        }]
    }]
}
```

### Gate Function

```
BEFORE creating mock responses:
  Check: "What fields does the real API response contain?"

  Actions:
    1. Examine actual API response from docs/examples
    2. Include ALL fields system might consume downstream
    3. Verify mock matches real response schema completely

  Critical:
    If you're creating a mock, you must understand the ENTIRE structure
    Partial mocks fail silently when code depends on omitted fields

  If uncertain: Include all documented fields
```

## Anti-Pattern 5: Integration Tests as Afterthought

**The violation:**
```
Implementation complete
No tests written
"Ready for testing"
```

**Why this is wrong:**
- Testing is part of implementation, not optional follow-up
- TDD would have caught this
- Can't claim complete without tests

**The fix:**
```
TDD cycle:
1. Write failing test
2. Implement to pass
3. Refactor
4. THEN claim complete
```

## When Mocks Become Too Complex

**Warning signs:**
- Mock setup longer than test logic
- Mocking everything to make test pass
- Mocks missing methods real components have
- Test breaks when mock changes

**your human partner's question:** "Do we need to be using a mock here?"

**Consider:** Integration tests with real components often simpler than complex mocks

## TDD Prevents These Anti-Patterns

**Why TDD helps:**
1. **Write test first** -> Forces you to think about what you're actually testing
2. **Watch it fail** -> Confirms test tests real behavior, not mocks
3. **Minimal implementation** -> No test-only methods creep in
4. **Real dependencies** -> You see what the test actually needs before mocking

**If you're testing mock behavior, you violated TDD** — you added mocks without watching test fail against real code first.

## Quick Reference

| Anti-Pattern | Fix |
|--------------|-----|
| Assert on mock elements | Test real component or unmock it |
| Test-only methods in production | Move to test utilities |
| Mock without understanding | Understand dependencies first, mock minimally |
| Incomplete mocks | Mirror real API completely |
| Tests as afterthought | TDD — tests first |
| Over-complex mocks | Consider integration tests |

## Red Flags

- Assertions that only check mock call counts
- Methods only called in test files
- Mock setup is >50% of test
- Test fails when you remove mock
- Can't explain why mock is needed
- Mocking "just to be safe"

## The Bottom Line

**Mocks are tools to isolate, not things to test.**

If TDD reveals you're testing mock behavior, you've gone wrong.

Fix: Test real behavior or question why you're mocking at all.
