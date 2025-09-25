Overview
The plan separates the preprocessing_runoff workflow into two modes:

Maintenance mode: Runs overnight, processes historical data (today-50 to today-3 days)
Forecast mode: Runs during the day, processes only recent data (today-2 to today)
This approach will significantly speed up the daily forecast runs by pre-processing the bulk of historical data during maintenance periods.

Key Benefits
Performance Optimization: Daily forecast runs will be much faster since they only process 3 days of data instead of 50+ days
Consistent Pattern: Follows the same maintenance/forecast pattern already established in the machine learning module
Reliability: Overnight maintenance ensures data is ready for fast daily operations
Modularity: Clean separation of concerns between historical data processing and real-time updates
Architecture
The solution involves:

Two specialized scripts: One for maintenance (bulk processing) and one for forecast (incremental updates)
Data persistence: Maintenance results saved to intermediate files for quick loading
Mode detection: Environment variable RUN_MODE determines which operation to perform
Pipeline integration: Modified Docker tasks support both modes seamlessly


