# Understanding Forecast Uncertainty in SAPPHIRE

## What are forecast ranges?

SAPPHIRE produces not just a single forecast value but a range of
possible outcomes. The range is expressed as quantile bands:

- **q05 to q95**: We expect 90% of outcomes to fall within this range
- **q25 to q75**: The most likely 50% of outcomes (interquartile range)
- **q50**: The median forecast (central estimate)
- Wider bands indicate more uncertainty; narrower bands indicate more
  confidence in the forecast

## How are forecast ranges produced?

Different model types produce ranges differently:

### Linear Regression (LR)

LR forecasts use delta bands, calculated as 0.674 times the standard
deviation of observed flow for the forecast period. These bands
represent average historical variability for the station and period,
not model-specific uncertainty. All LR forecasts for the same station
and period have the same delta value. Roughly 50% of observations
fall within the delta range.

### Machine Learning models (TFT, TiDE, TSMixer, GBT)

ML models produce quantile bands (q05 through q95) directly as part
of their output. These represent the model's estimate of the full
probability distribution and are specific to each individual forecast.

### Ensemble forecasts (EM, Skilled Mean, Naive Mean)

Ensemble forecasts combine quantile bands from multiple models by
averaging corresponding quantiles across models. For example, the
ensemble q10 is the average of each contributing model's q10 for the
same target period. This technique is known as vincentization.

## How to interpret forecast quality metrics

### CRPS (Continuous Ranked Probability Score)

Overall probabilistic skill metric. Lower values are better. CRPS
combines calibration and sharpness into a single number. A CRPS of
zero means perfect probabilistic forecasts. Compare CRPS values
between models for the same station to identify which model provides
better probabilistic forecasts.

### Reliability score

Measures whether the quantile bands are honest. A reliability score
close to zero means well-calibrated bands: q10 captures approximately
10% of observations below it, q25 captures approximately 25%, and so
on. Values above 0.1 suggest the bands are systematically too wide
or too narrow.

### Sharpness (sharpness_90, sharpness_50)

Measures how informative the forecasts are. sharpness_90 is the
average width of the q05 to q95 interval; sharpness_50 is the
average width of the q25 to q75 interval. Lower values are better,
given good calibration. Compare sharpness between models for the
same station to see which provides more precise forecasts.

## Known limitations

### Temporal aggregation

When forecast ranges are aggregated over time (for example, averaging
monthly q05 values to produce a quarterly q05), the resulting bands
are an approximation. This method underestimates the true spread of
the aggregate flow. The actual q05 of the combined period may be
lower than the average of the monthly q05 values, because the true
quantile depends on how flows in different months correlate with
each other.

Forecast ranges at longer aggregation horizons (quarterly, seasonal)
should be interpreted as indicative rather than precise probability
statements.

### Ensemble averaging (vincentization)

Averaging quantiles across models is a valid approximation for
mixture distributions. However, the resulting bands are slightly
narrower than the true combined uncertainty because the method does
not account for disagreement between models about the shape of the
distribution.

### LR delta bands are climatological

LR uncertainty bands reflect average historical variability for the
station and period, not the quality of today's specific forecast. Two
LR forecasts for the same station in the same pentad will always have
the same delta, regardless of how good or bad the individual forecast
is.
