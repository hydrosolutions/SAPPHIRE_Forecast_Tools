import sys
import os
import pandas as pd
import holoviews as hv
from bokeh.resources import INLINE


# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Print the current working directory
print(os.getcwd())

import src

# Read two versions of the daily discharge data
orig = pd.read_csv('../../../sensitive_data_forecast_tools/intermediate_data/runoff_day copy.csv')
new = pd.read_csv('../../../sensitive_data_forecast_tools/intermediate_data/runoff_day.csv')

# Initialize Holoviews
hv.extension('bokeh')

# Create a list to hold the plots
plots0 = []
plots1 = []

# Plot the two versions of the daily discharge data. One plot for each unique site code.
codes = orig['code'].unique()

for code in codes[0:40]:
    orig_subset = orig[orig['code'] == code]
    new_subset = new[new['code'] == code]

    # Ensure the date column is in datetime format
    orig_subset['date'] = pd.to_datetime(orig_subset['date'])
    new_subset['date'] = pd.to_datetime(new_subset['date'])

    # Create Holoviews curves
    orig_curve = hv.Curve((orig_subset['date'], orig_subset['discharge']), 'date', 'discharge', label='Original')
    new_curve = hv.Curve((new_subset['date'], new_subset['discharge']), 'date', 'discharge', label='New')

    # Overlay the curves
    overlay = orig_curve * new_curve
    overlay = overlay.opts(title=f'Code: {code}', xlabel='Date', ylabel='Discharge')

    # Append the overlay to the plots list
    plots0.append(overlay)

for code in codes[40:80]:
    orig_subset = orig[orig['code'] == code]
    new_subset = new[new['code'] == code]

    # Ensure the date column is in datetime format
    orig_subset['date'] = pd.to_datetime(orig_subset['date'])
    new_subset['date'] = pd.to_datetime(new_subset['date'])

    # Create Holoviews curves
    orig_curve = hv.Curve((orig_subset['date'], orig_subset['discharge']), 'date', 'discharge', label='Original')
    new_curve = hv.Curve((new_subset['date'], new_subset['discharge']), 'date', 'discharge', label='New')

    # Overlay the curves
    overlay = orig_curve * new_curve
    overlay = overlay.opts(title=f'Code: {code}', xlabel='Date', ylabel='Discharge')

    # Append the overlay to the plots list
    plots1.append(overlay)

# Combine all plots into a single Holoviews layout
layout0 = hv.Layout(plots0).cols(4).opts(shared_axes=False)
layout1 = hv.Layout(plots1).cols(4).opts(shared_axes=False)

# Save the layout as an HTML file
hv.save(layout0, 'runoff_comparison_0.html', resources=INLINE)
hv.save(layout1, 'runoff_comparison_1.html', resources=INLINE)


