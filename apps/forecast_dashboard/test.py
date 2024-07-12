import pandas as pd
import panel as pn
import hvplot.pandas

# Assuming df is your DataFrame
df = pd.DataFrame({
    'site': ['site1', 'site2', 'site1', 'site2',
             'site1', 'site2', 'site1', 'site2'],
    'period': ['period1', 'period1', 'period2', 'period2',
               'period1', 'period1', 'period2', 'period2'],
    'predictor': [1, 2, 3, 4,
                  1.5, 2.5, 3.5, 4.5],
    'target': [4.5, 5.6, 6.7, 7.8,
               5, 6, 7, 8]
})

# Create the site and period selection widgets
site_select = pn.widgets.Select(name='Site', options=df['site'].unique().tolist())
period_select = pn.widgets.Select(name='Period', options=df['period'].unique().tolist())

# Create a DataFrame widget to hold the filtered data
filtered_data = pn.widgets.DataFrame(df)

# Function to filter the DataFrame and update the DataFrame widget
@pn.depends(site=site_select, period=period_select, watch=True)
def filter_data(site, period):
    filtered_data.value = df[(df['site'] == site) & (df['period'] == period)]

# Create the DataFrame widget
data_table = pn.widgets.DataFrame(filtered_data.value)

# Function to generate a scatter plot
@pn.depends(data_table.param.value)
def scatter_plot(data):
    return data.hvplot.scatter('predictor', 'target')

# Create the scatter plot
scatter = pn.pane.HoloViews(scatter_plot)

# Function to update the DataFrame widget and the scatter plot
def update(event):
    data = filter_data(site_select.value, period_select.value)
    data_table.value = data
    scatter.object = scatter_plot(data)

# Update the DataFrame widget and the scatter plot when the site or period selection changes
site_select.param.watch(update, 'value')
period_select.param.watch(update, 'value')

# Function to update the scatter plot based on the selected rows in the DataFrame widget
def update_scatter(event):
    scatter.object = scatter_plot(data_table.value.loc[event.new])

# Update the scatter plot when the selection in the DataFrame widget changes
data_table.param.watch(update_scatter, 'selection')

# Arrange the widgets in a layout
pn.panel(pn.Column(site_select, period_select, data_table, scatter)).servable()