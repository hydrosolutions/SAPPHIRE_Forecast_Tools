# functions to create the layout of the dashboard
import os
import panel as pn
import datetime as dt
from .vizualization import update_sidepane_card_visibility


# Define components of the layout
def define_sidebar(_, station_widget, forecast_card):
    return pn.Column(
        pn.Row(pn.Card(station_widget,
                       title=_('Hydropost:'),)),
        #pn.Row(pentad_card),
        #pn.Row(pn.Card(pentad_selector, title=_('Pentad:'))),
        #pn.Row(pn.Card(date_picker, date_picker_with_pentad_text,
                       #title=_('Date:'),
                       #width_policy='fit', width=station.width,
                       #collapsed=False)),
        pn.Row(forecast_card),
        #pn.Row(range_selection),
        #pn.Row(manual_range),
        #pn.Row(print_button),
        #pn.Row(pn.Card(warning_text_pane, title=_('Notifications'),
        #            width_policy='fit', width=station.width)),
    )

def get_logos(in_docker_flag):
    if in_docker_flag == "True":
        return pn.Row(
            pn.pane.Image(os.path.join(
                "apps", "forecast_dashboard", "www", "sapphire_project_logo.jpg"),
                width=70),
            pn.pane.Image(os.path.join(
                "apps", "forecast_dashboard", "www", "hydrosolutionsLogo.jpg"),
                width=100),
            pn.pane.Image(os.path.join(
                "apps", "forecast_dashboard", "www", "sdc.jpeg"),
                width=150))
    else:
        return pn.Row(
            pn.pane.Image(os.path.join(
                "www", "sapphire_project_logo.jpg"),
                width=70),
            pn.pane.Image(os.path.join(
                "www", "hydrosolutionsLogo.jpg"),
                width=100),
            pn.pane.Image(os.path.join(
                "www", "sdc.jpeg"),
                width=150))

def define_disclaimer(_, in_docker_flag):
    logos = get_logos(in_docker_flag)
    return pn.Column(
        pn.pane.HTML(_('disclaimer_who')),
        pn.pane.Markdown(_("disclaimer_waranty")),
        pn.pane.HTML("<p> </p>"),
        logos,
        pn.pane.Markdown(_("Last updated on ") + dt.datetime.now().strftime("%b %d, %Y") + ".")
    )

def define_tabs(_, daily_hydrograph_plot, forecast_data_and_plot,
                forecast_summary_table, pentad_forecast_plot, bulletin_table,
                write_bulletin_button, indicator, disclaimer,
                forecast_card, pentad_card):

    # Organize the panes in tabs
    no_date_overlap_flag = True
    if no_date_overlap_flag == False:
        tabs = pn.Tabs(
            # Predictors tab
            (_('Predictors'),
            pn.Column(
                 pn.Row(
                     pn.Card(daily_hydrograph_plot, title=_("Hydrograph"))
                ),
            ),
            ),
            (_('Forecast'),
             #pn.Column(
            #     pn.Row(
            #        pn.Card(data_table, title=_('Data table'), collapsed=True),
            #        pn.Card(linear_regression, title=_("Linear regression"), collapsed=True)
            #        ),
            #     pn.Row(
            #         pn.Card(norm_table, title=_('Norm statistics'), sizing_mode='stretch_width'),),
            #     pn.Row(
            #         pn.Card(forecast_table, title=_('Forecast table'), sizing_mode='stretch_width')),
                     pn.Card(
                         #pentad_forecast_plot,
                         title=_('Hydrograph'),
                     ),
                     pn.Card(
                         forecast_summary_table,
                         title=_('Summary table'),
                         sizing_mode='stretch_width'
                     ),
                     pn.Card(
                         daily_hydrograph_plot,
                         title=_('Analysis of the forecast'))
            #     pn.Row(
            #         pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods"))),
            #     pn.Row(
            #         pn.Card(pentad_skill, title=_("Forecast accuracy")))
            #)
            ),
            (_('Disclaimer'), disclaimer),
            dynamic=True,
            sizing_mode='stretch_both'
        )
    else: # If no_date_overlap_flag == True
        tabs = pn.Tabs(
            # Predictors tab
            (_('Predictors'),
            pn.Column(
                 pn.Row(
                     pn.Card(daily_hydrograph_plot, title=_("Hydrograph")),
                 ),
             ),
            ),
            (_('Forecast'),
             pn.Column(
                pn.Card(
                    pn.Row(
                        forecast_data_and_plot
                    ),
                    title=_('Linear regression'),
                    sizing_mode='stretch_width',
                    collapsible=True,
                    collapsed=False
                ),
                pn.Card(
                    forecast_summary_table,
                    title=_('Summary table'),
                    sizing_mode='stretch_width',
                ),
                pn.Card(
                    pentad_forecast_plot,
                    title=_('Hydrograph'),
                    height=500,
                    collapsible=True,
                    collapsed=False
                ),
                     #pn.Card(
                     #    pentad_forecast_plot,
                     #    title=_('Analysis'))
            #         #pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods")),
            #         #pn.Card(pentad_skill, title=_("Forecast accuracy")),
                )
            ),
            (_('Bulletin'),
             pn.Column(
                 pn.Card(
                     pn.Column(
                         bulletin_table,
                        pn.Row(
                             write_bulletin_button,
                            indicator),
                     ),
                     title='Forecast bulletin',
                ),
             )
            ),
            (_('Disclaimer'), disclaimer),
            dynamic=True,
            sizing_mode='stretch_both'
        )
    tabs.param.watch(lambda event: update_sidepane_card_visibility(
    tabs, forecast_card, pentad_card, event), 'active')
    return tabs

def create_dashboard():

    pass
