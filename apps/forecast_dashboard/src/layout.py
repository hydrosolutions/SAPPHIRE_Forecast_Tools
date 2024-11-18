# functions to create the layout of the dashboard
import os
import param
import panel as pn
import datetime as dt
from .vizualization import update_sidepane_card_visibility
from .gettext_config import translation_manager

import param



# region Widget update functions

def update_station_widget(event, station):
    _ = translation_manager._
    station.name = _("Select discharge station:")
    print("update_station_widget: new name: ", station.name)

# endregion


# May be deprecated, could not get it to work
class DashboardTitle(param.Parameterized):
    value = param.String(default="SAPPHIRE Central Asia - Pentadal forecast dashboard")


# Define components of the layout
def define_sidebar(_, station_card, forecast_card, basin_card, message_pane, reload_card):
    return pn.Column(
        pn.Row(station_card),
        #pn.Row(pentad_card),
        #pn.Row(pn.Card(pentad_selector, title=_('Pentad:'))),
        #pn.Row(pn.Card(date_picker, date_picker_with_pentad_text,
                       #title=_('Date:'),
                       #width_policy='fit', width=station.width,
                       #collapsed=False)),
        pn.Row(forecast_card),
        pn.Row(basin_card),
        pn.Row(message_pane),
        pn.Row(reload_card),
         #pn.Row(range_selection),
        #pn.Row(manual_range),
        #pn.Row(print_button),
        #pn.Row(pn.Card(warning_text_pane, title=_('Notifications'),
        #            width_policy='fit', width=station.width)),
    )

def get_logos(in_docker_flag):
    # overwrite in_docker_flag
    in_docker_flag = "False"
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
        pn.pane.HTML("<p> </p>"),
        logos,
        pn.pane.HTML("<p> </p>"),
        pn.pane.Markdown(_("disclaimer_waranty")),
        pn.pane.Markdown(_("Last updated on ") + dt.datetime.now().strftime("%b %d, %Y") + ".")
    )

def define_tabs(_,
                daily_hydrograph_plot, rainfall_plot, temperature_plot,
                #daily_rel_norm_runoff, daily_rel_to_norm_rainfall,
                forecast_data_and_plot,
                forecast_summary_table, pentad_forecast_plot, effectiveness_plot,
                bulletin_table,
                write_bulletin_button, bulletin_download_panel, disclaimer,
                station_card, forecast_card, add_to_bulletin_button, basin_card,
                pentad_card, reload_card, add_to_bulletin_popup, show_daily_data_widget,
                skill_table, skill_metrics_download_filename, skill_metrics_download_button):

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
                pn.Row(
                    pn.Card(rainfall_plot, title=_("Precipitation"))
                ),
                )
            ),
            (_('Forecast'),
             pn.Column(
            #     pn.Row(
            #        pn.Card(data_table, title=_('Data table'), collapsed=True),
            #        pn.Card(linear_regression, title=_("Linear regression"), collapsed=True)
            #        ),
            #     pn.Row(
            #         pn.Card(norm_table, title=_('Norm statistics'), sizing_mode='stretch_width'),),
            #     pn.Row(
            #         pn.Card(forecast_table, title=_('Forecast table'), sizing_mode='stretch_width')),
                     pn.Card(
                         pentad_forecast_plot,
                         title=_('Hydrograph'),
                     ),
                     pn.Card(
                         forecast_summary_table,
                         title=_('Summary table'),
                     ),
                     pn.Card(
                         daily_hydrograph_plot,
                         title=_('Analysis of the forecast'))
            #     pn.Row(
            #         pn.Card(pentad_effectiveness, title=_("Effectiveness of the methods"))),
            #     pn.Row(
            #         pn.Card(pentad_skill, title=_("Forecast accuracy")))
            )
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
                     #pn.Card(daily_rel_norm_runoff, title=_("Relative to norm runoff")),
                     sizing_mode='stretch_width',
                     min_height=400,
                 ),
                 pn.Row(
                     pn.Card(rainfall_plot, title=_("Precipitation")),
                     #pn.Card(daily_rel_to_norm_rainfall, title=_("Relative to norm rainfall")),
                     sizing_mode='stretch_width',
                 ),
                 pn.Row(
                     pn.Card(temperature_plot, title=_("Temperature")),
                     sizing_mode='stretch_width',
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
                    collapsed=False,
                    min_height=560,
                    max_height=560,
                ),
                pn.Card(
                    pn.Row(
                        add_to_bulletin_button, add_to_bulletin_popup
                    ),
                    forecast_summary_table,
                    title=_('Summary table'),
                    sizing_mode='stretch_both',
                    min_height=400,

                ),
                pn.Card(
                    pn.Column(
                        pn.Row(
                            pn.pane.Markdown(_("Show forecasts aggregated to pentadal values:")),
                            show_daily_data_widget
                        ),
                        pentad_forecast_plot,
                    ),
                    title=_('Hydrograph'),
                    height=600,
                    #height=None,
                    collapsible=True,
                    collapsed=False
                ),
                #pn.Card(
                #    skill_table,
                #    title = 'test card',
                #    height=600,
                #),
                pn.Card(
                    pn.Row(
                     effectiveness_plot,
                    ),
                    title=_("Forecast skill metrics"),
                    height=800,
                    collapsible=True,
                ),
                pn.Card(
                    pn.Column(
                        skill_table,
                        skill_metrics_download_filename,
                        skill_metrics_download_button,
                    ),
                    title=_("Table of forecast skill metrics"),
                    height=600,
                    collapsible=True,
                    #sizing_mode='stretch_width',
                ),
            ),
            ),  # end of Forecast tab
            (_('Bulletin'),
             pn.Column(
                    pn.Card(
                        pn.Column(
                            bulletin_table,
                            write_bulletin_button,
                        ),
                        title='Forecast bulletin',
                        sizing_mode='stretch_width',
                    ),
                    pn.Card(
                        pn.Row(
                            bulletin_download_panel,
                        ),
                        title='Download bulletin',
                        sizing_mode='stretch_width',
                        collapsed=True,
                    ),
             )
            ),
            (_('Disclaimer'), disclaimer),
            dynamic=True,
            sizing_mode='stretch_both'
        )
    tabs.param.watch(lambda event: update_sidepane_card_visibility(
    tabs, station_card, forecast_card, basin_card, pentad_card, reload_card, event), 'active')
    return tabs


