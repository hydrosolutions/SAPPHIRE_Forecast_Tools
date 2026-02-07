import os
from datetime import datetime, timedelta

import panel as pn

from src.auth_utils import load_credentials, check_current_user, save_current_user, remove_current_user, log_auth_event, clear_auth_logs, check_auth_state, log_user_activity, clear_activity_log, check_recent_activity
from dashboard import widgets


class AuthManager:
    """Manages authentication state, login/logout UI, and inactivity timeout."""
    def __init__(self):
        self._last_activity_time: datetime | None = None
        self._timeout = timedelta(
            minutes=int(os.getenv("ieasyforecast_minutes_inactive_until_logout1", 1))
        )

        # --- UI components ---
        self.username_input, self.password_input, self.login_submit_button, self.login_feedback = (
            widgets.create_login_widgets()
        )
        self.logout_confirm, self.logout_yes, self.logout_no = (
            widgets.create_logout_confirm_widgets()
        )
        self.logout_button = widgets.create_logout_button()

        self.login_form = widgets.create_login_form(
            self.username_input, self.password_input,
            self.login_submit_button, self.login_feedback,
        )
        self.logout_panel = widgets.create_logout_panel(
            self.logout_confirm, self.logout_yes, self.logout_no,
        )

        # Panels toggled by show/hide — registered via `register_panels`
        self._managed_panels: dict[str, pn.viewable.Viewable] = {}

        # --- Bind handlers ---
        self.login_submit_button.on_click(self._handle_login)
        self.logout_button.on_click(self._handle_logout_request)
        self.logout_yes.on_click(self._handle_logout_confirm)
        self.logout_no.on_click(self._handle_logout_cancel)
    
    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_panels(
        self,
        dashboard_content: pn.viewable.Viewable,
        sidebar_content: pn.viewable.Viewable,
        language_buttons: pn.viewable.Viewable,
    ) -> None:
        """Register the panels whose visibility is controlled by auth state."""
        self._managed_panels = {
            "dashboard_content": dashboard_content,
            "sidebar_content": sidebar_content,
            "language_buttons": language_buttons,
        }
    
    def track_widget(self, widget: pn.widgets.Widget, parameter: str = "value") -> None:
        """Watch a widget parameter to reset the inactivity timer."""
        widget.param.watch(self._on_user_interaction, parameter)
    
    def track_widgets(self, specs: list[tuple[pn.widgets.Widget, str]]) -> None:
        """Convenience: track many widgets at once.

        *specs* is a list of ``(widget, parameter_name)`` tuples.
        """
        for widget, param_name in specs:
            self.track_widget(widget, param_name)
    
    def initialize(self) -> None:
        """Set initial visibility and restore session if applicable.

        Call this **after** all panels have been registered and added to the
        template.
        """
        self._set_auth_visibility(logged_in=False)

        current_user = check_auth_state()
        if current_user and check_recent_activity(current_user, "language_change"):
            self._show_dashboard()
        else:
            self._show_login_form()
    
    # ------------------------------------------------------------------
    # Login / logout handlers
    # ------------------------------------------------------------------

    def _handle_login(self, event) -> None:
        username = self.username_input.value
        password = self.password_input.value
        credentials = load_credentials()

        if username not in credentials or credentials[username] != password:
            self.login_feedback.object = "Invalid username or password."
            self.login_feedback.visible = True
            return

        current_user = check_current_user()
        if current_user and current_user != username:
            self.login_feedback.object = (
                f"Another user ({current_user}) is currently logged in."
            )
            self.login_feedback.visible = True
            return

        # Successful login
        self._last_activity_time = datetime.now()
        save_current_user(username)
        log_auth_event(username, "logged in")
        log_user_activity(username, "login")

        self.login_feedback.object = "Login successful!"
        self.login_feedback.visible = True
        self._show_dashboard()

        # Check inactivity every 5 minutes
        pn.state.add_periodic_callback(self._check_inactivity, 10_000)
    
    def _handle_logout_request(self, event) -> None:
        self.logout_confirm.visible = True
        self.logout_yes.visible = True
        self.logout_no.visible = True

    def _handle_logout_confirm(self, event) -> None:
        current_user = check_current_user()
        if current_user:
            log_auth_event(current_user, "logged out")
            log_user_activity(current_user, "logout")
            remove_current_user()
            clear_auth_logs()
            clear_activity_log()

        self._hide_logout_confirm()
        self._hide_dashboard()
        self._show_login_form()

    def _handle_logout_cancel(self, event) -> None:
        self._hide_logout_confirm()
    
    # ------------------------------------------------------------------
    # Inactivity
    # ------------------------------------------------------------------

    def _check_inactivity(self) -> None:
        print("Checking inactivity...")
        current_user = check_current_user()
        if current_user and self._last_activity_time is not None:
            if datetime.now() - self._last_activity_time > self._timeout:
                print(f"User {current_user} logged out due to inactivity")
                print("logging out user due to inactivity")
                self._handle_logout_confirm(None)

    def _on_user_interaction(self, event=None) -> None:
        self._last_activity_time = datetime.now()
    
    # ------------------------------------------------------------------
    # Visibility helpers
    # ------------------------------------------------------------------

    def _set_auth_visibility(self, logged_in: bool) -> None:
        self.login_form.visible = not logged_in
        self.logout_button.visible = logged_in
        self.logout_panel.visible = logged_in
        for panel in self._managed_panels.values():
            panel.visible = logged_in

    def _show_dashboard(self) -> None:
        self._set_auth_visibility(logged_in=True)

    def _hide_dashboard(self) -> None:
        self._set_auth_visibility(logged_in=False)

    def _show_login_form(self) -> None:
        self.username_input.value = ""
        self.password_input.value = ""
        self.login_feedback.visible = False
        self._set_auth_visibility(logged_in=False)

    def _hide_logout_confirm(self) -> None:
        self.logout_confirm.visible = False
        self.logout_yes.visible = False
        self.logout_no.visible = False
