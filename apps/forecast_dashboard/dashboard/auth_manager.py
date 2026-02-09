import os
import httpx
from datetime import datetime, timedelta

import panel as pn

from dashboard import widgets


# Cookie config
_COOKIE_ACCESS  = "sapphire_access_token"
_COOKIE_REFRESH = "sapphire_refresh_token"
_COOKIE_MAX_AGE = 60 * 60 * 24  # 1 day (seconds)


class AuthManager:
    """Manages authentication state, login/logout UI, and inactivity timeout.

    Tokens are persisted in secure browser cookies so that a page reload
    does *not* require re-login.  On ``initialize()`` the manager reads
    any existing cookies, verifies the access token (refreshing if needed),
    and restores the session automatically.
    """

    def __init__(self):
        self._last_activity_time: datetime | None = None
        self._timeout = timedelta(
            minutes=int(os.getenv("ieasyforecast_minutes_inactive_until_logout", 10))
        )

        # Auth microservice base URL (through API gateway)
        self._auth_base_url = os.getenv(
            "SAPPHIRE_AUTH_SERVICE_URL", "http://localhost:8000/api/auth"
        )

        # In-memory token storage (per-session, backed by cookies)
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._current_user: dict | None = None

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

        # Hidden pane used to execute client-side JS (cookie ops)
        self._js_pane = pn.pane.HTML("", width=0, height=0, visible=False)

        # Panels toggled by show/hide — registered via `register_panels`
        self._managed_panels: dict[str, pn.viewable.Viewable] = {}

        # --- Bind handlers ---
        self.login_submit_button.on_click(self._handle_login)
        self.logout_button.on_click(self._handle_logout_request)
        self.logout_yes.on_click(self._handle_logout_confirm)
        self.logout_no.on_click(self._handle_logout_cancel)
    
    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _post(self, path: str, **kwargs) -> httpx.Response:
        """POST to the auth microservice."""
        url = f"{self._auth_base_url}{path}"
        with httpx.Client(timeout=10) as client:
            return client.post(url, **kwargs)

    def _get(self, path: str, **kwargs) -> httpx.Response:
        """GET from the auth microservice."""
        url = f"{self._auth_base_url}{path}"
        with httpx.Client(timeout=10) as client:
            return client.get(url, **kwargs)

    def _auth_headers(self) -> dict[str, str]:
        """Return Authorization header with the current access token."""
        if self._access_token:
            return {"Authorization": f"Bearer {self._access_token}"}
        return {}

    # ------------------------------------------------------------------
    # Cookie helpers
    # ------------------------------------------------------------------

    def _run_js(self, js: str) -> None:
        """Execute JavaScript on the client by injecting a <script> tag.

        Uses a hidden HTML pane; each update replaces the previous script
        so we add a unique nonce to guarantee the browser re-evaluates it.
        """
        import time
        nonce = int(time.time() * 1000)
        self._js_pane.object = f"<script nonce='{nonce}'>{js}</script>"

    def _save_tokens_to_cookies(self) -> None:
        """Persist tokens to browser cookies via JS execution."""
        secure_flag = "Secure;" if os.getenv("SAPPHIRE_SECURE_COOKIES", "").lower() in ("1", "true") else ""
        same_site = os.getenv("SAPPHIRE_COOKIE_SAMESITE", "Lax")
        lines = []
        if self._access_token:
            lines.append(
                f"document.cookie = '{_COOKIE_ACCESS}={self._access_token};"
                f"path=/;max-age={_COOKIE_MAX_AGE};SameSite={same_site};{secure_flag}';"
            )
        if self._refresh_token:
            lines.append(
                f"document.cookie = '{_COOKIE_REFRESH}={self._refresh_token};"
                f"path=/;max-age={_COOKIE_MAX_AGE};SameSite={same_site};{secure_flag}';"
            )
        if lines:
            self._run_js("\n".join(lines))

    def _clear_token_cookies(self) -> None:
        """Delete token cookies from the browser."""
        lines = [
            f"document.cookie = '{name}=;path=/;max-age=0';"
            for name in (_COOKIE_ACCESS, _COOKIE_REFRESH)
        ]
        self._run_js("\n".join(lines))

    def _load_tokens_from_cookies(self) -> bool:
        """Read token cookies sent with the current request.

        Returns True if at least an access token was found.
        """
        cookies: dict = getattr(pn.state, "cookies", {}) or {}
        access = cookies.get(_COOKIE_ACCESS)
        refresh = cookies.get(_COOKIE_REFRESH)
        if access:
            self._access_token = access
            self._refresh_token = refresh
            return True
        return False

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
        """Set initial visibility.

        If valid token cookies exist from a previous page load the session
        is restored automatically — no re-login required.
        """
        if self._load_tokens_from_cookies() and self._verify_token():
            self._last_activity_time = datetime.now()
            self._show_dashboard()
            # Re-persist (tokens may have been refreshed during verify)
            self._save_tokens_to_cookies()
            pn.state.add_periodic_callback(self._check_inactivity, 300_000)
        else:
            # Tokens absent or invalid — clean up and show login
            self._access_token = None
            self._refresh_token = None
            self._current_user = None
            self._clear_token_cookies()
            self._set_auth_visibility(logged_in=False)
            self._show_login_form()

    @property
    def current_user(self) -> dict | None:
        return self._current_user

    @property
    def is_authenticated(self) -> bool:
        return self._access_token is not None

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _try_refresh_token(self) -> bool:
        """Attempt to get a new access token using the refresh token.
        Returns True on success."""
        if not self._refresh_token:
            return False
        try:
            resp = self._post("/refresh", json={"refresh_token": self._refresh_token})
            if resp.status_code == 200:
                data = resp.json()
                self._access_token = data["access_token"]
                self._refresh_token = data.get("refresh_token", self._refresh_token)
                # Persist the rotated tokens
                self._save_tokens_to_cookies()
                return True
        except Exception as e:
            print(f"Token refresh failed: {e}")
        return False

    def _verify_token(self) -> bool:
        """Verify the current access token against the auth service."""
        if not self._access_token:
            return False
        try:
            resp = self._get("/verify", headers=self._auth_headers())
            if resp.status_code == 200:
                data = resp.json()
                self._current_user = data.get("user")
                return True
            # Token expired — try refresh
            if resp.status_code == 401 and self._try_refresh_token():
                return self._verify_token()
        except Exception as e:
            print(f"Token verification failed: {e}")
        return False
    
    # ------------------------------------------------------------------
    # Login / logout handlers
    # ------------------------------------------------------------------

    def _handle_login(self, event) -> None:
        username = self.username_input.value
        password = self.password_input.value

        if not username or not password:
            self.login_feedback.object = "Please enter both username and password."
            self.login_feedback.visible = True
            return

        try:
            # The /login endpoint expects OAuth2 form data
            resp = self._post(
                "/login",
                data={"username": username, "password": password},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        except Exception as e:
            self.login_feedback.object = f"Could not reach auth service: {e}"
            self.login_feedback.visible = True
            return

        if resp.status_code == 200:
            data = resp.json()
            self._access_token = data["access_token"]
            self._refresh_token = data["refresh_token"]
            self._current_user = data.get("user")
            self._last_activity_time = datetime.now()

            # Persist tokens to cookies for session retention
            self._save_tokens_to_cookies()

            self.login_feedback.object = "Login successful!"
            self.login_feedback.visible = True
            self._show_dashboard()

            # Periodic inactivity check (every 5 minutes)
            pn.state.add_periodic_callback(self._check_inactivity, 300_000)

        elif resp.status_code == 401:
            self.login_feedback.object = "Invalid username or password"
            self.login_feedback.visible = True
        elif resp.status_code == 400:
            detail = resp.json().get("detail", "Login failed.")
            self.login_feedback.object = detail
            self.login_feedback.visible = True
        else:
            self.login_feedback.object = f"Login failed (HTTP {resp.status_code})."
            self.login_feedback.visible = True
    
    def _handle_logout_request(self, event) -> None:
        self.logout_confirm.visible = True
        self.logout_yes.visible = True
        self.logout_no.visible = True

    def _handle_logout_confirm(self, event) -> None:
        # Notify the auth service to revoke the refresh token
        if self._refresh_token:
            try:
                self._post("/logout", json={"refresh_token": self._refresh_token})
            except Exception as e:
                print(f"Remote logout call failed (non-critical): {e}")

        # Clear local state
        self._access_token = None
        self._refresh_token = None
        self._current_user = None
        self._last_activity_time = None

        # Clear browser cookies
        self._clear_token_cookies()

        self._hide_logout_confirm()
        self._hide_dashboard()
        self._show_login_form()


    def _handle_logout_cancel(self, event) -> None:
        self._hide_logout_confirm()
    
    # ------------------------------------------------------------------
    # Inactivity
    # ------------------------------------------------------------------

    def _check_inactivity(self) -> None:
        if self._access_token and self._last_activity_time is not None:
            if datetime.now() - self._last_activity_time > self._timeout:
                user_label = (
                    self._current_user.get("username", "unknown")
                    if self._current_user else "unknown"
                )
                print(f"User {user_label} logged out due to inactivity")
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
