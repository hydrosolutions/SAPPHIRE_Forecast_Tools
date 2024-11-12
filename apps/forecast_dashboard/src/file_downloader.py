import panel as pn
import param
import os
from datetime import datetime
import zipfile
import io
import pathlib

class FileDownloader(param.Parameterized):
    """A Panel component for downloading files from a specific directory."""

    selected_files = param.ListSelector(default=[])
    download_button = param.Action(lambda x: x.download_files(), label='Download Selected Files')

    def __init__(self, directory_path, **params):
        super().__init__(**params)
        self.directory_path = pathlib.Path(directory_path)

        # Create the interface components
        self.file_selector = pn.widgets.MultiSelect(
            name='Available Files',
            options=self.get_sorted_files(),
            size=10  # Show 10 files at once
        )

        self.download_btn = pn.widgets.Button(
            name='Download Selected Files',
            button_type='primary',
            disabled=True
        )

        # Link components
        self.file_selector.link(self, callbacks={'value': self._update_selected_files})
        self.download_btn.on_click(self._handle_download)

    def get_sorted_files(self):
        """Get list of files sorted by modification time (newest first)."""
        files = []
        for file in self.directory_path.glob('*'):
            if file.is_file():
                files.append((file.name, file.stat().st_mtime))

        # Sort by modification time, newest first
        files.sort(key=lambda x: x[1], reverse=True)
        return [f[0] for f in files]

    def _update_selected_files(self, event):
        """Update selected files and enable/disable download button."""
        self.selected_files = event.new
        self.download_btn.disabled = len(self.selected_files) == 0

    def _handle_download(self, event):
        """Handle the download button click."""
        if len(self.selected_files) == 1:
            # Single file download
            file_path = self.directory_path / self.selected_files[0]
            with open(file_path, 'rb') as f:
                content = f.read()

            return pn.pane.HTML(f"""
                <a id="download" href="data:application/octet-stream;base64,{content.hex()}"
                   download="{self.selected_files[0]}" style="display: none;"></a>
                <script>
                    document.getElementById('download').click();
                </script>
            """)
        else:
            # Multiple files - create zip
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for filename in self.selected_files:
                    file_path = self.directory_path / filename
                    zip_file.write(file_path, filename)

            zip_buffer.seek(0)
            content = zip_buffer.read()

            return pn.pane.HTML(f"""
                <a id="download" href="data:application/zip;base64,{content.hex()}"
                   download="selected_files.zip" style="display: none;"></a>
                <script>
                    document.getElementById('download').click();
                </script>
            """)

    def panel(self):
        """Return the Panel interface."""
        return pn.Column(
            pn.pane.Markdown('### File Download Interface'),
            self.file_selector,
            self.download_btn
        )

