import panel as pn
import param
import os
from datetime import datetime
import zipfile
import io
import pathlib
from .gettext_config import translation_manager
import logging

logger = logging.getLogger(__name__)

class FileDownloader(param.Parameterized):
    """A Panel component for downloading files from a specific directory."""

    selected_files = param.ListSelector(default=[])

    def __init__(self, directory_path, **params):
        super().__init__(**params)
        self.directory_path = pathlib.Path(directory_path)
        print(f"\n\n\nFileDownloader: directory_path: {self.directory_path}")
        self._ = translation_manager._

        # Create the interface components
        self.file_selector = pn.widgets.MultiSelect(
            name=self._('Select bulletins to download'),
            options=self.get_sorted_files(),
            size=10,  # Show 10 files at once,
            sizing_mode='stretch_width'
        )

        # Create a placeholder for the download widget
        self.download_placeholder = pn.Column()

        self.download_btn = pn.widgets.Button(
            name=self._('Prepare download for selected files'),
            button_type='primary',
            disabled=False
        )

        # Link components
        self.file_selector.link(self, callbacks={'value': self._update_selected_files})
        self.download_btn.on_click(self._handle_download)

    def get_sorted_files(self):
        """Get list of files recursively from all subdirectories, sorted by modification time (newest first)."""
        print(f"\n\n\nGetting files from {self.directory_path}")
        files = []

        # Recursively walk through all subdirectories
        for root, _, filenames in os.walk(self.directory_path):
            root_path = pathlib.Path(root)
            for filename in filenames:
                file_path = root_path / filename
                # Get the relative path from the base directory
                rel_path = file_path.relative_to(self.directory_path)

                # Check if it's an Excel file
                if filename.endswith(('.xls', '.xlsx')):
                    files.append((str(rel_path), file_path.stat().st_mtime))

        # Sort by modification time, newest first
        files.sort(key=lambda x: x[1], reverse=True)
        print(f"\n\n\nFiles: {files}")
        return [f[0] for f in files]

    def _update_selected_files(self, target, event):
        """Update selected files and enable/disable download button."""
        logger.debug(f"Updating selected files: {event.new}")
        self.selected_files = event.new
        self.download_btn.disabled = len(self.selected_files) == 0

    def _handle_download(self, event):
        """Handle the download button click."""
        try:
            if len(self.selected_files) == 1:
                # Single file download
                file_path = self.directory_path / self.selected_files[0]
                filename = os.path.basename(file_path)
                print(f"Downloading single file: {file_path} as {filename}")

                download_widget = pn.widgets.FileDownload(
                    file=str(file_path),
                    filename=filename,
                    button_type="primary",
                    name=self._('Click here if download does not start automatically'),
                    auto=True
                )

            else:
                # Multiple files - create zip
                zip_filename = "selected_files.zip"
                zip_buffer = io.BytesIO()

                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for filename in self.selected_files:
                        file_path = self.directory_path / filename
                        print(f"Adding to zip: {file_path}")
                        # Add file to zip with its relative path
                        zip_file.write(file_path, filename)

                zip_buffer.seek(0)

                download_widget = pn.widgets.FileDownload(
                    file=zip_buffer,
                    filename=zip_filename,
                    button_type="primary",
                    name=self._('Click here if download does not start automatically')
                )

            # Update the download placeholder with the new widget
            self.download_placeholder.clear()
            self.download_placeholder.append(download_widget)

            # Remove the download widget after a certain time
            #pn.state.curdoc.add_periodic_callback(cleanup, 5000)  # 5 seconds


        except Exception as e:
            logger.error(f"Error during download: {e}", exc_info=True)
            raise

    def panel(self):
        """Return the Panel interface."""
        return pn.Column(
            self.file_selector,
            self.download_btn,
            self.download_placeholder  # Add the placeholder to the layout
        )