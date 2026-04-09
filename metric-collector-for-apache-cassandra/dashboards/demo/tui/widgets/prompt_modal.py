"""Modal popup for entering prompts to edit/add panels."""

from __future__ import annotations

from dataclasses import dataclass

from textual import on
from textual.app import ComposeResult
from textual.containers import Center, Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, LoadingIndicator, Static


@dataclass
class PromptResult:
    """Result from the prompt modal."""
    action: str  # "edit_panel", "add_panel", "add_section"
    prompt: str
    panel_id: int | None = None
    panel_title: str | None = None
    section_id: int | None = None
    section_title: str | None = None


class PromptModal(ModalScreen[PromptResult | None]):
    """Modal screen with a text input for user prompts."""

    DEFAULT_CSS = """
    PromptModal {
        align: center middle;
    }
    PromptModal #modal-container {
        width: 80;
        max-width: 90%;
        height: auto;
        max-height: 80%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    PromptModal #modal-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        padding-bottom: 1;
    }
    PromptModal #modal-subtitle {
        width: 100%;
        text-align: center;
        color: $text-muted;
        padding-bottom: 1;
    }
    PromptModal #prompt-input {
        width: 100%;
        margin-bottom: 1;
    }
    PromptModal #button-row {
        width: 100%;
        height: 3;
        align: center middle;
    }
    PromptModal #button-row Button {
        margin: 0 1;
    }
    PromptModal #status-label {
        width: 100%;
        text-align: center;
        color: $warning;
        padding: 1 0;
        display: none;
    }
    PromptModal #loading {
        display: none;
        height: 3;
    }
    """

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(
        self,
        action: str,
        title: str,
        subtitle: str = "",
        panel_id: int | None = None,
        panel_title: str | None = None,
        section_id: int | None = None,
        section_title: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.action = action
        self.modal_title = title
        self.modal_subtitle = subtitle
        self.panel_id = panel_id
        self.panel_title = panel_title
        self.section_id = section_id
        self.section_title = section_title

    def compose(self) -> ComposeResult:
        with Vertical(id="modal-container"):
            yield Label(self.modal_title, id="modal-title")
            if self.modal_subtitle:
                yield Label(self.modal_subtitle, id="modal-subtitle")
            yield Input(placeholder="Describe your changes...", id="prompt-input")
            yield Label("", id="status-label")
            yield LoadingIndicator(id="loading")
            with Center(id="button-row"):
                yield Button("Submit", variant="primary", id="submit-btn")
                yield Button("Cancel", variant="default", id="cancel-btn")

    def on_mount(self) -> None:
        self.query_one("#prompt-input", Input).focus()

    @on(Button.Pressed, "#submit-btn")
    def handle_submit(self) -> None:
        prompt_text = self.query_one("#prompt-input", Input).value.strip()
        if not prompt_text:
            return
        self.dismiss(PromptResult(
            action=self.action,
            prompt=prompt_text,
            panel_id=self.panel_id,
            panel_title=self.panel_title,
            section_id=self.section_id,
            section_title=self.section_title,
        ))

    @on(Button.Pressed, "#cancel-btn")
    def handle_cancel(self) -> None:
        self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)

    @on(Input.Submitted, "#prompt-input")
    def handle_input_submitted(self) -> None:
        self.handle_submit()
