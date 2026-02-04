import flet as ft
import mtio


file_picker = ft.FilePicker()


@ft.control
class DuPage(ft.Column):
    selected_path: str | None = None

    def init(self):
        self.controls = [
            ft.Text(value=self.selected_path or "Please select a path"),
            ft.Button(content="Select Path", on_click=self.handle_pick_files),
        ]

    async def handle_pick_files(self, e: ft.Event[ft.Button]):
        sel_dir = await ft.FilePicker().get_directory_path()
        size = mtio.du(sel_dir, 8)
        if sel_dir:
            self.selected_path = sel_dir
            self.controls[0] = ft.Text(value=f"{self.selected_path}: {size}")


def main(page: ft.Page):
    page.navigation_bar = ft.NavigationBar(
        destinations=[
            ft.NavigationBarDestination(icon=ft.Icons.CIRCLE, label="Dir Size"),
        ],
    )
    page.add(DuPage())


ft.run(main)
