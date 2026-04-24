from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
import os

console = Console()

INPUT_OPTIONS = {
    "1": "sample",
    "2": "jul",
    "3": "aug",
    "4": "both"
}

PIPELINES = ["mapreduce", "mongo", "hive", "pig"]


def run_command(cmd):
    console.print(f"\n[bold yellow]Running:[/bold yellow] {cmd}\n")
    os.system(cmd)
    console.print("\n[bold green]✔ Done[/bold green]\n")


def run_multiple(commands):
    for cmd in commands:
        run_command(cmd)


def show_input_menu():
    table = Table(title="Select Input Dataset", show_lines=True)

    table.add_column("Option", style="cyan", justify="center")
    table.add_column("Dataset", style="magenta")

    table.add_row("1", "Sample Dataset")
    table.add_row("2", "NASA Jul95")
    table.add_row("3", "NASA Aug95")
    table.add_row("4", "Both Datasets")

    console.print(table)


def show_pipeline_menu():
    table = Table(title="Select Pipeline", show_lines=True)

    table.add_column("Option", style="cyan", justify="center")
    table.add_column("Pipeline", style="magenta")

    for i, p in enumerate(PIPELINES, start=1):
        table.add_row(str(i), p)

    table.add_row("0", "Exit")

    console.print(table)


def show_query_menu(pipeline, input_choice):
    table = Table(title=f"Pipeline: {pipeline} | Input: {input_choice}", show_lines=True)

    table.add_column("Option", style="cyan", justify="center")
    table.add_column("Action", style="magenta")

    table.add_row("1", "Run Query 1 (Daily Traffic)")
    table.add_row("2", "Run Query 2 (Top Resources)")
    table.add_row("3", "Run Query 3 (Error Analysis)")
    table.add_row("4", "Run All Queries")
    table.add_row("5", "Show Report (q1)")
    table.add_row("6", "Show Report (q2)")
    table.add_row("7", "Show Report (q3)")
    table.add_row("8", "Show All Reports")
    table.add_row("0", "Back")

    console.print(table)


def main():
    while True:
        console.clear()

        console.print(
            Panel(
                "[bold cyan]ETL Log Analytics System[/bold cyan]\n[dim]Multi-Pipeline Support[/dim]",
                expand=False
            )
        )

        # -------- Pipeline Selection --------
        show_pipeline_menu()
        p_choice = Prompt.ask("\nSelect pipeline", default="0")

        if p_choice == "0":
            console.print("[bold red]Exiting...[/bold red]")
            break

        if not p_choice.isdigit() or int(p_choice) not in range(1, len(PIPELINES) + 1):
            console.print("[bold red]Invalid choice![/bold red]")
            input("Press Enter...")
            continue

        pipeline = PIPELINES[int(p_choice) - 1]

        # -------- Input Selection --------
        show_input_menu()
        i_choice = Prompt.ask("\nSelect input", default="1")

        if i_choice not in INPUT_OPTIONS:
            console.print("[bold red]Invalid input choice![/bold red]")
            input("Press Enter...")
            continue

        input_choice = INPUT_OPTIONS[i_choice]

        # -------- Query Menu --------
        while True:
            console.clear()

            console.print(
                Panel(
                    f"[bold green]Pipeline:[/bold green] {pipeline} | [bold blue]Input:[/bold blue] {input_choice}",
                    expand=False
                )
            )

            show_query_menu(pipeline, input_choice)

            choice = Prompt.ask("\nEnter your choice", default="0")

            base = f"python -m controller.main --pipeline {pipeline} --input {input_choice}"

            if choice == "1":
                run_command(f"{base} --query q1")

            elif choice == "2":
                run_command(f"{base} --query q2")

            elif choice == "3":
                run_command(f"{base} --query q3")

            elif choice == "4":
                run_multiple([
                    f"{base} --query q1",
                    f"{base} --query q2",
                    f"{base} --query q3"
                ])

            elif choice == "5":
                run_command(f"{base} --query q1 --report")

            elif choice == "6":
                run_command(f"{base} --query q2 --report")

            elif choice == "7":
                run_command(f"{base} --query q3 --report")

            elif choice == "8":
                run_multiple([
                    f"{base} --query q1 --report",
                    f"{base} --query q2 --report",
                    f"{base} --query q3 --report"
                ])

            elif choice == "0":
                break

            else:
                console.print("[bold red]Invalid choice![/bold red]")

            input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()