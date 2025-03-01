# src/mytaskmanager/cli.py
import argparse
from .config import load_config, save_config

def main():
    parser = argparse.ArgumentParser(
        prog="banana_task",
        description="Command-line interface for banana-task package."
    )
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands.")

    # 'config' subcommand
    config_parser = subparsers.add_parser("config", help="Set or show configuration.")
    config_parser.add_argument("--db-url", type=str, help="Database URL.")
    config_parser.add_argument("--output-dir", type=str, help="Directory for JSON results.")
    config_parser.add_argument("--use-cache", type=str, help="True or False.")
    config_parser.add_argument("--skip-if-in-progress", type=str, help="True or False.")
    # New arguments:
    config_parser.add_argument("--project-name", type=str, help="Name of the project.")
    config_parser.add_argument("--log-level", type=str, help="Log level (DEBUG, INFO, etc.).")

    args = parser.parse_args()

    if args.command == "config":
        handle_config_command(args)
    else:
        parser.print_help()

def handle_config_command(args):
    """
    Logic for 'banana_task config' command.
    If arguments are provided, save them; otherwise print current config.
    """
    updated = {}

    if args.db_url is not None:
        updated["db_url"] = args.db_url
    if args.output_dir is not None:
        updated["output_dir"] = args.output_dir
    if args.use_cache is not None:
        updated["use_cache"] = (args.use_cache.lower() == "true")
    if args.skip_if_in_progress is not None:
        updated["skip_if_in_progress"] = (args.skip_if_in_progress.lower() == "true")
    if args.project_name is not None:
        updated["project_name"] = args.project_name
    if args.log_level is not None:
        updated["log_level"] = args.log_level.upper()  # optional: convert to uppercase

    if updated:
        save_config(updated)
        print("Configuration updated.")
    else:
        # No updates; just print current config
        config = load_config()
        print("Current configuration:")
        for k, v in config.items():
            print(f"  {k} = {v}")
