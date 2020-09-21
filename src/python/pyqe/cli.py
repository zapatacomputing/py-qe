from ._pyqe import send_workflowresult_to_sql, export_to_csv, export_to_xlsx
from ._sql import set_configuration, get_configuration
import argparse
import sys
import json


def parse_arguments():
    parser = argparse.ArgumentParser(description="Convert workflow result.")
    subparsers = parser.add_subparsers(help="sub-command help")
    parser.set_defaults(func=None)

    config_parser = subparsers.add_parser(
        "set-config",
        help="Configure SQL connection.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    config_parser.add_argument("user", type=str, help="the username")
    config_parser.add_argument("database", type=str, help="the database name")
    config_parser.add_argument(
        "--url", type=str, default="localhost", help="the SQL server URL"
    )
    config_parser.add_argument(
        "--port", type=str, default="5432", help="the SQL server port"
    )
    config_parser.add_argument("--password", type=str, default="", help="the password")
    config_parser.set_defaults(func=set_configuration_command)

    show_config_parser = subparsers.add_parser(
        "show-config", help="Show SQL configuration."
    )
    show_config_parser.set_defaults(func=show_configuration_command)

    upload_parser = subparsers.add_parser(
        "upload", help="Upload workflow result to SQL connection."
    )
    upload_parser.add_argument(
        "file", type=str, help="The workflow result JSON file to process."
    )
    upload_parser.set_defaults(func=upload)

    export_parser = subparsers.add_parser(
        "export", help="Export workflow result to file."
    )
    export_parser.add_argument(
        "file", type=str, help="The workflow result JSON file to process."
    )
    export_parser.add_argument(
        "--format", type=str, choices=["xlsx", "csv"], default="csv", help="Format to export to."
    )
    export_parser.set_defaults(func=export)

    args = parser.parse_args()

    if args.func is None:
        parser.print_help(sys.stderr)
        sys.exit(1)
    else:
        args.func(args)


def set_configuration_command(args):
    configuration = {
        "user": args.user,
        "database": args.database,
        "url": args.url,
        "port": args.port,
        "password": args.password,
    }
    set_configuration(configuration)


def show_configuration_command(args):
    config = get_configuration()
    if config:
        print(json.dumps(config, indent=2))
    else:
        print("SQL connection not configured.")


def upload(args):
    config = get_configuration()
    if not config:
        print("SQL connection not configured. Run `convert-workflowresult set-config` to configure.")
    with open(args.file) as f:
        workflowresult = json.load(f)
    send_workflowresult_to_sql(workflowresult)

def export(args):
    with open(args.file) as f:
        workflowresult = json.load(f)
    
    if args.format == 'csv':
        export_to_csv(workflowresult)
    elif args.format == 'xlsx':
        export_to_xlsx(workflowresult)
    else:
        print(f"Unsupported output format {args.format}")
        sys.exit(1)
