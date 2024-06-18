import argparse
import os
from datetime import datetime, timezone

from pyairtable import Api
from pyairtable.formulas import match

from indexer.app import App


# A utility to report new deployments to the central MEAG airtable record.
class AirtableInterface(App):

    def define_options(self, ap: argparse.ArgumentParser) -> None:
        ap.add_argument("--name", help="additional deployment name")
        ap.add_argument(
            "--env",
            choices=["production", "staging", "other"],
            help="Formal environment name",
        )
        ap.add_argument("--version", help="A descriptive version string")
        ap.add_argument(
            "--hardware",
            nargs="+",
            help="The names of one or more machines being deployed to",
        )

    def main_loop(self) -> None:
        assert self.args
        self.create_deployment(
            "story-indexer",
            self.args.name,
            self.args.env,
            self.args.version,
            self.args.hardware,
        )

    # This function ideally will be re-used over a few other system repositories, so I'm breaking it out like this for now
    def create_deployment(
        self,
        codebase_name: str,
        deployment_name: str | None,
        environment: str,
        version_info: str,
        hardware_names: list[str],
    ) -> None:
        # The base id is not secret, but the api key is

        api = Api(os.environ["AIRTABLE_API_KEY"])
        MEAG_BASE_ID = "appuh6zjiSqCFCcT6"

        # Get codebases id:
        codebase_table = api.table(MEAG_BASE_ID, table_name="Software - Codebases")
        codebase_res = codebase_table.first(formula=match({"name": codebase_name}))
        if codebase_res is None:
            names = [c["fields"]["Name"] for c in codebase_table.all()]
            raise ValueError(f"codebase must be one of {names}")
        else:
            codebase = codebase_res["id"]

        # Get hardware ids (there may be multiple hardwares):
        hardware_table = api.table(MEAG_BASE_ID, table_name="Hardware - Angwin Cluster")

        hardware_res = [
            hardware_table.first(formula=match({"name": hardware_name}))
            for hardware_name in hardware_names
        ]
        if None in hardware_res:
            # The names of the machines in the angwin cluster are all valid options.
            names = [h["fields"]["name"] for h in hardware_table.all()]
            raise ValueError(f"hardware must be at least one of {names}")

        hardware = [h["id"] for h in hardware_res]  # type: ignore[index]

        deployment_table = api.table(MEAG_BASE_ID, table_name="Software - Deployments")

        if deployment_name is None:
            deployment_name = environment

        name = ":".join([codebase_name, deployment_name])

        # Get the current time in UTC
        utc_now = datetime.now(timezone.utc)
        iso_timestamp = utc_now.isoformat()

        resp = deployment_table.batch_upsert(
            [
                {
                    "fields": {
                        "Name": name,
                        "Software - Codebases": [codebase],
                        "Environment": environment,
                        "Hardware": hardware,
                        "Version": version_info,
                        "Deploy Time": iso_timestamp,
                    }
                }
            ],
            key_fields=["Name"],
        )
        print(resp)


if __name__ == "__main__":
    app = AirtableInterface("airtable-update", "update deployment records on airtable")
    app.main()
