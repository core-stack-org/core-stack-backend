from django.core.management.base import BaseCommand, CommandError

from computing.pending_stac_sync import sync_pending_stac


class Command(BaseCommand):
    help = (
        "Generate STAC items and sync them to S3 for active tehsils whose layers "
        "are already on GeoServer (is_sync_to_geoserver=True) and pending STAC "
        "(is_stac_specs_generated=False). Runs serially per tehsil with one "
        "targeted S3 upload per tehsil."
    )

    def add_arguments(self, parser):
        parser.add_argument("--state")
        parser.add_argument("--district")
        parser.add_argument("--block")
        parser.add_argument(
            "--layer-type",
            help=(
                "Bulk pipeline name (as in sync_pending_geoserver). Only that "
                "layer type's datasets are processed."
            ),
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                "Also include layers already marked is_stac_specs_generated=True "
                "(active tehsils only). Requires --yes unless --dry-run."
            ),
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Rebuild existing STAC item JSON files and thumbnails on disk.",
        )
        parser.add_argument(
            "--overwrite-metadata",
            action="store_true",
            help="Re-download STAC metadata CSVs from GitHub once before syncing.",
        )
        s3_group = parser.add_mutually_exclusive_group()
        s3_group.add_argument(
            "--upload-to-s3",
            action="store_true",
            default=None,
            dest="upload_to_s3",
            help="Upload updated STAC collections and thumbnails to S3 (overrides STAC_UPLOAD_TO_S3).",
        )
        s3_group.add_argument(
            "--no-s3",
            action="store_false",
            dest="upload_to_s3",
            help="Write STAC files locally only without uploading to S3.",
        )
        parser.add_argument(
            "--limit-tehsils",
            type=int,
            default=None,
            metavar="N",
            help="Process at most N active tehsils in this run.",
        )
        parser.add_argument("--yes", action="store_true")
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        if options["force"] and not (options["dry_run"] or options["yes"]):
            raise CommandError(
                "--force re-processes layers already marked STAC-generated; "
                "check with --dry-run, then pass --yes."
            )

        try:
            summary = sync_pending_stac(
                state=options["state"],
                district=options["district"],
                block=options["block"],
                layer_type=options["layer_type"],
                force=options["force"],
                upload_to_s3=options["upload_to_s3"],
                overwrite=options["overwrite"],
                overwrite_metadata=options["overwrite_metadata"],
                dry_run=options["dry_run"],
                limit_tehsils=options["limit_tehsils"],
            )
        except ValueError as exc:
            raise CommandError(str(exc))

        mode = "DRY RUN" if options["dry_run"] else "RUN"
        s3_label = "S3 upload ON" if summary["upload_to_s3"] else "S3 upload OFF"
        self.stdout.write(
            f"[sync_pending_stac:{mode}] ({s3_label}) "
            f"{summary['layers_found']} synced layer(s) across "
            f"{summary['tehsils_found']} active tehsil(s); "
            f"selected {summary['tehsils_selected']} tehsil(s) / "
            f"{summary['targets_planned']} STAC target(s), "
            f"{summary['skipped_unmapped']} unmapped."
        )

        if options["dry_run"]:
            for tp in summary["tehsil_plans"]:
                self.stdout.write(
                    f"  tehsil: {tp['state']}/{tp['district']}/{tp['block']} "
                    f"({len(tp['targets'])} target(s))"
                )
                for target in tp["targets"]:
                    yr = (
                        f" [{target['start_year']}]"
                        if target.get("start_year")
                        else ""
                    )
                    self.stdout.write(
                        f"    would sync ({target['layer_type']}): "
                        f"{target['layer_name']}{yr} "
                        f"(layer_ids={target['layer_ids']})"
                    )
        else:
            for res in summary["tehsil_results"]:
                s3_status = (
                    "s3=ok"
                    if res["s3_synced"] is True
                    else ("s3=FAILED" if res["s3_synced"] is False else "s3=skipped")
                )
                self.stdout.write(
                    f"  tehsil: {res['state']}/{res['district']}/{res['block']} "
                    f"built={len(res['built'])} failed={len(res['failed'])} "
                    f"{s3_status} ({res['s3_paths_count']} path(s))"
                )
                for fail in res["failed"]:
                    self.stderr.write(
                        f"    FAILED ({fail['layer_type']}): "
                        f"{fail['layer_name']} - {fail['reason']}"
                    )

            self.stdout.write(
                f"Done: processed {summary['tehsils_processed']} tehsil(s), "
                f"built {summary['targets_built']}, failed {summary['targets_failed']}, "
                f"s3_failures {summary['s3_sync_failures']}."
            )

        for skipped in summary["skipped_layers"]:
            self.stdout.write(
                f"  skipped: {skipped['dataset']}:{skipped['layer_name']} "
                f"({skipped['state']}/{skipped['district']}/{skipped['block']}) "
                f"- {skipped['reason']}"
            )
