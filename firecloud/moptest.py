import argparse
from fiss import mop

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--project', default="broad-firecloud-dsde")
parser.add_argument('--workspace', default="HCA_Optimus_Pipeline_2020-07-15-00-48-10_sushma_DELETE")
parser.add_argument('--verbose', default=True)
parser.add_argument('--include', default="")
parser.add_argument('--exclude', default="")
parser.add_argument('--dry_run', default=True)
parser.add_argument('--make_manifest', default=True)
args = parser.parse_args()
# broad-firecloud-dsde/HCA_Optimus_Pipeline_2020-07-15-00-48-10_sushma_DELETE/
# args.project = "broad-firecloud-dsde"
# args.workspace = "Terra-Data-Quickstart_MS"
mop(args)
