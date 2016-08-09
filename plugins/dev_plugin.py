#######################
# FISSFC Dev Plugin.
#######################
import json
import subprocess
import os

from yapsy.IPlugin import IPlugin
from six import print_

from firecloud import api as fapi
from firecloud.fiss import _are_you_sure


class GDACFissfcPlugin(IPlugin):

    ### Change these to set default values for Fissfc
    # API_URL = "https://api.firecloud.org/api"
    DEFAULT_PROJECT = "broad-firecloud-testing"

    def register_commands(self, subparsers):
        """
        Add commands to a list of subparsers. This will be called by
        Fissfc to add additional command targets from this plugin.

        Each command added should follow the pattern:

        parser = subparsers.add_parser('cmd', ...)
        parser.add_argument(...)
        ...
        parser.set_defaults(func=do_my_cmd)

        where do_my_cmd is a function that takes one argument "args":

        def do_my_cmd(args):
            pass

        """
        #print_("DEV PLUGIN: Loaded commands")
        prsr = subparsers.add_parser(
            'upload',  description='Copy the file or directory into the given')
        prsr.add_argument('workspace', help='Workspace name')
        prsr.add_argument('source', help='File or directory to upload')
        prsr.add_argument('-s', '--show', action='store_true',
                            help="Show the gsutil command, but don't run it")

        dest_help = 'Destination relative to the bucket root. '
        dest_help += 'If omitted the file will be placed in the root directory'
        prsr.add_argument('-d', '--destination', help=dest_help)


        prsr.set_defaults(func=upload)



def upload(args):
    r = fapi.get_workspace(args.namespace, args.workspace, args.api_url)
    fapi._check_response_code(r, 200)

    bucket = r.json()['workspace']['bucketName']

    dest = 'gs://' + bucket + '/'
    if args.destination is not None:
        dest =  dest + args.destination.lstrip('/')

    gsutil_args = ["gsutil", "-o", "GSUtil:parallel_composite_upload_threshold=50M", "cp"]

    if os.path.isdir(args.source):
        gsutil_args.append("-r")
    gsutil_args.extend([args.source, dest])

    print_(' '.join(gsutil_args))
    if not args.show:
        return subprocess.check_call(gsutil_args)
