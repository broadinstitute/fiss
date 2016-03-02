#######################
# FISSFC Plugin Template
#######################

from yapsy.IPlugin import IPlugin

class GDACFissfcPlugin(IPlugin):

    ### Change these to set default values for Fissfc
    # API_URL = "https://portal.firecloud.org/service/api"
    # DEFAULT_PROJECT = "broad-firecloud-testing"

    def register_commands(self, subparsers):
        """
        Add commands to a list of subparsers. This will be called by
        Fissfc to add additional command targets from this plugin.

        Each command added should follow the pattern:

        parser = subparsers.add_parser('cmd', ...)
        parser.add_argument(...)
        ...
        parser.set_defaults(func=do_my_cmd)

        where do_my_cmd is a function that takes one argument "args":, i.e:

        def do_my_cmd(args):
            pass

        """
        #prsr = subparsers.add_parser('test_cmd', description='Test dynamically loaded cmd')
        #prsr.set_defaults(func=test_print)
        pass
        
def test_print(args):
    print "It worked!"