import os
import platform
import contextlib
import tempfile
import subprocess
from setuptools import setup, find_packages, Command
from setuptools.command.install import install
from setuptools.package_index import PackageIndex
from setuptools.command.easy_install import six, rmtree_safe, rmtree, log
from firecloud.__about__ import __version__
from firecloud import which
_README           = os.path.join(os.path.dirname(__file__), 'README')
_LONG_DESCRIPTION = open(_README).read()

class InstallCommand(install):
    def needs_gcloud(self):
        """Returns true if gcloud is unavailable and needed for
        authentication."""
        gcloud_default_path = ['google-cloud-sdk', 'bin']
        if platform.system() != "Windows":
            gcloud_default_path = os.path.join(os.path.expanduser('~'),
                                               *gcloud_default_path)
        else:
            gcloud_default_path = os.path.join(os.environ['LOCALAPPDATA'],
                                               'Google', 'Cloud SDK',
                                               *gcloud_default_path)
        return not os.getenv('SERVER_SOFTWARE',
                             '').startswith('Google App Engine/') \
               and gcloud_default_path not in os.environ["PATH"].split(os.pathsep) \
               and which('gcloud') is None

    sub_commands = install.sub_commands + [('install_gcloud', needs_gcloud)]

class InstallGcloudCommand(Command):
    """ Install Google Cloud SDK"""
    def initialize_options(self):
        self.win_gcloud_url = None
        self.win_gcloud_installer = None
        self.nix_gcloud_url = None
        self.silent = None
        self.curl = None
        self.bash = None
        self.package_index = None

    def finalize_options(self):

        if platform.system() != "Windows":
            self.curl = which('curl')
            self.bash = which('bash')
            self.gcloud_url = "https://sdk.cloud.google.com"
            self.silent = "--disable-prompts"
        else:
            self.silent = "/S"
            self.gcloud_url = "https://dl.google.com/dl/cloudsdk/channels/" \
                              "rapid/GoogleCloudSDKInstaller.exe"
        self.package_index = PackageIndex()

    # Copied from setuptools.command.easy_install.easy_install
    @contextlib.contextmanager
    def _tmpdir(self):
        tmpdir = tempfile.mkdtemp(prefix=six.u("install_gcloud-"))
        try:
            # cast to str as workaround for #709 and #710 and #712
            yield str(tmpdir)
        finally:
            os.path.exists(tmpdir) and rmtree(rmtree_safe(tmpdir))

    def run(self):
        warn_msg = "Please install the Google Cloud SDK manually:\n\t" \
                   "https://cloud.google.com/sdk/downloads"
        if platform.system() == "Windows":
            with self._tmpdir() as tmpdir:
                gcloud_install_cmd = \
                       self.package_index.download(self.gcloud_url, tmpdir)
                try:
                    output = subprocess.check_output([gcloud_install_cmd,
                                                      self.silent],
                                                     stderr=subprocess.STDOUT)
                    log.info(output.decode('utf-8'))
                except subprocess.CalledProcessError as cpe:
                    log.warn(u' '.join(cpe.cmd) + u":\n\t" +
                             cpe.output.decode('utf-8'))

        elif self.curl is not None and self.bash is not None:
            try:
                script = subprocess.check_output([self.curl, "-s", "-S",
                                                  self.gcloud_url],
                                                 stderr=subprocess.STDOUT)
                if script:
                    with self._tmpdir() as tmpdir:
                        gcloud_install_cmd = os.path.join(tmpdir,
                                                          'installer.sh')
                        with open(gcloud_install_cmd, 'w') as gcloud_install_fd:
                            gcloud_install_fd.write(script.decode('utf-8'))
                        output = subprocess.check_output([self.bash,
                                                          gcloud_install_cmd,
                                                          self.silent],
                                                         stderr=subprocess.STDOUT)
                        log.info(output.decode('utf-8'))
                else:
                    log.warn("Unable to download installer script. " + warn_msg)
            except subprocess.CalledProcessError as cpe:
                log.warn(u' '.join(cpe.cmd) + u":\n\t" +
                         cpe.output.decode('utf-8'))
        else:
            log.warn("Unable to find curl and/or bash. " + warn_msg)

    def get_inputs(self):
        return []

    def get_outputs(self):
        return []

# Setup information
setup(
    cmdclass = {
        'install_gcloud': InstallGcloudCommand,
        'install': InstallCommand
    },
    name = 'firecloud',
    version = __version__,
    packages = find_packages(),
    description = 'Firecloud API bindings and FISS CLI',
    author = 'Broad Institute CGA Genome Data Analysis Center',
    author_email = 'gdac@broadinstitute.org',
    long_description = _LONG_DESCRIPTION,
    long_description_content_type='text/plain',
    license = "BSD 3-Clause License",
    url = 'https://github.com/broadinstitute/fiss',
    entry_points = {
        'console_scripts': [
            'fissfc = firecloud.fiss:main_as_cli'
            # Disable until fiss is formally deprecated
            # 'fiss = firecloud.fiss:main'
        ]
    },
    test_suite = 'nose.collector',
    install_requires = [
        'google-auth>=1.6.3',
        'pydot',
        'requests[security]',
        'setuptools>=40.3.0',
        'six',
        'nose',
        'pylint==1.7.2'
    ],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator"
    ]
)
