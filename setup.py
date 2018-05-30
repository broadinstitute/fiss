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
_README           = os.path.join(os.path.dirname(__file__), 'README')
_LONG_DESCRIPTION = open(_README).read()

# Based on https://stackoverflow.com/a/379535
def which(program):
    def is_exe(fpath):
        return os.path.exists(fpath) and os.access(fpath, os.X_OK) and os.path.isfile(fpath)

    def ext_candidates(fpath):
        yield fpath
        # Handle windows executable extensions
        for ext in os.environ.get("PATHEXT", "").split(os.pathsep):
            if ext: yield fpath + ext

    fpath, _ = os.path.split(program)
    if fpath and is_exe(program):
        return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            for candidate in ext_candidates(exe_file):
                if is_exe(candidate):
                    return candidate

    return None

class InstallCommand(install):
    def needs_gcloud(self):
        """Returns true if gcloud is unavailable and needed for
        authentication."""
        return not os.getenv('SERVER_SOFTWARE',
                             '').startswith('Google App Engine/') and \
               which('gcloud') is None
    
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
        self.win_gcloud_url = \
        "https://dl.google.com/dl/cloudsdk/channels/rapid/google-cloud-sdk.zip"
        self.nix_gcloud_url = "https://sdk.cloud.google.com"
        self.win_gcloud_installer = "google-cloud-sdk\install.bat"
        self.silent = "--disable-prompts"
        if platform.system() != "Windows":
            self.curl = which('curl')
            self.bash = which('bash')
        self.package_index = PackageIndex()
    
    # Copied from setuptools.command.easy_install.easy_install
    @contextlib.contextmanager
    def _tmpdir(self):
        tmpdir = tempfile.mkdtemp(prefix=six.u("easy_install-"))
        try:
            # cast to str as workaround for #709 and #710 and #712
            yield str(tmpdir)
        finally:
            os.path.exists(tmpdir) and rmtree(rmtree_safe(tmpdir))

    def run(self):
        warn_msg = "Please install the Google Cloud SDK manually:\n\t" \
                   "https://cloud.google.com/sdk/downloads"
        if platform.system() == "Windows":
            from zipfile import ZipFile
            with self._tmpdir() as tmpdir:
                ZipFile(self.package_index.download(self.win_gcloud_url,
                                                    tmpdir)).extractall(tmpdir)
                try:
                    output = subprocess.check_output([os.path.join(tmpdir,
                                                                   self.win_gcloud_installer),
                                                      self.silent],
                                                     stderr=subprocess.STDOUT)
                    log.info(output)
                except subprocess.CalledProcessError as cpe:
                    log.warn(cpe.cmd + ":\n\t" + cpe.output)
                
        elif self.curl is not None and self.bash is not None:
            try:
                script = subprocess.check_output([self.curl, "-s", "-S",
                                                  self.nix_gcloud_url],
                                                 stderr=subprocess.STDOUT)
                if script:
                    with self._tmpdir() as tmpdir:
                        gcloud_install_cmd = os.path.join(tmpdir,
                                                          'installer.sh')
                        with open(gcloud_install_cmd, 'w') as gcloud_install_fd:
                            gcloud_install_fd.write(script)
                        output = subprocess.check_output([self.bash,
                                                          gcloud_install_cmd,
                                                          self.silent],
                                                         stderr=subprocess.STDOUT)
                        log.info(output)
                else:
                    log.warn("Unable to download installer script. " + warn_msg)
            except subprocess.CalledProcessError as cpe:
                log.warn(cpe.cmd + ":\n\t" + cpe.output)
        else:
            log.warn("Unable to find curl and/or bash. " + warn_msg)

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
        'google-auth',
        'pydot',
        'requests[security]',
        'six',
        'pylint==1.7.2'
    ],
    classifiers = [
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",

    ],

)
