import os
import warnings
warnings.filterwarnings('ignore', 'Your application has authenticated using end user credentials from Google Cloud SDK. We recommend that most server applications use service accounts instead. If your application continues to use end user credentials from Cloud SDK, you might receive a "quota exceeded" or "API not enabled" error. For more information about service accounts, see https://cloud.google.com/docs/authentication/')


# Based on https://stackoverflow.com/a/379535
def which(program):
    """Find given program on system PATH. Works for both *NIX and Windows"""
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

def __ensure_gcloud():
    """The *NIX installer is not guaranteed to add the google cloud sdk to the
    user's PATH (the Windows installer does). This ensures that if the default
    directory for the executables exists, it is added to the PATH for the
    duration of this package's use."""
    if which('gcloud') is None:
        gcloud_path = os.path.join(os.path.expanduser('~'),
                                   'google-cloud-sdk', 'bin')
        env_path = os.getenv('PATH')
        if os.path.isdir(gcloud_path):
            if env_path is not None:
                os.environ['PATH'] = gcloud_path + os.pathsep + env_path
            else:
                os.environ['PATH'] = gcloud_path

__ensure_gcloud()
