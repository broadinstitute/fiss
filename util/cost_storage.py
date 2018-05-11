import subprocess
from firecloud import api as fapi

def get_vm_cost(machine_type, preemptible=True):
    """
    Cost per hour
    """
    preemptible_dict = {
        'n1-standard-1': 0.0100,  # 3.75 GB
        'n1-standard-2': 0.0200,  # 7.5 GB
        'n1-standard-4': 0.0400,  # 15  GB
        'n1-standard-8': 0.0800,  # 30  GB
        'n1-standard-16':0.1600,  # 60  GB
        'n1-standard-32':0.3200,  # 120 GB
        'n1-standard-64':0.6400,  # 240 GB
        'n1-highmem-2':  0.0250,  # 13  GB
        'n1-highmem-4':  0.0500,  # 26  GB
        'n1-highmem-8':  0.1000,  # 52  GB
        'n1-highmem-16': 0.2000,  # 104 GB
        'n1-highmem-32': 0.4000,  # 208 GB
        'n1-highmem-64': 0.8000,  # 416 GB
        'n1-highcpu-2':  0.0150,  # 1.80 GB
        'n1-highcpu-4':  0.0300,  # 3.60 GB
        'n1-highcpu-8':  0.0600,  # 7.20 GB
        'n1-highcpu-16': 0.1200,  # 14.40 GB
        'n1-highcpu-32': 0.2400,  # 28.80 GB
        'n1-highcpu-64': 0.4800,  # 57.6 GB
        'f1-micro':      0.0035,  # 0.6 GB
        'g1-small':      0.0070,  # 1.7 GB
    }

    standard_dict = {
        'n1-standard-1': 0.0475,
        'n1-standard-2': 0.0950,
        'n1-standard-4': 0.1900,
        'n1-standard-8': 0.3800,
        'n1-standard-16': 0.7600,
        'n1-standard-32': 1.5200,
        'n1-standard-64': 3.0400,
        'n1-highmem-2':  0.1184,
        'n1-highmem-4':  0.2368,
        'n1-highmem-8':  0.4736,
        'n1-highmem-16': 0.9472,
        'n1-highmem-32': 1.8944,
        'n1-highmem-64': 3.7888,
        'n1-highcpu-2':  0.0709,
        'n1-highcpu-4':  0.1418,
        'n1-highcpu-8':  0.2836,
        'n1-highcpu-16': 0.5672,
        'n1-highcpu-32': 1.1344,
        'n1-highcpu-64': 2.2688,
        'f1-micro':      0.0076,
        'g1-small':      0.0257,
    }

    if preemptible:
        return preemptible_dict[machine_type]
    else:
        return standard_dict[machine_type]

def get_storage(args):
    """
    Get total amount of storage used, in TB
    Pricing: $0.026/GB/month (multi-regional)
    $0.02/GB/month (regional)
    import os
    os.environ['CLOUDSDK_PYTHON'] = '/usr/local/var/pyenv/versions/2.7.12/bin/python'
    """
    r = fapi.get_workspace(args.project, args.workspace)
    assert r.status_code == 200
    r = r.json()
    s = subprocess.check_output('gsutil du -s gs://'+r['workspace']['bucketName'], shell=True, executable='/bin/bash')
    return float(s.decode().split()[0])/ 1024 ** 4


