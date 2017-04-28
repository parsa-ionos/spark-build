import os
import shakedown


def add_job(job_name):
    jobs_folder = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'jobs', 'json'
    )
    _run_cli('job add {}'.format(
        os.path.join(jobs_folder, '{}.json'.format(job_name))
    ))


def run_job(job_name, timeout_seconds):
    _run_cli('job run {}'.format(job_name))

    shakedown.wait_for(
        lambda: (
            'Successful runs: 1' in
            _run_cli('job history {}'.format(job_name))
        ),
        timeout_seconds=timeout_seconds,
        ignore_exceptions=False
    )


def remove_job(job_name):
    _run_cli('job remove {}'.format(job_name))


def _run_cli(cmd):
    (stdout, stderr, ret) = shakedown.run_dcos_command(cmd)
    if ret != 0:
        err = 'Got error code {} when running command "dcos {}":\nstdout: "{}"\nstderr: "{}"'.format(
            ret, cmd, stdout, stderr)
        raise Exception(err)
    return stdout
